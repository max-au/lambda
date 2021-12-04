%% @doc
%% Local broker process. Connected to all authorities.
%% Connected to exchanges for which there are outstanding orders
%%  from any local clients.
%%
%% Message exchange protocol:
%%  * server -> broker: {sell, Module, Seller, Capacity}
%%  * server -> broker: {'DOWN', ...}
%%  * plb -> broker: {buy, Module, Buyer, Quantity}
%%  * broker -> plb: {complete_order, Sellers}
%%  * plb -> broker: {'DOWN', ...}
%%  * exchange -> broker: remote exchange executes the order (fully or partially)
%%  * exchange -> broker: {'DOWN', ...}
%%  * authority -> broker: remote authority starts (connects)
%%  * authority -> broker: remote authority updates a list of exchanges for a module
%%  * broker -> exchange: {sell, ...}, {buy, ...}
%%  * broker -> authority
%%
%% @end
-module(lambda_broker).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/0,
    authorities/1,
    authorities/2,
    %% broker API
    sell/3,
    sell/4,
    buy/3,
    buy/4,
    cancel/2,

    %% Internal API for exchange
    complete_order/4,

    %% Internal API for observability
    watch/2,
    unwatch/2,

    applications/1
]).

%% Introspection API
-export([
    exchanges/2,
    orders/2
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("kernel/include/logger.hrl").

-type sell_meta() :: #{
    module => lambda:meta()
}.

-type buy_meta() :: #{
    module => lambda:meta()
}.

-export_type([sell_meta/0, buy_meta/0]).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server and links it to calling process.
-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc returns list of authorities known to this broker
-spec authorities(lambda:dst()) -> [pid()].
authorities(Broker) ->
    gen_server:call(Broker, authorities).

%% @doc adds lists of authorities known
-spec authorities(lambda:dst(), #{pid() => lambda_discovery:address()}) -> ok.
authorities(Broker, Authorities) ->
    Broker ! {peers, Authorities},
    ok.

%%--------------------------------------------------------------------
%% Extended API

%% @doc Called by the server to "sell" some capacity. Creates a sell
%%      order, which is sent (updated) to all exchanges serving the
%%      Module. If there are no exchanges known, authorities are
%%      queried.
-spec sell(lambda:dst(), module(), pos_integer()) -> ok.
sell(Srv, Module, Quantity) ->
    sell(Srv, Module, Quantity, #{}).

%% @doc Sells some capacity of (potentially newly published) Module,
%%      with meta-information provided.
-spec sell(lambda:dst(), module(), pos_integer(), sell_meta()) -> ok.
sell(Srv, Module, Quantity, Meta) ->
    gen_server:cast(Srv, {sell, Module, self(), Quantity, Meta}).

%% @doc Creates an outstanding "buy" order, which is fanned out to
%%      all known exchanges serving the module.
%%      Asynchronous, buyer is expected to monitor the broker.
-spec buy(lambda:dst(), module(), pos_integer()) -> ok.
buy(Srv, Name, Quantity) ->
    %% Pass self() explicitly to allow tricks with proxy-ing gen_server calls
    buy(Srv, Name, Quantity, #{}).

-spec buy(lambda:dst(), module(), pos_integer(), buy_meta()) -> ok.
buy(Srv, Name, Quantity, Meta) ->
    %% Pass self() explicitly to allow tricks with proxy-ing gen_server calls
    gen_server:cast(Srv, {buy, Name, self(), Quantity, Meta}).

%% @doc Cancels an outstanding order. Asynchronous.
-spec cancel(lambda:dst(), pid()) -> ok.
cancel(Srv, Proc) ->
    gen_server:cast(Srv, {cancel, Proc}).

%% @doc Called by an exchange, to notify buyer's seller of a
%%      successful order completion.
-spec complete_order(lambda:dst(), module(), non_neg_integer(), []) -> ok | noconnect | nosuspend.
complete_order(Srv, Module, Id, Sellers) ->
    erlang:send(Srv, {complete_order, Module, Id, Sellers}, [noconnect, nosuspend]).

%%--------------------------------------------------------------------
%% Introspection API
-spec exchanges(lambda:dst(), module()) -> [pid()].
exchanges(Srv, Module) ->
    gen_server:call(Srv, {exchanges, Module}).

-type filter() :: #{
    type => sell | buy
}.

-spec orders(lambda:dst(), filter()) -> [pid()].
orders(Srv, Filters) ->
    gen_server:call(Srv, {orders, Filters}).

-spec watch(lambda:dst(), pid()) -> reference() | {error, already_exists}.
watch(Srv, Observer) ->
    gen_server:call(Srv, {watch, Observer}).

-spec unwatch(lambda:dst(), pid()) -> ok.
unwatch(Srv, Observer) ->
    gen_server:cast(Srv, {unwatch, Observer}).

-spec applications(lambda:dst()) -> [atom()].
applications(Srv) ->
    gen_server:call(Srv, applications).

%%--------------------------------------------------------------------
%% Implementation (gen_server)

-type order() :: {
    buy | sell,
    Id :: non_neg_integer(),
    pid(),
    Quantity :: pos_integer(),
    Meta :: sell_meta() | buy_meta(),
    Previous :: [pid()]         %% sellers that have been already forwarded
}.

-record(lambda_broker_state, {
    %% self address (used by authorities)
    self :: undefined | lambda_discovery:address(),
    %% authority processes + authority addresses to peer discovery
    authority = #{} :: #{pid() => lambda_discovery:address()},
    %% exchanges connections
    exchanges = #{} :: #{module() => [pid()]},
    %% next order ID (vector clock for this broker)
    next_id = 0 :: non_neg_integer(),
    %% outstanding orders for this module
    orders = #{} :: #{module() => [order()]},
    %% monitoring for orders and exchanges
    monitors = #{} :: #{pid() => {buy | sell | exchange, module(), reference()}},
    %% watchers: observability for the broker state, for debugging/visibility
    watchers = #{} :: #{pid() => reference()}
}).

-type state() :: #lambda_broker_state{}.

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, cache_self(#lambda_broker_state{})}.

%% debug: find authorities known
handle_call(authorities, _From, #lambda_broker_state{authority = Auth} = State) ->
    ?LOG_DEBUG("reporting authorities ~200p", [maps:keys(Auth)], #{domain => [lambda]}),
    {reply, maps:keys(Auth), State};
handle_call({exchanges, Module}, _From, #lambda_broker_state{exchanges = Exch} = State) ->
    ?LOG_DEBUG("reporting exchanges for ~s ~200p", [Module, maps:get(Module, Exch, [])], #{domain => [lambda]}),
    {reply, maps:get(Module, Exch, []), State};
handle_call({orders, Filters}, _From, #lambda_broker_state{orders = Orders} = State) ->
    ?LOG_DEBUG("reporting orders for ~200p", [Filters], #{domain => [lambda]}),
    {reply, filter_orders(Orders, Filters), State};
handle_call({watch, Observer}, _From, #lambda_broker_state{watchers = Watchers} = State) when is_map_key(Observer, Watchers)->
    {reply, {error, already_exists}, State};
handle_call({watch, Observer}, _From, #lambda_broker_state{watchers = Watchers} = State) ->
    MRef = erlang:monitor(process, Observer),
    {reply, MRef, State#lambda_broker_state{watchers = Watchers#{Observer => MRef}}};
handle_call(applications, _From, State) ->
    Apps = [{Name, Desc, Vsn} || {Name, Desc, Vsn} <- application:loaded_applications(),
        lists:prefix("adbmal", lists:reverse(Desc))], %% admbal <= lambda in reverse
    {reply, Apps, State}.

handle_cast({Type, Module, Trader, Quantity, Meta}, #lambda_broker_state{next_id = Id,
    exchanges = Exchanges, authority = Authority, orders = Orders, monitors = Monitors} = State0)
    when Type =:= buy; Type =:= sell ->
    State = cache_self(State0),
    case maps:find(Trader, Monitors) of
        {ok, {_Type, Module, _MRef}} ->
            ?LOG_DEBUG("~s received updated quantity from ~p for ~s (~b)", [Type, Trader, Module, Quantity], #{domain => [lambda]}),
            %% update outstanding sell order with new Quantity
            Outstanding = maps:get(Module, Orders),
            {Type, XId, Trader, _XQ, XMeta, XPrev} = lists:keyfind(Trader, 3, Outstanding),
            NewOrders = lists:keyreplace(Trader, 3, Outstanding, {Type, XId, Trader, Quantity, XMeta, XPrev}),
            %% notify known exchanges
            Exch = maps:get(Module, Exchanges, []),
            [lambda_exchange:sell(Ex, XId, Quantity, XMeta, {Trader, State#lambda_broker_state.self}) || Ex <- Exch],
            {noreply, State#lambda_broker_state{
                orders = Orders#{Module => NewOrders}}};
        error ->
            ?LOG_DEBUG("~s ~b ~s for ~p (exchanges: ~p)", [Type, Quantity, Module, Trader, Exchanges], #{domain => [lambda]}),
            %% monitor seller
            MRef = erlang:monitor(process, Trader),
            NewMons = Monitors#{Trader => {Type, Module, MRef}},
            Existing = maps:get(Module, Orders, []),
            %% request exchanges from all authorities (unless already known)
            case maps:find(Module, Exchanges) of
                {ok, Exch} when Type =:= buy ->
                    %% already subscribed, send orders to known exchanges
                    [lambda_exchange:buy(Ex, Id, Quantity, Meta) || Ex <- Exch];
                {ok, Exch} when Type =:= sell ->
                    %% already subscribed, send orders to known exchanges
                    [lambda_exchange:sell(Ex, Id, Quantity, Meta, {Trader, State#lambda_broker_state.self}) || Ex <- Exch];
                error ->
                    %% not subscribed to any exchanges yet
                    subscribe_exchange(Authority, Module)
            end,
            NewOrders = [{Type, Id, Trader, Quantity, Meta, []} | Existing],
            {noreply, State#lambda_broker_state{next_id = Id + 1, monitors = NewMons,
                orders = Orders#{Module => NewOrders}}}
    end;

%% handles cancellations for both sell and buy orders (should it be split?)
handle_cast({cancel, Trader}, State) ->
    {noreply, cancel(Trader, true, State)};

%% handles 'unwatch'
handle_cast({unwatch, Observer}, #lambda_broker_state{watchers = Watchers} = State) ->
    case maps:take(Observer, Watchers) of
        {Ref, NewWatchers} ->
            erlang:demonitor(Ref, [flush]),
            {noreply, State#lambda_broker_state{watchers = NewWatchers}};
        error ->
            {noreply, State}
    end.

handle_info({peers, Peers}, #lambda_broker_state{authority = Auth} = State0) ->
    %% initial discovery
    ?LOG_DEBUG("discovering authorities ~200p", [Peers], #{domain => [lambda]}),
    State = cache_self(State0),
    discover(Auth, Peers, State#lambda_broker_state.self),
    {noreply, State};

handle_info({authority, NewAuth, _AuthAddr, _Peers}, #lambda_broker_state{authority = Auth} = State)
    when is_map_key(NewAuth, Auth) ->
    ?LOG_DEBUG("duplicate authority ~s (from ~200p)", [node(NewAuth), _AuthAddr], #{domain => [lambda]}),
    {noreply, State};

%% authority discovered
handle_info({authority, Authority, AuthAddr, Peers}, #lambda_broker_state{authority = Auth} = State0) ->
    ?LOG_DEBUG("got authority ~s:~p (~200p)", [node(Authority), Authority, AuthAddr], #{domain => [lambda]}),
    _MRef = erlang:monitor(process, Authority),
    State = cache_self(State0),
    %% new authority may know more exchanges for outstanding orders
    [subscribe_exchange(#{Authority => []}, Mod) || Mod <- maps:keys(State#lambda_broker_state.orders)],
    %% extra discovery for authorities known remotely but not locally
    NewAuth = Auth#{Authority => AuthAddr},
    %% notify those interested in watching broker state
    notify_watchers(authority, NewAuth, Peers, State0#lambda_broker_state.watchers),
    %% continue discovering more authorities
    discover(NewAuth, Peers, State#lambda_broker_state.self),
    {noreply, State#lambda_broker_state{authority = NewAuth}};

%% exchange list updates for Module
handle_info({exchange, Module, Exch}, #lambda_broker_state{exchanges = Exchanges, orders = Orders} = State0) ->
    State = cache_self(State0),
    Outstanding = maps:get(Module, Orders),
    Known = maps:get(Module, Exchanges, []),
    %% send updates for outstanding orders to added exchanges
    NewExch = Exch -- Known,
    NewExch =/= [] andalso
        ?LOG_DEBUG("new exchanges for ~s: ~200p (~200p), outstanding: ~300p", [Module, NewExch, Exch, Outstanding], #{domain => [lambda]}),
    [case Type of
         buy -> lambda_exchange:buy(Ex, Id, Quantity, Meta);
         sell -> lambda_exchange:sell(Ex, Id, Quantity, Meta, {Trader, State#lambda_broker_state.self})
     end || Ex <- NewExch, {Type, Id, Trader, Quantity, Meta, Prev} <- Outstanding,
        lists:member(Ex, Prev) =:= false],
    {noreply, State#lambda_broker_state{exchanges = Exchanges#{Module => NewExch ++ Known}}};

%% order complete (probably a partial completion, or a duplicate)
handle_info({complete_order, Module, Id, Sellers}, #lambda_broker_state{orders = Orders} = State) ->
    %% this is HORRIBLE codestyle, TODO: rewrite in Erlang, not in C :)
    case maps:find(Module, Orders) of
        {ok, Outstanding} ->
            %% find the order
            case lists:keysearch(Id, 2, Outstanding) of
                {value, {buy, Id, Buyer, Quantity, BuyMeta, Previous}} ->
                    ?LOG_DEBUG("order reply: id ~b for ~p with ~200p", [Id, Buyer, Sellers], #{domain => [lambda]}),
                    %% notify buyers if any order complete
                    case notify_buyer(Buyer, Quantity, Sellers, Previous, []) of
                        {QuantityLeft, AlreadyUsed} ->
                            %% incomplete, keep current state (updating remaining quantity)
                            Out = lists:keyreplace(Id, 1, Outstanding, {buy, Id, Buyer, QuantityLeft, BuyMeta, AlreadyUsed}),
                            {noreply, State#lambda_broker_state{orders = Orders#{Module => Out}}};
                        done ->
                            %% complete, may trigger exchange subscription removal
                            Out = lists:keydelete(Id, 1, Outstanding),
                            {noreply, State#lambda_broker_state{orders = Orders#{Module => Out}}}
                    end;
                false ->
                    %% buy order was canceled while in-flight to exchange
                    ?LOG_DEBUG("order reply: id ~b has no buyer", [Id], #{domain => [lambda]}),
                    {noreply, State}
            end;
        error ->
            %% buy order was canceled while in-flight to exchange
            ?LOG_DEBUG("order reply: id ~b has no module known for buyer", [Id], #{domain => [lambda]}),
            {noreply, State}
    end;


%% something went down: authority, seller, buyer
handle_info({'DOWN', _Mref, process, Pid, _Reason}, #lambda_broker_state{authority = Auth} = State) ->
    %% it's very rare for an authority to go down, but it's also very fast to check
    case maps:take(Pid, Auth) of
        {_, NewAuth} ->
            ?LOG_DEBUG("authority down: ~s ~p (~200p)", [node(Pid), Pid, _Reason], #{domain => [lambda]}),
            {noreply, State#lambda_broker_state{authority = NewAuth}};
        error ->
            ?LOG_DEBUG("canceling (down) order from ~p (~200p)", [Pid, _Reason], #{domain => [lambda]}),
            {noreply, cancel(Pid, false, State)}
    end;

%% Peer discover attempt: another client tries to discover authority, but
%%  hits the client instead. Send a list of known authorities to the client.
handle_info({discover, Peer, _Port}, #lambda_broker_state{authority = Authority} = State) when is_pid(Peer) ->
    Peer ! {authority, maps:values(Authority)},
    {noreply, State};

%% Discovery retry: when there are no authorities known, attempt to discover some
handle_info(discover, #lambda_broker_state{authority = Authority} = State) when Authority =:= #{} ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Internal implementation

discover(Existing, New, Self) ->
    maps:map(
        fun (Location, _Addr) when is_map_key(Location, Existing) ->
                ok;
            (Location, Addr) ->
                set_node(Location, Addr),
                lambda_authority:discover(Location, Self)
        end, New).

cache_self(#lambda_broker_state{self = undefined} = State) ->
    try State#lambda_broker_state{self = lambda_discovery:get_node()}
    catch exit:{noproc, _} ->
        State
    end;
cache_self(State) ->
    State.

set_node(Location, Addr) when is_pid(Location) ->
    set_node(node(Location), Addr);
set_node({Reg, Node}, Addr) when is_atom(Reg), is_atom(Node) ->
    set_node(Node, Addr);
set_node(Node, Addr) when is_atom(Node) ->
    try lambda_discovery:set_node(Node, Addr)
    catch exit:{noproc, _} -> ok end.

cancel(Pid, Demonitor, #lambda_broker_state{orders = Orders, monitors = Monitors, exchanges = Exchanges} = State) ->
    case maps:take(Pid, Monitors) of
        {{exchange, Module, _MRef}, NewMonitors} ->
            ?LOG_DEBUG("exchange ~p for ~s down, known: ~200p", [Pid, Module, maps:get(Module, Exchanges, error)], #{domain => [lambda]}),
            case maps:get(Module, Exchanges) of
                [Pid] ->
                    State#lambda_broker_state{exchanges = maps:remove(Module, Exchanges), monitors = NewMonitors};
                Pids ->
                    State#lambda_broker_state{exchanges = Exchanges#{Module => lists:delete(Pid, Pids)}, monitors = NewMonitors}
            end;
        {{Type, Module, MRef}, NewMonitors} ->
            ?LOG_DEBUG("cancel ~s order for ~s from ~p, orders: ~200p", [Type, Module, Pid, maps:get(Module, Orders)], #{domain => [lambda]}),
            %% demonitor if it's forced cancellation
            Demonitor andalso erlang:demonitor(MRef, [flush]),
            %% enumerate to find the order
            case maps:get(Module, Orders) of
                [{Type, Id, Pid, _Q, _Meta, _Prev}] ->
                    cancel_exchange_order(Type, Id, maps:get(Module, Exchanges)),
                    %% no orders left for this module, unsubscribe from exchanges
                    broadcast(State#lambda_broker_state.authority, {cancel, Module, self()}),
                    State#lambda_broker_state{orders = maps:remove(Module, Orders), monitors = NewMonitors};
                Outstanding ->
                    %% other orders are still in
                    {value, {Type, Id, Pid, _Q, _Meta, _Prev}, NewOut} = lists:keytake(Pid, 3, Outstanding),
                    cancel_exchange_order(Type, Id, maps:get(Module, Exchanges)),
                    State#lambda_broker_state{orders = Orders#{Module => NewOut}, monitors = NewMonitors}
            end
    end.

cancel_exchange_order(Type, Id, Exchanges) ->
    [lambda_exchange:cancel(Ex, Type, Id) || Ex <- Exchanges].

subscribe_exchange(Auth, Module) ->
    broadcast(Auth, {exchange, Module, self()}).

broadcast(Authority, Msg) ->
    [Auth ! Msg || Auth <- maps:keys(Authority)].

notify_buyer(Buyer, Quantity, [], Previous, Servers) ->
    %% partial, or fully duplicate?
    ?LOG_DEBUG("buyer ~p notification: ~200p", [Buyer, Servers], #{domain => [lambda]}),
    connect_sellers(Buyer, Servers),
    {Quantity, Previous};
notify_buyer(Buyer, Quantity, [{Seller, QSell, Meta} | Remaining], Previous, Servers) ->
    %% filter and discard duplicates
    case lists:member(Seller, Previous) of
        true ->
            notify_buyer(Buyer, Quantity, Remaining, Previous, Servers);
        false when Quantity =< QSell ->
            %% complete, in total
            ?LOG_DEBUG("buyer ~p complete (~b) notification: ~200p", [Buyer, Quantity, [{Seller, QSell} | Servers]], #{domain => [lambda]}),
            connect_sellers(Buyer, [{Seller, QSell, Meta} | Servers]),
            done;
        false ->
            %% not yet complete, but maybe more servers are there?
            notify_buyer(Buyer, Quantity - QSell, Remaining, [Seller | Previous], [{Seller, QSell, Meta} | Servers])
    end.

connect_sellers(_Buyer, []) ->
    ok;
connect_sellers(Buyer, Contacts) ->
    %% sellers contacts were discovered, ensure discovery knows contacts
    [set_node(Pid, Addr) || {{Pid, Addr}, _Quantity, _Meta} <- Contacts],
    %% filter our contact information
    Servers = [{Pid, Quantity, Meta} || {{Pid, _Addr}, Quantity, Meta} <- Contacts],
    %% pass on order to the actual buyer
    lambda_plb:complete_order(Buyer, Servers).

%%--------------------------------------------------------------------
%% Introspection internals
filter_orders(Orders, #{module := Mod} = Filters) ->
    filter_type(maps:get(Mod, Orders, []), Filters);
filter_orders(Orders, Filters) ->
    filter_type(maps:values(Orders), Filters).

filter_type(Orders, #{type := Type}) ->
    [Order || Order <- Orders, element(1, Order) =:= Type];
filter_type(Orders, _Filters) ->
    Orders.

notify_watchers(authority, Auth, Peers, Watchers) ->
    Connected = maps:keys(Auth),
    Undiscovered = maps:keys(Peers) -- Connected,
    [Watcher ! {Ref, authority, Connected, Undiscovered} || {Watcher, Ref} <- maps:to_list(Watchers)].