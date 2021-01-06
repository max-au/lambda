%% @doc
%% Local broker process. Connected to all authorities.
%% Connected to exchanges for which there are outstanding orders
%%  from any local clients.
%%
%% Message exchange protocol:
%%  * server -> broker: {sell, Module, Seller, Capacity}
%%  * server -> broker: {'DOWN', ...}
%%  * plb -> broker: {buy, Module, Buyer, Quantity}
%%  * broker -> plb: {order, Sellers}
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
    start_link/1,
    authorities/1,
    %% broker API
    sell/3,
    buy/3,
    cancel/2
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

%%--------------------------------------------------------------------
%% @doc
%% Starts the server and links it to calling process.
-spec start_link(lambda:points()) -> gen:start_ret().
start_link(Peers) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Peers, []).

%% @doc returns list of authorities known to this broker
-spec authorities(gen:emgr_name()) -> [pid()].
authorities(Broker) ->
    gen_server:call(Broker, authorities).

%%--------------------------------------------------------------------
%% Extended API

%% @doc Called by the server to "sell" some capacity. Creates a sell
%%      order, which is sent (updated) to all exchanges serving the
%%      Module. If there are no exchanges known, authorities are
%%      queried.
%%      Asynchronous, does not guarantee success, therefore Seller
%%      is expected to monitor the broker.
-spec sell(gen:emgr_name(), module(), pos_integer()) -> ok.
sell(Srv, Name, Quantity) ->
    gen_server:cast(Srv, {sell, Name, self(), Quantity}).

%% @doc Creates an outstanding "buy" order, which is fanned out to
%%      all known exchanges serving the module.
%%      Asynchronous, buyer is expected to monitor the broker.
-spec buy(gen:emgr_name(), module(), pos_integer()) -> ok.
buy(Srv, Name, Quantity) ->
    %% Pass self() explicitly to allow tricks with proxy-ing gen_server calls
    gen_server:cast(Srv, {buy, Name, self(), Quantity}).

%% @doc Cancels an outstanding order. Asynchronous.
-spec cancel(gen:emgr_name(), pid()) -> ok.
cancel(Srv, Proc) ->
    gen_server:cast(Srv, {cancel, Proc}).

%%--------------------------------------------------------------------
%% Implementation (gen_server)

-type order() :: {
    Id :: non_neg_integer(),
    buy | sell,
    pid(),
    Quantity :: pos_integer(),
    Previous :: [pid()]         %% sellers that have been already forwarded
}.

-record(lambda_broker_state, {
    %% self address (used by authorities)
    self :: lambda_discovery:address(),
    %% bootstrap process informatin
    boot :: gen:emgr_name(),
    %% authority processes + authority addresses to peer discovery
    authority = #{} :: #{pid() => lambda_discovery:address()},
    %% exchanges connections
    exchanges = #{} :: #{module() => [pid()]},
    %% next order ID (vector clock for this broker)
    next_id = 0 :: non_neg_integer(),
    %% outstanding orders for this module
    orders = #{} :: #{module() => [order()]},
    %% monitoring for orders and exchanges
    monitors = #{} :: #{pid() => {buy | sell | exchange, module(), reference()}}
}).

-type state() :: #lambda_broker_state{}.

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: broker " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

-spec init(lambda:points()) -> {ok, state()}.
init(Peers) ->
    %% bootstrap discovery: attempt to find authorities
    Self = lambda_discovery:get_node(),
    %% initial discovery
    discover(Peers, Self),
    {ok, #lambda_broker_state{self = Self, boot = Peers}}.

%% debug: find authorities known
handle_call(authorities, _From, #lambda_broker_state{authority = Auth} = State) ->
    {reply, maps:keys(Auth), State}.

handle_cast({Type, Module, Trader, Quantity}, #lambda_broker_state{self = Self, next_id = Id, exchanges = Exchanges, authority = Authority, orders = Orders, monitors = Monitors} = State)
    when Type =:= buy; Type =:= sell ->
    case maps:find(Trader, Monitors) of
        {ok, {_Type, Module, _MRef}} ->
            ?dbg("~s received updated quantity from ~p for ~s (~b)", [Type, Trader, Module, Quantity]),
            %% update outstanding sell order with new Quantity
            Outstanding = maps:get(Module, Orders),
            %% NewOrders = lists:keyreplace(Trader, 3, Orders, {Id, Type, Trader, Quantity, Ex}),
            NewOut = Outstanding,
            %% TODO: implement !!!
            %% notify known exchanges
            %% Exch = maps:get(Module, Exchanges, []),
            {noreply, State#lambda_broker_state{
                orders = Orders#{Module => NewOut}}};
        error ->
            ?dbg("~s ~b ~s for ~p", [Type, Quantity, Module, Trader]),
            %% monitor seller
            MRef = erlang:monitor(process, Trader),
            NewMons = Monitors#{Trader => {Type, Module, MRef}},
            Existing = maps:get(Module, Orders, []),
            %% request exchanges from all authorities (unless already known)
            case maps:find(Module, Exchanges) of
                {ok, Exch} when Type =:= buy ->
                    %% already subscribed, send orders to known exchanges
                    [lambda_exchange:buy(Ex, Id, Quantity) || Ex <- Exch];
                {ok, Exch} when Type =:= sell ->
                    %% already subscribed, send orders to known exchanges
                    [lambda_exchange:sell(Ex, {Trader, Self}, Id, Quantity) || Ex <- Exch];
                error ->
                    %% not subscribed to any exchanges yet
                    subscribe_exchange(Authority, Module)
            end,
            NewOrders = [{Id, Type, Trader, Quantity, []} | Existing],
            {noreply, State#lambda_broker_state{next_id = Id + 1, monitors = NewMons,
                orders = Orders#{Module => NewOrders}}}
    end;

%% handles cancellations for both sell and buy orders (should it be split?)
handle_cast({cancel, Trader}, State) ->
    {reply, cancel(Trader, true, State)}.

handle_info({authority, NewAuth, _AuthAddr, _MoreAuth}, #lambda_broker_state{authority = Auth} = State)
    when is_map_key(NewAuth, Auth) ->
    ?dbg("~s: ~p duplicate authority ~s (brings ~200p and ~200p)", [node(), self(), node(NewAuth), _AuthAddr, _MoreAuth]),
    {noreply, State};

%% authority discovered
handle_info({authority, NewAuth, AuthAddr, MoreAuth}, #lambda_broker_state{self = Self, authority = Auth} = State)
    when not is_map_key(NewAuth, Auth) ->
    ?dbg("authority ~s (~200p) has ~200p", [node(NewAuth), AuthAddr,  MoreAuth]),
    _MRef = erlang:monitor(process, NewAuth),
    %% new authority may know more exchanges for outstanding orders
    [subscribe_exchange(#{NewAuth => []}, Mod) || Mod <- maps:keys(State#lambda_broker_state.orders)],
    discover(MoreAuth, Self),
    {noreply, State#lambda_broker_state{authority = Auth#{NewAuth => AuthAddr}}};

%% exchange list updates for Module
handle_info({exchange, Module, Exch}, #lambda_broker_state{self = Self, exchanges = Exchanges, orders = Orders} = State) ->
    Outstanding = maps:get(Module, Orders),
    Known = maps:get(Module, Exchanges, []),
    %% send updates for outstanding orders to added exchanges
    NewExch = Exch -- Known,
    NewExch =/= [] andalso
        ?dbg("new exchanges for ~s: ~200p (~200p), outstanding: ~300p", [Module, NewExch, Exch, Outstanding]),
    [case Type of
         buy -> lambda_exchange:buy(Ex, Id, Quantity);
         sell -> lambda_exchange:sell(Ex, {Trader, Self}, Id, Quantity)
     end || Ex <- NewExch, {Id, Type, Trader, Quantity, Prev} <- Outstanding,
        lists:member(Ex, Prev) =:= false],
    {noreply, State#lambda_broker_state{exchanges = Exchanges#{Module => NewExch ++ Known}}};

%% order complete (probably a partial completion, or a duplicate)
handle_info({order, Id, Module, Sellers}, #lambda_broker_state{orders = Orders} = State) ->
    ?dbg("order reply: id ~b with ~200p", [Id, Sellers]),
    Outstanding = maps:get(Module, Orders),
    %% find the order
    {value, {Id, buy, Buyer, Quantity, Previous}} = lists:keysearch(Id, 1, Outstanding),
    %% notify buyers if any order complete
    case notify_buyer(Buyer, Quantity, Sellers, Previous, []) of
        {QuantityLeft, AlreadyUsed} ->
            %% incomplete, keep current state (updating remaining quantity)
            Out = lists:keyreplace(Id, 1, Outstanding, {Id, buy, Buyer, QuantityLeft, AlreadyUsed}),
            {noreply, State#lambda_broker_state{orders = Orders#{Module => Out}}};
        done ->
            %% complete, may trigger exchange subscription removal
            Out = lists:keydelete(Id, 1, Outstanding),
            {noreply, State#lambda_broker_state{orders = Orders#{Module => Out}}}
    end;

%% something went down: authority, seller, buyer
handle_info({'DOWN', _Mref, process, Pid, _Reason}, #lambda_broker_state{authority = Auth} = State) ->
    %% it's very rare for an authority to go down, but it's also very fast to check
    case maps:take(Pid, Auth) of
        {_, NewAuth} ->
            ?dbg("authority down: ~s ~p (~200p)", [node(Pid), Pid, _Reason]),
            {noreply, State#lambda_broker_state{authority = NewAuth}};
        error ->
            ?dbg("canceling (down) order from ~p (~200p)", [Pid, _Reason]),
            {noreply, cancel(Pid, false, State)}
    end;

%% Peer discover attempt: another client tries to discover authority, but
%%  hits the client instead. Send a list of known authorities to the client.
handle_info({discover, Peer, _Port}, #lambda_broker_state{authority = Authority} = State) when is_pid(Peer) ->
    Peer ! {authority, maps:values(Authority)},
    {noreply, State}.


%%--------------------------------------------------------------------
%% Internal implementation

discover(Points, Self) ->
    maps:map(
        fun (Location, Addr) ->
            ?dbg("~s discovering ~200p of ~300p", [node(), Location, Addr]),
            lambda_discovery:set_node(Location, Addr),
            Location ! {discover, self(), Self}
        end, Points).

cancel(Pid, Demonitor, #lambda_broker_state{orders = Orders, monitors = Monitors, exchanges = Exchanges} = State) ->
    case maps:take(Pid, Monitors) of
        {{exchange, Module, _MRef}, NewMonitors} ->
            ?dbg("exchange ~p for ~s down, known: ~200p", [Pid, Module, maps:get(Module, Exchanges, error)]),
            case maps:get(Module, Exchanges) of
                [Pid] ->
                    State#lambda_broker_state{exchanges = maps:remove(Module, Exchanges), monitors = NewMonitors};
                Pids ->
                    State#lambda_broker_state{exchanges = Exchanges#{Module => lists:delete(Pid, Pids)}, monitors = NewMonitors}
            end;
        {{Type, Module, MRef}, NewMonitors} ->
            ?dbg("cancel ~s order for ~s from ~p, orders: ~200p", [Type, Module, Pid, maps:get(Module, Orders)]),
            %% demonitor if it's forced cancellation
            Demonitor andalso erlang:demonitor(MRef, [flush]),
            %% enumerate to find the order
            case maps:get(Module, Orders) of
                [{_Id, Type, Pid, _Q, _Prev}] ->
                    %% no orders left for this module, unsubscribe from exchanges
                    broadcast(State#lambda_broker_state.authority, {cancel, Module, self()}),
                    State#lambda_broker_state{orders = maps:remove(Module, Orders), monitors = NewMonitors};
                Outstanding ->
                    %% other orders are still in
                    NewOut = lists:keydelete(Pid, 3, Outstanding),
                    State#lambda_broker_state{orders = Orders#{Module => NewOut}, monitors = NewMonitors}
            end
    end.

subscribe_exchange(Auth, Module) ->
    broadcast(Auth, {exchange, Module, self()}).

broadcast(Authority, Msg) ->
    [Auth ! Msg || Auth <- maps:keys(Authority)].

notify_buyer(Buyer, Quantity, [], Previous, Servers) ->
    %% partial, or fully duplicate?
    ?dbg("buyer ~p notification: ~200p", [Buyer, Servers]),
    connect_sellers(Buyer, Servers),
    {Quantity, Previous};
notify_buyer(Buyer, Quantity, [{Seller, QSell} | Remaining], Previous, Servers) ->
    %% filter and discard duplicates
    case lists:member(Seller, Previous) of
        true ->
            notify_buyer(Buyer, Quantity, Remaining, Previous, Servers);
        false when Quantity =< QSell ->
            %% complete, in total
            ?dbg("buyer ~p complete (~b) notification: ~200p", [Buyer, Quantity, [{Seller, QSell} | Servers]]),
            connect_sellers(Buyer, [{Seller, QSell} | Servers]),
            done;
        false ->
            %% not yet complete, but maybe more servers are there?
            notify_buyer(Buyer, Quantity - QSell, Remaining, [Seller | Previous], [{Seller, QSell} | Servers])
    end.

connect_sellers(_Buyer, []) ->
    ok;
connect_sellers(Buyer, Contacts) ->
    %% sellers contacts were discovered, ensure it can be resolved
    Servers = [{Pid, Quantity} || {{Pid, _Addr}, Quantity} <- Contacts],
    %% pass on order to the actual buyer
    Buyer ! {order, Servers}.
