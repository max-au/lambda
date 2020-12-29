%% @doc
%% Local broker process. Connected to all authorities.
%% Connected to exchanges for which there are outstanding orders
%%  from any local clients.
%%
%% Receives following input:
%%  * local server makes "sell" order
%%  * local plb makes "buy" order
%%  * local server/plb cancels order or terminates
%%  * remote exchange executes the order (fully or partially)
%%  * remote authority starts (connects)
%%  * remote authority updates a list of exchanges for a module
%%  * remote exchange disconnects
%% @end
-module(lambda_broker).
-author("maximfca@gmail.com").

%% API
-export([
    start/1,
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

%% Process location (similar to emgr_name()). Process ID (pid)
%%  designates both node name and ID. If process ID is not known,
%%  locally registered process name can be used.
-type location() :: pid() | {atom(), node()}.

%% Points: maps location to the address
-type points() :: #{location() => lambda_epmd:address()}.

-export_type([location/0, points/0]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server outside of supervision hierarchy.
%% Useful for testing in conjunction with 'peer'. Unlike start_link,
%%  throws immediately, without the need to unwrap the {ok, Pid} tuple.
%% Does not register name locally (testing it is!)
-spec start(points()) -> gen:start_ret().
start(Bootstrap) ->
    {ok, Pid} = gen_server:start(?MODULE, Bootstrap, []),
    Pid.

%% @doc
%% Starts the server and links it to calling process.
-spec start_link(points()) -> gen:start_ret().
start_link(Bootstrap) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Bootstrap, []).

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
-spec sell(gen:emgr_name(), module(), pos_integer()) -> ok.
sell(Srv, Name, Quantity) ->
    gen_server:call(Srv, {sell, Name, self(), Quantity}).

%% @doc Creates an outstanding "buy" order, which is fanned out to
%%      all known exchanges serving the module.
-spec buy(gen:emgr_name(), module(), pos_integer()) -> ok.
buy(Srv, Name, Quantity) ->
    %% Pass self() explicitly to allow tricks with proxy-ing gen_server calls
    gen_server:call(Srv, {buy, Name, self(), Quantity}).

%% @doc Cancels an outstanding order.
-spec cancel(gen:emgr_name(), pid()) -> ok.
cancel(Srv, Proc) ->
    gen_server:call(Srv, {cancel, Proc}).

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
    self :: lambda_epmd:address(),
    %% authority processes + authority addresses to peer discovery
    authority = #{} :: #{pid() => lambda_epmd:address()},
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

-define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: broker " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

-spec init(points()) -> {ok, state()}.
init(Bootstrap) ->
    %% bootstrap discovery: attempt to find authorities
    Self = lambda_epmd:get_node(node()),
    ?dbg("discovering ~200p", [Bootstrap]),
    %% initial discovery
    discover(Bootstrap, Self),
    {ok, #lambda_broker_state{self = Self}}.

handle_call({Type, Module, Seller, Quantity}, _From, #lambda_broker_state{monitors = Monitors} = State)
    when is_map_key(Seller, Monitors) ->
    ?dbg("~s received updated quantity for ~s (~b)", [Type, Module, Quantity]),
    %% update outstanding sell order with new Quantity
    {reply, no, State};
handle_call({Type, Module, Trader, Quantity}, _From, #lambda_broker_state{self = Self, next_id = Id, authority = Authority, orders = Orders, monitors = Monitors} = State)
    when Type =:= buy; Type =:= sell ->
    ?dbg("~s ~b ~s for ~p", [Type, Quantity, Module, Trader]),
    %% monitor seller
    MRef = erlang:monitor(process, Trader),
    NewMons = Monitors#{Trader => {Type, Module, MRef}},
    %% request exchanges from all authorities (unless already known)
    Existing = maps:get(Module, Orders, []),
    NewOrders =
        case maps:find(Module, State#lambda_broker_state.exchanges) of
            {ok, Exch} when Type =:= buy ->
                %% already subscribed, send orders to known exchanges
                [lambda_exchange:buy(Ex, Id, Quantity) || Ex <- Exch],
                [{Id, Type, Trader, Quantity, []} | Existing];
            {ok, Exch} when Type =:= sell ->
                %% already subscribed, send orders to known exchanges
                [lambda_exchange:sell(Ex, {self(), Self}, Id, Quantity) || Ex <- Exch],
                [{Id, Type, Trader, Quantity, []} | Existing];
            error ->
                %% not subscribed to any exchanges yet
                subscribe_exchange(Authority, Module),
                [{Id, Type, Trader, Quantity, []} | Existing]
        end,
    {reply, ok, State#lambda_broker_state{next_id = Id + 1, monitors = NewMons, orders = Orders#{Module => NewOrders}}};

%% handles cancellations for both sell and buy orders (should it be split?)
handle_call({cancel, Trader}, _From, State) ->
    {reply, ok, cancel(Trader, true, State)};

%% debug: find authorities known
handle_call(authorities, _From, #lambda_broker_state{authority = Auth} = State) ->
    {reply, maps:keys(Auth), State}.

handle_cast(_Cast, _State) ->
    error(notsup).

handle_info({authority, NewAuth, _AuthAddr, _MoreAuth}, #lambda_broker_state{authority = Auth} = State)
    when is_map_key(NewAuth, Auth) ->
    io:format(standard_error, "~s: ~p duplicate authority ~s (brings ~200p and ~200p)", [node(), self(), node(NewAuth), _AuthAddr, _MoreAuth]),
    {noreply, State};

%% authority discovered
handle_info({authority, NewAuth, AuthAddr, MoreAuth}, #lambda_broker_state{self = Self, authority = Auth} = State)
    when not is_map_key(NewAuth, Auth) ->
    ?dbg("authority ~s (~200p) has ~200p", [node(NewAuth), AuthAddr,  MoreAuth]),
    _MRef = erlang:monitor(process, NewAuth),
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
         sell -> lambda_exchange:sell(Ex, {Pid, Self}, Id, Quantity)
     end || Ex <- NewExch, {Id, Type, Pid, Quantity, Prev} <- Outstanding,
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
handle_info({'DOWN', _Mref, _, Pid, _Reason}, #lambda_broker_state{authority = Auth} = State) ->
    %% it's very rare for an authority to go down, but it's also very fast to check
    case maps:take(Pid, Auth) of
        {_, NewAuth} ->
            ?dbg("authority down: ~s ~p", [node(Pid), Pid]),
            {noreply, State#lambda_broker_state{authority = NewAuth}};
        error ->
            ?dbg("canceling order from ~p", [Pid]),
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
            ?LOG_DEBUG("~s discovering ~p of ~p", [node(), Location, Addr]),
            lambda_epmd:set_node(Location, Addr),
            Location ! {discover, self(), Self}
        end, Points).

cancel(Pid, Demonitor, #lambda_broker_state{orders = Orders, monitors = Monitors, exchanges = Exchanges} = State) ->
    case maps:take(Pid, Monitors) of
        {{exchange, Module, _MRef}, NewMonitors} ->
            case maps:get(Module, Exchanges) of
                [Pid] ->
                    State#lambda_broker_state{exchanges = maps:remove(Module, Exchanges), monitors = NewMonitors};
                Pids ->
                    State#lambda_broker_state{exchanges = Exchanges#{Module => lists:delete(Pid, Pids)}, monitors = NewMonitors}
            end;
        {{Type, Module, MRef}, NewMonitors} ->
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
                    NewOut = lists:keydelete(Pid, 2, Outstanding),
                    State#lambda_broker_state{orders = Orders#{Module => {Exchanges, NewOut}}, monitors = NewMonitors}
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
notify_buyer(Buyer, Quantity, [{Sale, Seller} | Remaining], Previous, Servers) ->
    %% filter and discard duplicates
    case lists:member(Seller, Previous) of
        true ->
            notify_buyer(Buyer, Quantity, Remaining, Previous, Servers);
        false when Quantity =< Sale ->
            %% complete, in total
            ?dbg("buyer ~p complete notification: ~200p", [Buyer, [{Sale, Seller} | Servers]]),
            connect_sellers(Buyer, [{Sale, Seller} | Servers]),
            done;
        false ->
            %% not yet complete, but maybe more servers are there?
            notify_buyer(Buyer, Quantity - Sale, Remaining, [Seller | Previous], [{Sale, Seller} | Servers])
    end.

connect_sellers(_Buyer, []) ->
    ok;
connect_sellers(Buyer, Contacts) ->
    %% sellers contacts were discovered, ensure it can be resolved
    Servers = [{Pid, Quantity} || {{Pid, _Addr}, Quantity} <- Contacts],
    %% pass on order to the actual buyer
    Buyer ! {order, Servers}.
