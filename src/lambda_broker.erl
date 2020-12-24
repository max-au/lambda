%% @doc
%%  Lambda broker, matching client requests to servers.
%%  Broker operations:
%%   - sell (pid, scope, capacity) - a pid wants to sell capacity in scope
%%   - buy (pid, scope, capacity) - a pid wants to buy capacity in scope
%%
%%  Servers send "sell" orders when their capacity changes (e.g. when client
%%   disconnects, or there is manual capacity change). At any time server can
%%   cancel sell order (or disconnect, which triggers the cancellation).
%%  Servers are monitored for the entire duration.
%%
%%  Clients send "buy" orders, which can be fulfilled immediately, or put
%%   in the waiting orders (any client that has waiting order is monitored).
%%  When client order is fulfilled, broker stops monitoring the client.
%%  Client can cancel the order (or disconnect).
%%
%% A broker serves one (single) scope. Server updates all brokers when
%%  capacity changes (e.g. new client has connected).
%%
%% Redundancy: server publishes to all brokers serving a specific state.
%%  Client receives full or partial responses, and makes connections to
%%  servers.
%%
%% Broker match algorithm considers:
%%  * capacity
%%  * failure domains
%%  * previous allocations
%%
%% @end
-module(lambda_broker).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/1,
    sell/2,
    buy/2
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
-spec start_link(module()) -> gen:start_ret().
start_link(Scope) when is_atom(Scope) ->
    gen_server:start_link(?MODULE, Scope, []).

%%--------------------------------------------------------------------
%% API

sell(Broker, Capacity) ->
    erlang:send(Broker, {sell, Capacity, self()}, [noconnect, nosuspend]).

buy(Broker, Capacity) ->
    erlang:send(Broker, {buy, Capacity, self()}, [noconnect, nosuspend]).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_broker_state, {
    %% map servers to their remaining capacities
    servers = #{} :: #{pid() => non_neg_integer()},
    %% client orders not yet fulfilled
    orders = #{} :: #{pid() => {MRef :: reference(), Cap :: pos_integer()}}
}).

-type state() :: #lambda_broker_state{}.

-spec init([]) -> {ok, state()}.
init(Scope) ->
    %% publish broker to the cluster
    yes = lambda_registry:register_name(Scope, self()),
    {ok, #lambda_broker_state{}}.

handle_call(_Req, _From, #lambda_broker_state{}) ->
    error(notsup).

handle_cast(_Req, _State) ->
    error(notsup).

handle_info({buy, Capacity, From}, #lambda_broker_state{servers = Servers} = State) ->
    %% match existing capacity
    ?LOG_DEBUG("~p: buys ~b", [From, Capacity]),
    buy_impl(From, Capacity, maps:next(maps:iterator(Servers)), [], State);

handle_info({sell, Capacity, Server}, #lambda_broker_state{orders = Orders} = State) ->
    %% match any outstanding orders for this scope
    ?LOG_DEBUG("~p: sells ~b", [Server, Capacity]),
    sell_impl(Capacity, Server, maps:next(maps:iterator(Orders)), State);

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_broker_state{servers = Servers} = State) ->
    case maps:take(Pid, Servers) of
        {_Capacity, NewSrv} ->
            ?LOG_DEBUG("~p: server down, removed ~b capacity", [Pid, _Capacity]),
            %% server down: remove it from available capacity
            {noreply, State#lambda_broker_state{servers = NewSrv}};
        error ->
            %% cancel outstanding order for that process
            {{MRef, _Req}, NewOrders} = maps:take(Pid, State#lambda_broker_state.orders),
            ?LOG_DEBUG("~p: client down, removed order for ~b capacity", [Pid, _Req]),
            erlang:demonitor(MRef, [flush]),
            {noreply, State#lambda_broker_state{orders = NewOrders}}
    end.

%%--------------------------------------------------------------------
%% Internal implementation

%% BUY: match any existing sell orders
buy_impl(From, Remain, none, Partial, #lambda_broker_state{orders = Orders} = State) ->
    %% was it partially done?
    Partial =/= [] andalso begin From ! {order, Partial} end,
    %% store the order, not yet ready to fulfill
    ?LOG_DEBUG("~p: not fullfilled (partial: ~p), monitoring order", [From, Partial]),
    MRef = erlang:monitor(process, From),
    {noreply, State#lambda_broker_state{orders = Orders#{From => {MRef, Remain}}}};
buy_impl(From, Remain, {_Server, 0, Next}, Partial, State) ->
    %% server is full, TODO: optimise this flow
    buy_impl(From, Remain, maps:next(Next), Partial, State);
buy_impl(From, Remain, {Server, Cap, Next}, Partial, #lambda_broker_state{servers = Srv} = State) when Remain > Cap ->
    %% partial fulfillment
    %% continue with the order
    buy_impl(From, Remain - Cap, maps:next(Next), [{Server, Cap} | Partial], State#lambda_broker_state{servers = Srv#{Server => Cap}});
buy_impl(From, Remain, {Server, Cap, _Next}, Partial, #lambda_broker_state{servers = Srv} = State) ->
    %% order complete in full
    From ! {order, [{Server, Remain} | Partial]},
    ?LOG_DEBUG("~p: immediate buy complete (~p) for ~b", [From, Server, Remain]),
    {noreply, State#lambda_broker_state{servers = Srv#{Server => Cap - Remain}}}.

%% SELL: figure out if there are outstanding orders
sell_impl(Capacity, Server, none, State) ->
    %% no orders left
    sell_complete(Server, Capacity, State);
sell_impl(0, Server, _Iter, State) ->
    %% no capacity left
    sell_complete(Server, 0, State);
sell_impl(Capacity, Server, {To, {MRef, Cap}, _Iter}, #lambda_broker_state{orders = Orders} = State) ->
    case Capacity - Cap of
        Remain when Remain >= 0 ->
            %% order fulfilled
            erlang:demonitor(MRef, [flush]),
            New = maps:remove(To, Orders),
            To ! {order, [{Server, Cap}]},
            sell_impl(Remain, Server, maps:next(maps:iterator(New)), State#lambda_broker_state{orders = New});
        _Neg ->
            %% update and keep the order
            To ! {order, [{Server, Capacity}]},
            sell_complete(Server, 0, State#lambda_broker_state{orders = Orders#{To => {MRef, Cap - Capacity}}})
    end.

sell_complete(From, Cap, #lambda_broker_state{servers = Srv} = State) ->
    %% always monitor server process
    is_map_key(From, Srv) orelse monitor(process, From),
    {noreply, State#lambda_broker_state{servers = Srv#{From => Cap}}}.
