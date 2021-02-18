%% @doc
%% Lambda server.
%% Drawing analogy, lambda server is acceptor socket, spawning process
%%  per accepted connection.
%%
%% Lambda server publishes current capacity via local broker.
%%
%% Lambda server is always operational, and uses following
%%  message exchange protocol:
%% * plb -> server: {connect, Client, Quantity} - connection request
%% * connection -> server: {'EXIT', ...} - connection closed (capacity returned)
%% * server -> broker: {sell, Quantity} - update remaining quantity
%%
%% When there is capacity left, server spawns a new connection
%%  which will reply to the Client establishing monitored
%%  connection.
%% When there is no capacity left, server replies to the client
%%  directly (no connection created).
%%
%% @end
-module(lambda_listener).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/3
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
%% API implementation

-type options() :: #{
    capacity := pos_integer()
}.

-export_type([options/0]).

%% @doc Starts the server and links it to the caller.
-spec start_link(lambda:dst(), module(), options()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Broker, Module, Options) ->
    gen_server:start_link(?MODULE, [Broker, Module, Options], []).

%%-----------------------------------------------------------------
%% gen_server implementation

-record(lambda_listener_state, {
    module :: module(),
    %% total remaining capacity
    capacity :: non_neg_integer(),
    %% children (linked)
    conns = #{} :: #{pid() => pos_integer()},
    %% broker
    broker :: lambda:dst()
}).

init([Broker, Module, #{capacity := Capacity}]) ->
    %% monitor broker and sell some capacity
    _ = lambda_broker:sell(Broker, Module, Capacity, #{module => module_meta(Module)}),
    %% trap exists, as server acts as a supervisor
    process_flag(trap_exit, true),
    {ok, #lambda_listener_state{module = Module, capacity = Capacity, broker = Broker}}.

handle_call(get_count, _From, #lambda_listener_state{conns = Conns} = State) ->
    Sum = lists:foldl(
        fun (Child, {AF, AT}) ->
            {F, T} = gen_server:call(Child, get_count),
            {AF + F, AT + T} end, {0, 0}, maps:keys(Conns)),
    {reply, Sum, State}.

handle_cast(_Request, _State) ->
    erlang:error(notsup).

handle_info({'EXIT', Pid, _Reason}, #lambda_listener_state{module = Module, capacity = Capacity, conns = Conns, broker = Broker} = State) ->
    %% update outstanding "sell" order
    ?LOG_DEBUG("client ~p disconnected", [Pid], #{domain => [lambda]}),
    {Cap, NewConns} = maps:take(Pid, Conns),
    NewCap = Capacity + Cap,
    lambda_broker:sell(Broker, Module, NewCap),
    {noreply, State#lambda_listener_state{capacity = NewCap, conns = NewConns}};

handle_info({connect, To, _Cap}, #lambda_listener_state{capacity = 0} = State) ->
    ?LOG_DEBUG("client ~p wants ~b (nothing left)", [To, _Cap], #{domain => [lambda]}),
    To ! {error, no_capacity},
    {noreply, State};
handle_info({connect, To, Cap}, #lambda_listener_state{module = Module, conns = Conns, capacity = Capacity, broker = Broker} = State) ->
    ?LOG_DEBUG("client ~p wants ~b (~b left)", [To, Cap, Capacity], #{domain => [lambda]}),
    Allowed = min(Cap, Capacity),
    %% act as a supervisor here, starting child processes (connection handlers)
    {ok, Conn} = lambda_channel:start_link(To, Allowed),
    %% publish capacity update
    NewCap = Capacity - Cap,
    if NewCap =:= 0 -> lambda_broker:cancel(Broker, self()); true -> lambda_broker:sell(Broker, Module, NewCap) end,
    {noreply, State#lambda_listener_state{capacity = NewCap, conns = Conns#{Conn => Cap}}}.

%%--------------------------------------------------------------------
%% @private Collects meta-information (reflection) of a module to publish

module_meta(Module) ->
    Exported = [{F, Arity} || {F, Arity} <- Module:module_info(exports), F =/= module_info],
    %% hints explaining call/cast behaviour
    Attrs = [AttrList || {lambda, AttrList} <- Module:module_info(attributes)],
    #{md5 => Module:module_info(md5), exports => Exported, attributes => Attrs}.
