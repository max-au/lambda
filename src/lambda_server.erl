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
-module(lambda_server).
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

%% @doc Starts the server and links it to the caller.
-spec start_link(gen:emgr_name(), module(), options()) -> gen:start_ret().
start_link(Broker, Module, Options) ->
    gen_server:start_link(?MODULE, [Broker, Module, Options], []).

%%-----------------------------------------------------------------
%% gen_server implementation

-record(lambda_server_state, {
    module :: module(),
    %% total remaining capacity
    capacity :: pos_integer(),
    %% children (linked)
    conns = #{} :: #{pid() => pos_integer()},
    %% broker
    broker :: gen:emgr_name()
}).

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: server " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

init([Broker, Module, #{capacity := Capacity}]) ->
    %% monitor broker and sell some capacity
    _ = lambda_broker:sell(Broker, Module, Capacity),
    %% trap exists, as server acts as a supervisor
    process_flag(trap_exit, true),
    {ok, #lambda_server_state{module = Module, capacity = Capacity, broker = Broker}}.

handle_call(get_count, _From, #lambda_server_state{conns = Conns} = State) ->
    Sum = lists:foldl(
        fun (Child, {AF, AT}) ->
            {F, T} = gen_server:call(Child, get_count),
            {AF + F, AT + T} end, {0, 0}, maps:keys(Conns)),
    {reply, Sum, State}.

handle_cast(_Request, _State) ->
    erlang:error(notsup).

handle_info({'EXIT', Pid, _Reason}, #lambda_server_state{module = Module, capacity = Capacity, conns = Conns, broker = Broker} = State) ->
    %% update outstanding "sell" order
    ?dbg("client ~p disconnected", [Pid]),
    {Cap, NewConns} = maps:take(Pid, Conns),
    NewCap = Capacity + Cap,
    lambda_broker:sell(Broker, Module, NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = NewConns}};

handle_info({connect, To, _Cap}, #lambda_server_state{capacity = 0} = State) ->
    ?dbg("client ~p wants ~b (nothing left)", [To, _Cap]),
    To ! {error, no_capacity},
    {noreply, State};
handle_info({connect, To, Cap}, #lambda_server_state{module = Module, conns = Conns, capacity = Capacity, broker = Broker} = State) ->
    ?dbg("client ~p wants ~b (~b left)", [To, Cap, Capacity]),
    Allowed = min(Cap, Capacity),
    %% act as a supervisor here, starting child processes (connection handlers)
    {ok, Conn} = lambda_channel:start_link(To, Allowed),
    %% publish capacity update
    NewCap = Capacity - Cap,
    lambda_broker:sell(Broker, Module, NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = Conns#{Conn => Cap}}}.
