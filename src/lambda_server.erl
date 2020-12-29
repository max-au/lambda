%% @doc
%% Lambda server, doing accounting process work. Sends demand to
%%  connected clients.
%% When a new process is spawned, it sends a notification to the
%%  server, allowing monitoring and accounting.
%%
%% Lambda server publishes current capacity via local broker,
%%  with name passed as an argument. To protect from broker restarts,
%%  server monitors the broker, and reconnects when needed.
%%
%% @end
-module(lambda_server).
-author("maximfca@gmail.com").

%% API
-export([
    start/2,
    start_link/2
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports
-export([server/2, handle/4]).

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% API implementation

start(Module, Capacity) ->
    gen_server:start(?MODULE, [Module, Capacity], []).

start_link(Module, Capacity) ->
    gen_server:start_link(?MODULE, [Module, Capacity], []).

%%-----------------------------------------------------------------
%% gen_server implementation

-record(lambda_server_state, {
    module :: module(),
    %% total remaining capacity
    capacity :: pos_integer(),
    %% children monitored
    conns = #{} :: #{pid() => pos_integer()},
    %% broker connection (monitored)
    broker :: pid()
}).

-define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: server " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

init([Module, Capacity]) ->
    Broker = whereis(lambda_broker),
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
    ?dbg("Client ~p disconnected", [Pid]),
    {Cap, NewConns} = maps:take(Pid, Conns),
    NewCap = Capacity + Cap,
    lambda_broker:sell(Broker, Module, NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = NewConns}};

handle_info({connect, To, Cap}, #lambda_server_state{module = Module, conns = Conns, capacity = Capacity, broker = Broker} = State) ->
    ?dbg("Client connecting from: ~p", [To]),
    Allowed = min(Cap, Capacity),
    %% act as a supervisor here, starting child processes (connection handlers)
    {ok, Conn} = proc_lib:start_link(?MODULE, server, [To, Allowed]),
    %% publish capacity update
    NewCap = Capacity - Cap,
    lambda_broker:sell(Broker, Module, NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = Conns#{Conn => Cap}}}.

%%--------------------------------------------------------------------
%% Broker connection
%% TODO: detect terminating and restarting broker

%%--------------------------------------------------------------------
%% Server side implementation: server process

server(To, Capacity) ->
    monitor(process, To),
    proc_lib:init_ack({ok, self()}),
    server_loop(To, 0, 0, Capacity, Capacity).

server_loop(To, InFlight, Total, Processed, Capacity) when Processed < Capacity ->
    receive
        {started, Pid} ->
            erlang:monitor(process, Pid),
            server_loop(To, InFlight + 1, Total, Processed, Capacity);
        {'DOWN', _MRef, process, To, _Reason} ->
            %% client disconnected
            ok;
        {'DOWN', _MRef, process, _Pid, _Reason} ->
            %% worker exited
            server_loop(To, InFlight - 1, Total + 1, Processed + 1, Capacity);
        {'$gen_call', From, get_count} ->
            %% gen_server call hack, for testing mainly
            gen:reply(From, {InFlight, Total}),
            server_loop(To, InFlight, Total, Processed, Capacity)
    end;
server_loop(To, InFlight, Total, Demand, Capacity) ->
    To ! {demand, Demand, self()},
    ?dbg("demanding: ~b from ~p", [Demand, To]),
    server_loop(To, InFlight, Total, 0, Capacity).

%%--------------------------------------------------------------------
%% proxy to start client requests
handle(Sap, M, F, A) ->
    Sap ! {started, self()},
    ?dbg("Handling call: ~s:~s(~w)", [M, F, A]),
    erlang:apply(M, F, A).
