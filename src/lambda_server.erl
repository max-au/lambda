%% @doc
%% Server side: SAP, server accounting process
%% Currently implements supervisor pattern (probably should not).
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

start(Name, Capacity) ->
    gen_server:start(?MODULE, [Name, Capacity], []).

start_link(Name, Capacity) ->
    gen_server:start_link(?MODULE, [Name, Capacity], []).

%%-----------------------------------------------------------------
%% gen_server implementation

-record(lambda_server_state, {
    scope :: term(),
    %% total remaining capacity
    capacity :: pos_integer(),
    %% children monitored
    conns = #{} :: #{pid() => pos_integer()},
    %% brokers this server connected to
    brokers = #{} :: #{pid() => reference()}
}).

init([Scope, Capacity]) ->
    %% monitor brokers, globally
    Brokers = lambda_registry:subscribe(Scope, self()),
    BrokerMons = maps:from_list([{B, erlang:monitor(process, B)} || B <- Brokers]),
    %% publish total capacity
    lambda_broker:sell(Brokers, Capacity),
    %% trap exists, as server acts as a supervisor
    process_flag(trap_exit, true),
    {ok, #lambda_server_state{scope = Scope, capacity = Capacity, brokers = BrokerMons}}.

handle_call(get_count, _From, #lambda_server_state{conns = Conns} = State) ->
    Sum = lists:foldl(
        fun (Child, {AF, AT}) ->
            {F, T} = gen_server:call(Child, get_count),
            {AF + F, AT + T} end, {0, 0}, maps:keys(Conns)),
    {reply, Sum, State}.

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({'EXIT', Pid, _Reason}, #lambda_server_state{capacity = Capacity, conns = Conns, brokers = Brokers} = State) ->
    %% update brokers for changed capacity
    ?LOG_DEBUG("Client ~p disconnected", [Pid]),
    {Cap, NewConns} = maps:take(Pid, Conns),
    NewCap = Capacity + Cap,
    lambda_broker:sell(maps:keys(Brokers), NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = NewConns}};

handle_info({connect, To, Cap}, #lambda_server_state{conns = Conns, capacity = Capacity, brokers = Brokers} = State) ->
    ?LOG_DEBUG("Client connecting from: ~p", [To]),
    Allowed = min(Cap, Capacity),
    %% act as a supervisor here, starting child processes (connection handlers)
    {ok, Conn} = proc_lib:start_link(?MODULE, server, [To, Allowed]),
    %% publish capacity update to all brokers
    NewCap = Capacity - Cap,
    lambda_broker:sell(maps:keys(Brokers), NewCap),
    {noreply, State#lambda_server_state{capacity = NewCap, conns = Conns#{Conn => Cap}}}.

%%--------------------------------------------------------------------
%% Broker connection

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
    ?LOG_DEBUG("demanding: ~b from ~p~n", [Demand, To]),
    server_loop(To, InFlight, Total, 0, Capacity).

%%--------------------------------------------------------------------
%% proxy to start client requests
handle(Sap, M, F, A) ->
    Sap ! {started, self()},
    ?LOG_DEBUG("Handling call: ~s:~s(~w)", [M, F, A]),
    erlang:apply(M, F, A).
