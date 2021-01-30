%% @doc
%%  Property-based tests exercising all features of lambda.
%% @end
-module(lambda_SUITE).
-author("maximfca@gmail.com").

-include_lib("stdlib/include/assert.hrl").

%% Test server callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases
-export([
    sequential_prop/0, sequential_prop/1
]).

%% PropEr state machine
-export([
    initial_state/0,
    command/1,
    precondition/2,
    next_state/3,
    postcondition/3
]).

%% -------------------------------------------------------------------
%% Test suite settings
init_per_suite(Config) ->
    {ok, Disco} = lambda_discovery:start_link(),
    unlink(Disco),
    [{disco, Disco} | Config].

end_per_suite(Config) ->
    gen_server:stop(proplists:get_value(disco, Config)),
    proplists:delete(disco, Config).

all() ->
    [].

%% -------------------------------------------------------------------
%% Test cases

sequential_prop() ->
    [{doc, "Property-based test ensuring system reliability"},
        {timetrap, {seconds, 600}}].

sequential_prop(Config) when is_list(Config) ->
    erlang:system_flag(backtrace_depth, 100),
    Online = 1, %% erlang:system_info(schedulers_online) * 2,
    run_concurrent(500, 10, 50, Online, prop_sequential(self())),
    ok.

%% -------------------------------------------------------------------
%% Concurrency primitive

run_concurrent(TestCount, StartSize, MaxSize, Concurrency, Property) ->
    Control = self(),
    TestsPerProcess = TestCount div Concurrency,
    %% First process receives additional items
    Extra = TestCount - (TestsPerProcess * Concurrency),
    TestProcs = [TestsPerProcess + Extra | [TestsPerProcess || _ <- lists:seq(2, Concurrency)]],
    Workers =
        [
            proc_lib:spawn_link(
                fun () ->
                    Result = proper:quickcheck(Property,
                        [{to_file, user}, {numtests, NumTests}, {max_size, MaxSize}, {start_size, StartSize}, long_result]),
                    Control ! {result, self(), Result}
                end
            ) || NumTests <- TestProcs
        ],
    wait_result(erlang:system_time(second), 0, TestCount, Workers).

wait_result(_Started, _Done, _Total, []) ->
    ok;
wait_result(Started, Done, Total, Workers) ->
    receive
        progress ->
            Done rem 50 == 49 andalso
                begin
                    Done1 = Done + 1,
                    Now = erlang:system_time(second),
                    case Now - Started of
                        0 ->
                            ok;
                        Passed ->
                            Speed = Done1 / Passed,
                            Remaining = erlang:round((Total - Done1) / Speed),
                            io:fwrite(user, "~b of ~b complete (elapsed ~s, ~s left, ~.2f per second) ~n",
                                [Done1, Total, format_time(Passed), format_time(Remaining), Speed])
                    end
                end,
            wait_result(Started, Done + 1, Total, Workers);
        {result, Pid, true} ->
            wait_result(Started, Done, Total, lists:delete(Pid, Workers));
        {result, _Pid, {error, {Result, State, History, Cmds}}} ->
            Failed = if History =:= [] -> hd(Cmds); true -> catch lists:nth(length(History), Cmds) end,
            io:fwrite(user,
                "=======~nCommand: ~120p~n~nState: ~120p~n~n=======~nResult: ~120p~n~nCommands: ~120p~n~nHistory: ~200p~n~n",
                [Failed, State, Result, Cmds, History]),
            {fail, {error, Result}};
        {result, _Pid, {error, Reason}} ->
            {fail, {error, Reason}};
        {result, _Pid, CounterExample} ->
            {fail, {commands, CounterExample}}
    end.

format_time(Timer) when Timer > 3600 ->
    io_lib:format("~2..0b:~2..0b:~2..0b", [Timer div 3600, Timer rem 3600 div 60, Timer rem 60]);
format_time(Timer) when Timer > 60 ->
    io_lib:format("~2..0b:~2..0b", [Timer div 60, Timer rem 60]);
format_time(Timer) ->
    io_lib:format("~2b sec", [Timer]).


%% -------------------------------------------------------------------
%% Properties

prop_sequential(Control) ->
    proper:forall(proper_statem:commands(?MODULE),
        fun (Cmds) ->
            {History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
            %% report progress
            Control ! progress,
            %% XXX: remove
            Result =/= ok andalso
                begin
                    Failed = if History =:= [] -> hd(Cmds); true -> catch lists:nth(length(History), Cmds) end,
                    io:fwrite(user,
                        "=======~nCommand: ~120p~n~nState: ~120p~n~n=======~nResult: ~120p~n~nCommands: ~120p~n~nHistory: ~200p~n~n",
                        [Failed, State, Result, Cmds, History])
                end,
            %%%% cleanup
            cleanup(State),
            Result =:= ok
        end
    ).

%% -------------------------------------------------------------------
%% Requests
%make_request() -> ok.
%complete_request() -> ok.
%fail_request() -> ok.

%% -------------------------------------------------------------------
%% State machine

-record(lambda_state, {
    %% authority nodes
    authorities = [] :: [pid()],
    %% broker nodes
    brokers = [] :: [pid()],
    %% listener must be located on one of the broker nodes
    listeners = [] :: [pid()],
    %% plb must also have a corresponding broker
    plb = [],
    %% requests
    requests
}).

%% State cleanup
cleanup(#lambda_state{authorities = Auth, brokers = Brokers, listeners = Servers}) ->
    [gen_server:stop(Pid) || Pid <- Servers ++ Brokers ++ Auth].

%% Limits
-define (MAX_AUTHORITY, 8).
-define (MAX_BROKERS, 16).
-define (MAX_SERVERS, 8).
-define (MAX_PLB, 32).
-define (MAX_SERVER_CAPACITY, 100).
-define (MAX_PLB_CAPACITY, 50).
-define (MODULES, [one, two, three, four]).

initial_state() ->
    #lambda_state{}.

precondition(#lambda_state{authorities = Auth}, {call, gen_server, start, [lambda_authority, _Peers, []]}) ->
    length(Auth) < ?MAX_AUTHORITY;

precondition(#lambda_state{brokers = Brokers}, {call, gen_server, start, [lambda_broker, _Peers, []]}) ->
    length(Brokers) < ?MAX_BROKERS;

precondition(#lambda_state{brokers = Brokers, listeners = Servers}, {call, gen_server, start, [lambda_listener, [Broker, _Module, _Capacity], []]}) ->
    length(Servers) < ?MAX_SERVERS andalso lists:member(Broker, Brokers);

precondition(#lambda_state{brokers = Brokers, listeners = Servers}, {call, gen_server, start, [lambda_plb, [Broker, _Module, _Options], []]}) ->
    length(Servers) < ?MAX_PLB andalso lists:member(Broker, Brokers);

precondition(#lambda_state{plb = Plb}, {call, ?MODULE, make_request, []}) ->
    Plb =/= [];

%%precondition(#lambda_state{requests = Requests}, {call, ?MODULE, complete_request, []}) ->
%%    lists:member(Req, Requests);

precondition(#lambda_state{authorities = Auth, brokers = Brokers}, {call, gen_server, stop, [Pid]}) ->
    lists:member(Pid, Auth) orelse lists:member(Pid, Brokers).

postcondition(_State, _Cmd, _Res) ->
    %% io:format(user, "~200p => ~200p~n~200p~n", [_Cmd, _Res, _State]),
    true.

%% Events possible:
%%  * start/stop an authority/broker/listener/plb
%%  * request created/completed/crashed
%%  * net splits [NOT IMPLEMENTED]
%% Command generation logic re-implements "precondition" logic
command(#lambda_state{authorities = Auth, brokers = Brokers, plb = _Plb, requests = _Requests}) ->
    %% nodes starting (authority/broker)
    AuthorityStart = {1, {call, gen_server, start, [lambda_authority, [], []]}},
    BrokerStart = {5, {call, gen_server, start, [lambda_broker, [], []]}},
    %% listener/plb starting (requires a broker)
    ServerOrPlbStart =
        case Brokers of
            [] ->
                [];
            Brokers ->
                [
                    {10, {call, gen_server, start, [lambda_listener, [proper_types:oneof(Brokers), proper_types:oneof(?MODULES), #{capacity => proper_types:range(1, ?MAX_SERVER_CAPACITY)}], []]}},
                    {10, {call, gen_server, start, [lambda_plb, [proper_types:oneof(Brokers), proper_types:oneof(?MODULES), #{high => proper_types:range(1, ?MAX_PLB_CAPACITY)}], []]}}
                ]
        end,
    %% stop: anything that is running can be stopped at will
    Stop =
        case Auth ++ Brokers of
            [] ->
                [];
            Pids ->
                [{5, {call, gen_server, stop, [proper_types:oneof(Pids)]}}]
        end,
    %% new requests must be routed through a plb
    %StartRequest =
    %    case Plb of
    %        [] ->
    %            [];
    %        Plb ->
    %            [{5, {call, ?MODULE, make_request, []}}]
    %    end,
    %FinishRequest =
    %    case Requests of
    %        [] ->
    %            [];
    %        Requests ->
    %            [
    %                {5, {call, ?MODULE, complete_request, [proper_types:oneof(Requests)]}},
    %                {5, {call, ?MODULE, crash_request, [proper_types:oneof(Requests)]}}
    %            ]
    %    end,
    %% make your choice, Mr PropEr
    Choices = [AuthorityStart, BrokerStart | ServerOrPlbStart ++ Stop],%% ++ StartRequest ++ FinishRequest],
    proper_types:frequency(Choices).

next_state(#lambda_state{authorities = Auth} = State, Res, {call, gen_server, start, [lambda_authority, [], []]}) ->
    NewAuth = {call, erlang, element, [2, Res]},
    State#lambda_state{authorities = [NewAuth | Auth]};

next_state(#lambda_state{brokers = Brokers} = State, Res, {call, gen_server, start, [lambda_broker, [], []]}) ->
    NewBroker = {call, erlang, element, [2, Res]},
    State#lambda_state{brokers = [NewBroker | Brokers]};

next_state(#lambda_state{listeners = Servers} = State, Res, {call, gen_server, start, [lambda_listener, [_, _, _], []]}) ->
    NewServer = {call, erlang, element, [2, Res]},
    State#lambda_state{listeners = [NewServer | Servers]};

next_state(#lambda_state{plb = Plb} = State, Res, {call, gen_server, start, [lambda_plb, [_, _, _], []]}) ->
    NewPlb = {call, erlang, element, [2, Res]},
    State#lambda_state{plb = [NewPlb | Plb]};

next_state(#lambda_state{authorities = Auth, brokers = Brokers} = State, _Res, {call, gen_server, stop, [Pid]}) ->
    %% just remove Pid from all processes known
    State#lambda_state{authorities = lists:delete(Pid, Auth), brokers = lists:delete(Pid, Brokers)}.