%% @doc
%%     Smoke test for probabilistic load balancer.
%% @end
-module(lambda_plb_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases exports
-export([
    lb/0, lb/1,
    fail_capacity_wait/0, fail_capacity_wait/1,
    call_fail/0, call_fail/1,
    packet_loss/0, packet_loss/1,
    throughput/0, throughput/1
]).

%% Exports for spawned (peer) node
-export([
    call_fail_test/1,
    lb_test/4
]).

%% Should not be exported in the future
-export([
    pi/1,
    sleep/1,
    echo/1,
    exit/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    %% packet_loss is currently unresolved,
    %%  it appears to be impossible to make it work
    %%  with erpc (remote spawn) which is the default
    [lb, fail_capacity_wait, call_fail]. %% , packet_loss].

init_per_suite(Config) ->
    %% start an authority (which is also client node) running this test case code
    Boot = lambda_test:create_release(proplists:get_value(priv_dir, Config)),
    Auth = lambda_test:start_node(?MODULE, Boot, undefined,
        [],
        true),
    unlink(whereis(Auth)), %% otherwise Auth node exits immediately
    [{boot, Boot}, {auth, Auth} | Config].

end_per_suite(Config) ->
    peer:stop(proplists:get_value(auth, Config)),
    Config.

init_per_testcase(fail_capacity_wait, Config) ->
    %% not using extra client node, this test is local
    Config;
init_per_testcase(_TestCase, Config) ->
    Auth = proplists:get_value(auth, Config),
    {ok, Lb} = peer:call(Auth, lambda_plb, start_link, [lambda_broker, ?MODULE, #{capacity => 1000, compile => false}]),
    [{lb, Lb} | Config].

end_per_testcase(_TestCase, Config) ->
    %% stop the PLB started for this testcase
    proplists:get_value(lb, Config) =/= undefined andalso
        peer:call(proplists:get_value(auth, Config), gen_server, stop, [proplists:get_value(lb, Config)]),
    proplists:delete(lb, Config).

%%--------------------------------------------------------------------
%% actual functions to execute

echo(Term) ->
    Term.

exit(Reason) ->
    erlang:exit(self(), Reason).

pi(Precision) ->
    pi(4, -4, 3, Precision).

pi(LastResult, Numerator, Denominator, Precision) ->
    NextResult = LastResult + Numerator / Denominator,
    Pow = math:pow(10, Precision),
    case trunc(LastResult * Pow) =:= trunc(NextResult * Pow) of
        true ->
            trunc(NextResult * Pow) / Pow;
        false ->
            pi(NextResult, -1 * Numerator, Denominator+2, Precision)
    end.

sleep(Time) ->
    ct:sleep(Time).

%%--------------------------------------------------------------------
%% convenience primitives

multi_echo(0) ->
    ok;
multi_echo(Count) ->
    lambda_plb:call(?MODULE, echo, [[]], infinity),
    multi_echo(Count - 1).

make_node(Auth, Boot, Args, Capacity) ->
    Bootstrap = {static, #{{lambda_authority, Auth} => peer:call(Auth, lambda_discovery, get_node, [])}},
    Node = lambda_test:start_node(undefined, Boot, Bootstrap, Args, false),
    {ok, Worker} = peer:call(Node, lambda, publish, [?MODULE, #{capacity => Capacity}]),
    {Node, Worker}.

wait_complete(Procs) when is_list(Procs) ->
    wait_complete(maps:from_list(Procs));
wait_complete(Procs) when Procs =:= #{} ->
    ok;
wait_complete(Procs) ->
    receive
        {'DOWN', _MRef, process, Pid, _Reason} ->
            wait_complete(maps:remove(Pid, Procs))
    end.

%%--------------------------------------------------------------------
%% Load balancer correctness

lb() ->
    [{doc, "Tests weighted load balancer"}].

lb_test(Precalculated, Precision, SampleCount, ClientConcurrency) ->
    Spawned = [spawn_monitor(
        fun () -> [Precalculated = lambda_plb:call(?MODULE, pi, [Precision], infinity)
            || _ <- lists:seq(1, SampleCount div ClientConcurrency)]
        end)
        || _ <- lists:seq(1, ClientConcurrency)],
    ok = wait_complete(Spawned).

lb(Config) when is_list(Config) ->
    WorkerCount = 4,
    SampleCount = 1000,
    Precision = 3,
    ClientConcurrency = 100,
    ?assertEqual(0, SampleCount rem ClientConcurrency),
    Boot = proplists:get_value(boot, Config),
    Auth = proplists:get_value(auth, Config),
    %% start worker nodes, when every next node has +1 more capacity
    WorkIds = lists:seq(1, WorkerCount),
    Nodes = lambda_async:pmap([{fun make_node/4, [Auth, Boot, ["+S", integer_to_list(Seq)], Seq]}
        || Seq <- WorkIds]),
    %% check peers scheduler counts
    WorkIds = lambda_async:pmap([{peer, call, [N, erlang, system_info, [schedulers]]} || {N, _} <- Nodes]),
    %% expected result
    Precalculated = pi(Precision),
    %% don't care about capacity waiting! this it the WHOLE IDEA!
    %% fire samples: spawn a process per request
    ok = peer:call(Auth, ?MODULE, lb_test, [Precalculated, Precision, SampleCount, ClientConcurrency]),
    %% ensure weights and total counts expected
    TotalWeight = WorkerCount * (WorkerCount + 1) div 2,
    {WorkerCount, WeightedCounts} = lists:foldl(
        fun ({Node, Worker}, {Idx, WC}) ->
            {0, Count} = peer:call(Node, gen_server, call, [Worker, get_count]),
            ct:pal("Worker ~b/~b received ~b/~b~n",
                [Idx + 1, WorkerCount, Count, SampleCount * (Idx + 1) div TotalWeight]),
            {Idx + 1, [{Idx + 1, Count} | WC]}
        end, {0, []}, Nodes),
    %% stop all peers concurrently
    lambda_async:pmap([{peer, stop, [N]} || {N, _} <- Nodes]),
    {_, Counts} = lists:unzip(WeightedCounts),
    ?assertEqual(SampleCount, lists:sum(Counts)),
    [begin
        %% every server should have roughly it's share of samples
        Share = SampleCount * Seq div TotalWeight,
        %% ensure there is no 2x skew (isn't really expected from uniform PRNG)
        ?assert(Count div 2 < Share),
        ?assert(Count < Share * 2)
     end || {Seq, Count} <- WeightedCounts].

fail_capacity_wait() ->
    [{doc, "Exhaust capacity, then attempt to wait, do not wait long enough, and ensure tokens aren't wasted"}].

fail_capacity_wait(Config) when is_list(Config) ->
    Concurrency = 10, %% how many processes run in parallel
    Delay = 200,
    %% start both client & server locally (also verifies that it's possible - and there
    %%  are no registered names clashes)
    {ok, Disco} = lambda_discovery:start_link(),
    {ok, Auth} = lambda_authority:start_link(),
    {ok, Broker} = lambda_broker:start_link(),
    lambda_broker:authorities(Broker, #{Auth => undefined}),
    {ok, Worker} = lambda_listener:start_link(lambda_broker, ?MODULE, #{capacity => Concurrency}),
    %% client
    {ok, Plb} = lambda_plb:start_link(Broker, ?MODULE, #{capacity => 1000, compile => false}),
    %% spawn just enough requests to exhaust tokens
    Spawned = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    %% must wait until spawned processes at least requested once
    %% TODO: make it be reliable, not just a sleep
    ct:sleep(10),
    %% spawn and kill another batch - so tokens are sent to dead processes
    Dead = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [1], 1) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Dead),
    %% ^^^ processes spawned above will never be executed remotely
    %% now sleep (deliberately) to ensure demand is sent and wasted
    ct:sleep(Delay * 2),
    %% then, spawn more requests and ensure they pass
    Started = erlang:monotonic_time(),
    Spawned1 = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Spawned ++ Spawned1),
    Actual = erlang:convert_time_unit(erlang:monotonic_time() - Started, native, millisecond),
    %% ensure all requests were processed (and none of dead requests)
    ?assertEqual({0, Concurrency * 2}, gen_server:call(Worker, get_count)),
    %% stop servers
    [gen:stop(Srv) || Srv <- [Plb, Worker, Broker, Auth, Disco]],
    %% ensure Actual time is at least as Expected, but no more than double of it
    ?assert(Actual >= Delay, {actual, Actual, minimum, Delay}),
    ?assert(Actual =< 2 * Delay, {actual, Actual, maximum, Delay * 2}).

call_fail() ->
    [{doc, "Ensure failing calls do not exhaust pool"}].

call_fail_test(Concurrency) ->
    {ok, Worker} = lambda_listener:start_link(lambda_broker, ?MODULE, #{capacity => Concurrency}),
    Spawned = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, exit, [kill], infinity) end)
        || _ <- lists:seq(1, Concurrency * 5)],
    wait_complete(Spawned),
    %% ensure all requests were processed
    gen_server:call(Worker, get_count).

call_fail(Config) when is_list(Config) ->
    Concurrency = 5,
    Count = peer:call(proplists:get_value(auth, Config), ?MODULE, call_fail_test, [Concurrency]),
    ?assertEqual({0, Concurrency * 5}, Count).

packet_loss() ->
    [{doc, "Delibrately drops tokens, ensuring that processing does not stop"}].

packet_loss(Config) when is_list(Config) ->
    Concurrency = 5,
    {ok, Worker} = lambda_listener:start_link(lambda_broker, ?MODULE, #{capacity => Concurrency}),
    %% sync broker + plb to ensure it received demand
    Lb = proplists:get_value(lb, Config),
    lambda_test:sync_via(Worker, proplists:get_value(broker, Config)), %% this flushes broker (order sent to plb)
    lambda_test:sync_via(Lb, Worker), %% this flushes plb and worker via plb
    ?assertEqual(Concurrency, lambda_plb:capacity(Lb)),
    %% waste all tokens, using white-box testing
    [Lb ! {'$gen_call', {self(), erlang:make_ref()}, token} || _ <- lists:seq(1, Concurrency)],
    %% sync plb, ensuring tokens were wasted
    _ = sys:get_state(Lb),
    %% spawn requests and ensure they pass in a reasonable time
    Spawned = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, pi, [3], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Spawned),
    %% ensure all requests were processed
    ?assertEqual({0, Concurrency}, gen_server:call(Worker, get_count)).

%%--------------------------------------------------------------------
%% Performance & scalability

throughput() ->
    [{doc, "Tests client-side throughput of a single load balancer process"},
        {timetrap, {seconds, 360}}].

throughput(Config) when is_list(Config) ->
    %% change the lb process priority to 'high', so it wins over any other processes
    Lb = proplists:get_value(lb, Config),
    Broker = proplists:get_value(broker, Config),
    Boot = proplists:get_value(boot, Config),
    Auth = proplists:get_value(auth, Config),
    sys:replace_state(Lb, fun(S) -> erlang:process_flag(priority, high), S end),
    %% start servers
    [measure(Auth, Boot, Lb, Broker, CapPerNode, Nodes) || CapPerNode <- [100, 200, 500, 1000], Nodes <- [1, 2, 4, 8, 32, 64, 128]].

measure(Auth, Boot, Lb, Broker, CapPerNode, Nodes) ->
    %% remotely:
    {Node, Peer, Worker, CapPerNode} = make_node(Auth, Boot, [], CapPerNode),
    Workers = [begin {ok, W} = rpc:call(Node, lambda_listener, start, [?MODULE, CapPerNode]), W end
        || _ <- lists:seq(1, Nodes - 1)],
    %% ensure expected capacity achieved
    [lambda_test:sync_via(W, Broker) || W <- [Worker | Workers]],
    [lambda_test:sync_via(Lb, W) || W <- [Worker | Workers]],
    ?assertEqual(CapPerNode * Nodes, lambda_plb:capacity(Lb)),
    %% dispatch predefined amount of calls with some concurrency enabled
    [timed(Callers, CallsPerCaller, CapPerNode, Nodes)
        || Callers <- [1, 2, 4, 8, 16, 24, 32], CallsPerCaller <- [5000]],
    %% shut down
    net_kernel:disconnect(Node),
    peer:stop(Peer),
    %% flush broker
    lambda_test:sync_via(Broker, pg),
    lambda_test:sync_via(Lb, Broker),
    lambda_test:sync_via(Broker, Lb),
    %% ensure no capacity left in plb
    ?assertEqual(0, lambda_plb:capacity(Lb)),
    ok.

timed(Callers, CallsPerCaller, CapPerNode, Nodes) ->
    Started = erlang:monotonic_time(),
    Spawned = [spawn_monitor(fun () -> multi_echo(CallsPerCaller) end) || _ <- lists:seq(1, Callers)],
    wait_complete(Spawned),
    Finished = erlang:convert_time_unit(erlang:monotonic_time() - Started, native, microsecond),
    TotalCalls = Callers * CallsPerCaller,
    QPS = TotalCalls * 1000000 div Finished,
    io:format(user,
        "Echo: ~b us, ~b QPS (~b requests, ~b concurrent callers, with ~b nodes and ~b capacity per node)~n",
        [Finished div TotalCalls, QPS, TotalCalls, Callers, Nodes, CapPerNode]).

%%--------------------------------------------------------------------
%% event handler, for high/low watermark events
%%-behaviour(gen_event).
%%
%%-export([
%%    init/1,
%%    handle_event/2,
%%    handle_call/2
%%]).
%%
%%init({forward, Pid}) -> {ok, {forward, Pid}}.
%%
%%handle_event(Msg, {forward, Pid}) ->
%%    Pid ! Msg,
%%    {ok, {forward, Pid}}.
%%
%%handle_call(Req, {forward, Pid}) ->
%%    Pid ! Req,
%%    {ok, {forward, Pid}}.
%% wait for high watermark
%%    _ = gen_event:add_handler(plb:event_bus(Module), ?MODULE, {forward, self()}),
%%    Capacity = receive {high_watermark, Module} -> Actual end,
%%    gen_event:delete_handler(plb:event_bus(Module), ?MODULE, {forward, self()}),
%%    %%
%%    ct:pal("Ready to serve with ~b capacity", [Capacity]),
%%    %%
