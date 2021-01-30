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

%% Should not be exported in the future
-export([
    pi/1,
    sleep/1,
    echo/1,
    exit/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    %% packet_loss is currently unresolved,
    %%  it appears to be impossible to make it work
    %%  with erpc (remote spawn) which is the default
    [lb, fail_capacity_wait, call_fail]. %% , packet_loss].

init_per_suite(Config) ->
    ok = lambda_test:start_local(),
    Config.

end_per_suite(Config) ->
    lambda_test:end_local(),
    Config.

init_per_testcase(_TestCase, Config) ->
    {ok, Lb} = lambda_plb:start_link(lambda_broker, ?MODULE, #{capacity => 1000, compile => false}),
    [{lb, Lb} | Config].

end_per_testcase(_TestCase, Config) ->
    proplists:get_value(lb, Config) =/= undefined andalso
        gen_server:stop(proplists:get_value(lb, Config)),
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
    timer:sleep(Time).

%%--------------------------------------------------------------------
%% convenience primitives

multi_echo(0) ->
    ok;
multi_echo(Count) ->
    lambda_plb:call(?MODULE, echo, [[]], infinity),
    multi_echo(Count - 1).

make_node(Args, Capacity) ->
    Bootstrap = #{{lambda_authority, node()} => lambda_discovery:get_node()},
    {Peer, _Node} = lambda_test:start_node_link(Bootstrap, Args, false),
    {ok, Worker} = peer:apply(Peer, lambda, publish, [?MODULE, #{capacity => Capacity}]),
    unlink(Peer), %% need to unlink - otherwise pmap will terminate peer controller
    {Peer, Worker}.

wait_complete(Procs) when is_list(Procs) ->
    wait_complete(maps:from_list(Procs));
wait_complete(Procs) when Procs =:= #{} ->
    ok;
wait_complete(Procs) ->
    receive
        {'DOWN', _MRef, process, Pid, Reason} ->
            Reason =/= normal andalso
                ct:pal("~p: wait error: ~p", [Pid, Reason]),
            wait_complete(maps:remove(Pid, Procs))
    end.

%%--------------------------------------------------------------------
%% Load balancer correctness

lb() ->
    [{doc, "Tests weighted load balancer"}].

lb(Config) when is_list(Config) ->
    WorkerCount = 4,
    SampleCount = 1000,
    Precision = 3,
    ClientConcurrency = 100,
    ?assertEqual(0, SampleCount rem ClientConcurrency),
    %% start worker nodes, when every next node has +1 more capacity
    WorkIds = lists:seq(1, WorkerCount),
    Peers = lambda_async:pmap([{fun make_node/2, [["+S", integer_to_list(Seq)], Seq]}
        || Seq <- WorkIds]),
    %% check peers scheduler counts
    WorkIds = lambda_async:pmap([{peer, apply, [P, erlang, system_info, [schedulers]]} || {P, _} <- Peers]),
    %% expected result
    Precalculated = pi(Precision),
    %% don't care about capacity waiting! this it the WHOLE IDEA!
    %% fire samples: spawn a process per request
    Spawned = [spawn_monitor(
        fun () -> [Precalculated = lambda_plb:call(?MODULE, pi, [Precision], infinity)
            || _ <- lists:seq(1, SampleCount div ClientConcurrency)]
        end)
        || _ <- lists:seq(1, ClientConcurrency)],
    ok = wait_complete(Spawned),
    %% ensure weights and total counts expected
    TotalWeight = WorkerCount * (WorkerCount + 1) div 2,
    {WorkerCount, WeightedCounts} = lists:foldl(
        fun ({Peer, Worker}, {Idx, WC}) ->
            {0, Count} = peer:apply(Peer, gen_server, call, [Worker, get_count]),
            ct:pal("Worker ~b/~b received ~b/~b~n",
                [Idx + 1, WorkerCount, Count, SampleCount * (Idx + 1) div TotalWeight]),
            {Idx + 1, [{Idx + 1, Count} | WC]}
        end, {0, []}, Peers),
    %% stop all peers concurrently
    lambda_async:pmap([{peer, stop, [P]} || {P, _} <- Peers]),
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
    {ok, Worker} = lambda_listener:start_link(lambda_broker, ?MODULE, #{capacity => Concurrency}),
    %% spawn just enough requests to exhaust tokens
    Spawned = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    %% mut wait until spawned processes at least requested once
    %% TODO: make it bre reliable, not just a sleep
    timer:sleep(10),
    %% spawn and kill another batch - so tokens are sent to dead processes
    Dead = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [1], 1) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Dead),
    %% ^^^ processes spawned above will never be executed remotely
    %% now sleep to ensure demand is sent and wasted
    timer:sleep(Delay * 2),
    %% then, spawn more requests and ensure they pass
    Started = erlang:monotonic_time(),
    Spawned1 = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Spawned ++ Spawned1),
    Actual = erlang:convert_time_unit(erlang:monotonic_time() - Started, native, millisecond),
    %% ensure all requests were processed (and none of dead requests)
    ?assertEqual({0, Concurrency * 2}, gen_server:call(Worker, get_count)),
    %% ensure Actual time is at least as Expected, but no more than double of it
    ?assert(Actual >= Delay, {actual, Actual, minimum, Delay}),
    ?assert(Actual =< 2 * Delay, {actual, Actual, maximum, Delay * 2}).

call_fail() ->
    [{doc, "Ensure failing calls do not exhaust pool"}].

call_fail(Config) when is_list(Config) ->
    Concurrency = 5,
    {ok, Worker} = lambda_listener:start_link(lambda_broker, ?MODULE, #{capacity => Concurrency}),
    Spawned = [spawn_monitor(fun () -> lambda_plb:call(?MODULE, exit, [kill], infinity) end)
        || _ <- lists:seq(1, Concurrency * 5)],
    wait_complete(Spawned),
    %% ensure all requests were processed
    ?assertEqual({0, Concurrency * 5}, gen_server:call(Worker, get_count)).

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
    sys:replace_state(Lb, fun(S) -> erlang:process_flag(priority, high), S end),
    %% start servers
    [measure(Lb, Broker, CapPerNode, Nodes) || CapPerNode <- [100, 200, 500, 1000], Nodes <- [1, 2, 4, 8, 32, 64, 128]].

measure(Lb, Broker, CapPerNode, Nodes) ->
    %% remotely:
    {Node, Peer, Worker, CapPerNode} = make_node([], CapPerNode),
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
