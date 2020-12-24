%% @doc
%%     Smoke test for probabilistic load balancer.
%% @end
-module(lambda_SUITE).
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

%% Internal exports
-export([
    saviour/0
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    [lb, fail_capacity_wait, call_fail, packet_loss].

init_per_suite(Config) ->
    Saviour = proc_lib:start_link(?MODULE, saviour, []),
    Config1 = [{level, maps:get(level, logger:get_primary_config())}, {saviour, Saviour}  | Config],
    % logger:set_primary_config(level, all),
    case erlang:is_alive() of
        true ->
            Config1;
        false ->
            LongShort = case inet_db:res_option(domain) of
                            [] ->
                                shortnames;
                            Domain when is_list(Domain) ->
                                longnames
                        end,
            case net_kernel:start([master, LongShort]) of
                {ok, Pid} ->
                    [{net_kernel, Pid} | Config1];
                {error, {{shutdown, {failed_to_start_child, net_kernel, {'EXIT', nodistribution}}}, _}} ->
                    os:cmd("epmd -daemon"),
                    {ok, Pid1} = net_kernel:start([master, LongShort]),
                    [{net_kernel, Pid1} | Config1]
            end
    end.

end_per_suite(Config) ->
    Saviour = proplists:get_value(saviour, Config),
    MRef = erlang:monitor(process, Saviour),
    Saviour ! stop,
    receive
        {'DOWN', MRef, process, Saviour, _} ->
            ok
    end,
    Config1 = proplists:delete(saviour, Config),
    Config2 =
        case proplists:get_value(net_kernel, Config) of
            undefined ->
                Config1;
            Pid when is_pid(Pid) ->
                net_kernel:stop(),
                proplists:delete(net_kernel, Config1)
        end,
    logger:set_primary_config(level, ?config(level, Config2)),
    proplists:delete(level, Config2).

init_per_testcase(throughput, Config) ->
    {ok, Lb} = lambda:start_link(?MODULE, #{low => 0, high => 10000000}),
    [{lb, Lb} | Config];
init_per_testcase(_TestCase, Config) ->
    {ok, Lb} = lambda:start_link(?MODULE),
    [{lb, Lb} | Config].

end_per_testcase(_TestCase, Config) ->
    proplists:get_value(lb, Config) =/= undefined andalso
        gen_server:stop(proplists:get_value(lb, Config)),
    %% stop any servers left
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
    NextResult = LastResult + Numerator/Denominator,
    Pow = math:pow(10, Precision),
    case trunc(LastResult * Pow) =:= trunc(NextResult * Pow) of
        true ->
            trunc(NextResult*Pow)/Pow;
        false ->
            pi(NextResult, -1*Numerator, Denominator+2, Precision)
    end.

sleep(Time) ->
    timer:sleep(Time).

%%--------------------------------------------------------------------
%% convenience primitives

saviour() ->
    {ok, Physical} = lambda_epmd:start_link(),
    {ok, Registry} = lambda_registry:start_link(#{}),
    {ok, Broker} = lambda_broker:start_link(?MODULE),
    proc_lib:init_ack(self()),
    receive
        stop ->
            gen:stop(Broker),
            gen:stop(Registry),
            gen:stop(Physical)
    end.

flush(Pid) ->
    sys:get_state(Pid).

flush_via(Via, Pid) ->
    sys:replace_state(Via, fun(State) -> flush(Pid), State end).

multi_echo(0) ->
    ok;
multi_echo(Count) ->
    lambda:call(?MODULE, echo, [[]], infinity),
    multi_echo(Count - 1).

make_node(Args, Capacity) ->
    Node = peer:random_name(),
    {ok, Peer} = peer:start_link(#{node => Node,
        args => Args, connection => standard_io, wait_boot => 5000}),
    true = rpc:call(Node, code, add_path, [filename:dirname(code:which(?MODULE))]),
    {ok, _Apps} = rpc:call(Node, application, ensure_all_started, [lambda]),
    {ok, Worker} = rpc:call(Node, lambda_server, start, [?MODULE, Capacity]),
    {Node, Peer, Worker, Capacity}.

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
    Peers = [make_node(["+S", integer_to_list(Seq)], Seq) || Seq <- lists:seq(1, WorkerCount)],
    %% here we expect at least some capacity to pile up
    Lb = ?config(lb, Config),
    ?assert(lambda:capacity(Lb) > 0),
    %% fire samples: spawn a process per request
    Spawned = [spawn_monitor(
        fun () -> [lambda:call(?MODULE, pi, [Precision], infinity) || _ <- lists:seq(1, SampleCount div ClientConcurrency)] end)
        || _ <- lists:seq(1, ClientConcurrency)],
    wait_complete(Spawned),
    %% ensure weights and total counts expected
    TotalWeight = WorkerCount * (WorkerCount + 1) div 2,
    WeightedCounts = [
        begin
            {0, Count} = gen_server:call(Worker, get_count, infinity),
            ct:pal("Worker ~b/~b received ~b/~b~n",
                [Seq, WorkerCount, Count, SampleCount * Seq div TotalWeight]),
            peer:stop(Peer),
            {Seq, Count}
        end  || {_, Peer, Worker, Seq} <- Peers],
    {_, Counts} = lists:unzip(WeightedCounts),
    ?assertEqual(SampleCount, lists:sum(Counts)),
    %%
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
    {ok, Worker} = lambda_server:start_link(?MODULE, Concurrency),
    %% spawn just enough requests to exhaust tokens
    Spawned = [spawn_monitor(fun () -> lambda:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    %% spawn and kill another batch - so tokens are sent to dead processes
    Dead = [spawn_monitor(fun () -> lambda:call(?MODULE, sleep, [1], 1) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Dead),
    %% ^^^ processes spawned above will never be executed remotely
    %% now sleep to ensure demand is sent and wasted
    timer:sleep(Delay * 2),
    %% then, spawn more requests and ensure they pass
    Started = erlang:monotonic_time(),
    Spawned1 = [spawn_monitor(fun () -> lambda:call(?MODULE, sleep, [Delay], infinity) end)
        || _ <- lists:seq(1, Concurrency)],
    wait_complete(Spawned ++ Spawned1),
    Actual = erlang:convert_time_unit(erlang:monotonic_time() - Started, native, millisecond),
    %% ensure all requests were processed (and none of dead requests)
    ?assertEqual({0, Concurrency * 2}, gen_server:call(Worker, get_count)),
    %% ensure Actual time is at least as Expected, but no more than double of it
    ?assert(Actual > Delay, {actual, Actual, minimum, Delay}),
    ?assert(Actual < 2 * Delay, {actual, Actual, maximum, Delay * 2}).

call_fail() ->
    [{doc, "Ensure failing calls do not exhaust pool"}].

call_fail(Config) when is_list(Config) ->
    Concurrency = 5,
    {ok, Worker} = lambda_server:start_link(?MODULE, Concurrency),
    Spawned = [spawn_monitor(fun () -> lambda:call(?MODULE, exit, [kill], infinity) end)
        || _ <- lists:seq(1, Concurrency * 5)],
    wait_complete(Spawned),
    %% ensure all requests were processed
    ?assertEqual({0, Concurrency * 5}, gen_server:call(Worker, get_count)).

packet_loss() ->
    [{doc, "Delibrately drops tokens, ensuring that processing does not stop"}].

packet_loss(Config) when is_list(Config) ->
    Concurrency = 5,
    {ok, Worker} = lambda_server:start_link(?MODULE, Concurrency),
    %% sync broker + plb to ensure it received demand
    Lb = ?config(lb, Config),
    flush_via(Worker, ?config(broker, Config)), %% this flushes broker (order sent to plb)
    flush_via(Lb, Worker), %% this flushes plb and worker via plb
    ?assertEqual(Concurrency, lambda:capacity(Lb)),
    %% waste all tokens, using white-box testing
    [Lb ! {'$gen_call', {self(), erlang:make_ref()}, token} || _ <- lists:seq(1, Concurrency)],
    %% sync plb, ensuring tokens were wasted
    _ = sys:get_state(Lb),
    %% spawn requests and ensure they pass in a reasonable time
    Spawned = [spawn_monitor(fun () -> lambda:call(?MODULE, pi, [3], infinity) end)
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
    Lb = ?config(lb, Config),
    Broker = ?config(broker, Config),
    sys:replace_state(Lb, fun(S) -> erlang:process_flag(priority, high), S end),
    %% start servers
    [measure(Lb, Broker, CapPerNode, Nodes) || CapPerNode <- [100, 200, 500, 1000], Nodes <- [1, 2, 4, 8, 32, 64, 128]].

measure(Lb, Broker, CapPerNode, Nodes) ->
    %% remotely:
    {Node, Peer, Worker, CapPerNode} = make_node([], CapPerNode),
    Workers = [begin {ok, W} = rpc:call(Node, lambda_server, start, [?MODULE, CapPerNode]), W end
        || _ <- lists:seq(1, Nodes - 1)],
    %% ensure expected capacity achieved
    [flush_via(W, Broker) || W <- [Worker | Workers]],
    [flush_via(Lb, W) || W <- [Worker | Workers]],
    ?assertEqual(CapPerNode * Nodes, lambda:capacity(Lb)),
    %% dispatch predefined amount of calls with some concurrency enabled
    [timed(Callers, CallsPerCaller, CapPerNode, Nodes)
        || Callers <- [1, 2, 4, 8, 16, 24, 32], CallsPerCaller <- [5000]],
    %% shut down
    net_kernel:disconnect(Node),
    peer:stop(Peer),
    %% flush broker
    flush_via(Broker, pg),
    flush_via(Lb, Broker),
    flush_via(Broker, Lb),
    %% ensure no capacity left in plb
    ?assertEqual(0, lambda:capacity(Lb)),
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
%%    _ = gen_event:add_handler(plb:event_bus(Scope), ?MODULE, {forward, self()}),
%%    Capacity = receive {high_watermark, Scope, Actual} -> Actual end,
%%    gen_event:delete_handler(plb:event_bus(Scope), ?MODULE, {forward, self()}),
%%    %%
%%    ct:pal("Ready to serve with ~b capacity", [Capacity]),
%%    %%