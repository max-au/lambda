%% @doc
%%     Tests Lambda authority Gossip, mesh, and non-mesh brokers.
%% @end
-module(lambda_authority_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    local/0, local/1,
    remote/0, remote/1,
    basic/0, basic/1,
    proper/0, proper/1,
    reconnect/0, reconnect/1,
    stop_authority/0, stop_authority/1
]).

%% PropEr stateful testing exports
-export([
    initial_state/0,
    precondition/2,
    postcondition/3,
    command/1,
    next_state/3,
    %%
    start_authority/3,
    start_worker/3,
    connect/2,
    disconnect/2,
    check_state/0
]).

% -behaviour(proper_statem).

-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = lambda_test:create_release(Priv),
    [{boot, Boot} | Config].

end_per_suite(Config) ->
    Config.

groups() ->
    [{parallel, [parallel], [local, remote, reconnect, stop_authority]}].

all() ->
    %% [{group, parallel}, basic, proper].
    [{group, parallel}, basic].

%%--------------------------------------------------------------------
%% Convenience & data

start_tier(TestId, Boot, Count, AuthorityCount) when Count >= AuthorityCount ->
    %% start the nodes concurrently
    AuthNodes = [
            list_to_atom(lists:concat(["authority-", TestId, "-", integer_to_list(Seq)]))
        || Seq <- lists:seq(1, AuthorityCount)],
    Log = [], %% lambda_test:logger_config([lambda_discovery, lambda_bootstrap]),
    Tier = lambda_async:pmap([
        fun () ->
            Auth = Seq =< AuthorityCount,
            Node = lambda_test:start_node(TestId, Boot, {epmd, AuthNodes}, ["+S", "2:2"] ++ Log, Auth),
            {Node, Auth}
        end
        || Seq <- lists:seq(1, Count)], 5000 + Count * 100), %% add a second per every 10 nodes to start
    [?assertNotEqual(undefined, whereis(N), {failed_to_start, N, A}) || {N, A} <- Tier],
    Tier.

stop_tier(Peers) ->
    lambda_async:pmap([{peer, stop, [N]} || {N, _} <- Peers]).

multi_call(Peers, M, F, A) ->
    lambda_async:pmap([{peer, call, [P, M, F, A]} || P <- Peers]).

%% This function loops until there are no more
%%  changes in any authority state.
wait_stable(AuthPeers, Prev) ->
    Next = lists:sum([map_size(A) + map_size(B)
        || {lambda_authority_state, _, A, B} <- multi_call(AuthPeers, sys, get_state, [lambda_authority])]),
    case Next of
        Prev ->
            ct:pal("Finished wait at ~b", [Next]),
            ok;
        _ ->
            ct:sleep(100),
            wait_stable(AuthPeers, Next)
    end.

wait_brokers(BrokerPeers, AuthPeers, BrokerPids, AuthPids) ->
    %% queue flush: all nodes should complete:
    %%  * bootstrap
    Peers = BrokerPeers ++ AuthPeers,
    multi_call(Peers, lambda_bootstrap, discover, []),
    %%  * auth/broker bootstrap - calling to discovery, and
    %%    sending a request to all discovered authorities
    multi_call(BrokerPeers, sys, get_state, [lambda_broker]),
    multi_call(AuthPeers, sys, get_state, [lambda_authority]),
    %%  * remote authorities must drain the queue completely
    wait_stable(AuthPeers, -1),
    %%  * local auth/broker must complete processing
    multi_call(BrokerPeers, sys, get_state, [lambda_broker]),
    multi_call(AuthPeers, sys, get_state, [lambda_authority]),
    %%
    %% verification: all authorities know all nodes in the cluster
    AuthViews = multi_call(AuthPeers, lambda_authority, brokers, [lambda_authority]),
    [?assertEqual(BrokerPids, lists:sort(V)) || V <- AuthViews],
    %% verification: brokers know all authorities, but not any other brokers
    BrokerViews = multi_call(BrokerPeers, lambda_broker, authorities, [lambda_broker]),
    [?assertEqual(AuthPids, lists:sort(V)) || V <- BrokerViews].

verify_topo(Boot, TotalCount, AuthCount) ->
    ct:pal("Verifying ~b nodes with ~b authorities", [TotalCount, AuthCount]),
    %% start a tier, with several authorities
    AllPeers = start_tier(integer_to_list(AuthCount) ++ "-" ++ integer_to_list(TotalCount),
        Boot, TotalCount, AuthCount),
    %% Peers running Authority
    AuthPeers = [A || {A, true} <- AllPeers],
    {Nodes, _AuthBit} = lists:unzip(AllPeers),
    %% force bootstrap: bootstrap -> authority/broker
    multi_call(Nodes, lambda_bootstrap, discover, []),
    %% authorities must connect to all nodes - that's a mesh!
    Mesh = multi_call(AuthPeers, lambda_test, wait_connection, [Nodes]),
    [?assertEqual(ok, M, {M, N}) || {M, N} <- lists:zip(Mesh, AuthPeers)],
    %%
    ct:pal("Authorities connected (~b/~b)", [AuthCount, TotalCount]),
    %% Find all broker pids on all nodes
    BrokerPids = lists:sort(multi_call(Nodes, erlang, whereis, [lambda_broker])),
    %% get auth pids
    AuthPids = lists:sort(multi_call(AuthPeers, erlang, whereis, [lambda_authority])),
    %% verify pids are here, check that pids returned were pids.
    [?assert(is_pid(P), {pid, P}) || P <- BrokerPids ++ AuthPids],
    %% independently flush all queues of all brokers and authorities until expected
    %%  connections are made
    Result = wait_brokers(Nodes, AuthPeers, BrokerPids, AuthPids),
    %% stop everything
    stop_tier(AllPeers),
    Result.

%%--------------------------------------------------------------------
%% Test Cases

local() ->
    [{doc, "Tests authority discovery within a single Erlang node"}].

local(Config) when is_list(Config) ->
    %% start discovery
    {ok, Disco} = lambda_discovery:start_link(),
    LocalFakeAddress = #{addr => {127, 0, 0, 1}, port => 1},
    lambda_discovery:set_node(node(), LocalFakeAddress), %% not distributed
    %% start root (empty) authority
    {ok, AuthPid} = gen_server:start_link(lambda_authority, [], []),
    lambda_authority:peers(AuthPid, #{}),
    Bootstrap = #{AuthPid => lambda_discovery:get_node()},
    %% start a number of (unnamed) brokers pointing at authority
    Brokers = [
        begin
            {ok, BrokerPid} = gen_server:start_link(lambda_broker, [], []),
            lambda_broker:authorities(BrokerPid, #{AuthPid => LocalFakeAddress}),
            BrokerPid
        end || _ <- lists:seq(1, 8)],
    %% ensure authorities finished processing from broker point of view
    [lambda_test:sync_via(Broker, AuthPid) || Broker <- Brokers],
    %% ensure authority has discovered all of them
    ?assertEqual(lists:sort(Brokers), lists:sort(lambda_authority:brokers(AuthPid))),
    [?assertEqual([AuthPid], lambda_broker:authorities(R)) || R <- Brokers],
    %% start second authority
    {ok, Auth2} = gen_server:start_link(lambda_authority, [], []),
    lambda_authority:peers(Auth2, Bootstrap),
    %% let authorities sync. White-box testing here, knowing the protocol.
    lambda_test:sync_via(Auth2, AuthPid),
    lambda_test:sync_via(AuthPid, Auth2),
    lambda_test:sync(Brokers),
    %% ensure both authorities have the same list of connected processes
    ?assertEqual(lists:sort(lambda_authority:brokers(AuthPid)),
        lists:sort(lambda_authority:brokers(Auth2))),
    %% ensure authorities know each other
    Auths = lists:sort([AuthPid, Auth2]),
    [?assertEqual(Auths, lists:sort(lambda_authority:authorities(AP))) || AP <- Auths],
    %% ensure brokers know authorities too
    lambda_test:sync(Brokers),
    [?assertEqual(Auths, lists:sort(lambda_broker:authorities(R))) || R <- Brokers],
    %% all done, stop now
    [gen:stop(Pid) || Pid <- Brokers ++ [AuthPid, Auth2, Disco]].

remote() ->
    [{doc, "Tests two authority nodes on two localhost addresses"}].

remote(Config) when is_list(Config) ->
    %% start 2 authorities, making dist to listen on two localhost IP
    %%  addresses
    Boot = proplists:get_value(boot, Config),
    Common = ["+S", "2:2"],
    Auth1 = lambda_test:start_node(?FUNCTION_NAME, Boot, undefined,
        Common ++ ["-kernel", "inet_dist_use_interface", "{127,0,0,1}"], true),
    Addr1 = peer:call(Auth1, lambda_discovery, get_node, []),
    %% check that IP actually took effect
    ?assertEqual({127, 0, 0, 1}, maps:get(addr, Addr1), Addr1),
    %% boot second authority
    BootSpec = {static, #{{lambda_authority, Auth1} => Addr1}},
    Auth2 = lambda_test:start_node(?FUNCTION_NAME, Boot, BootSpec,
        Common, true),
    %% start a worker node that only knows about first authority
    Worker = lambda_test:start_node(?FUNCTION_NAME, Boot, BootSpec,
        Common, false),
    %% ensure exchanges happened
    lambda_test:sync([{Worker, lambda_bootstrap}, {Worker, lambda_broker},
        {Auth1, lambda_authority}, {Auth2, lambda_authority}, {Worker, lambda_broker}]),
    %% ensure it also knows the second
    WorkerAuths = peer:call(Worker, lambda_broker, authorities, [lambda_broker]),
    ?assertEqual(2, length(WorkerAuths), WorkerAuths),
    ?assertEqual(lists:sort([Auth1, Auth2]), lists:sort([node(A) || A <- WorkerAuths])),
    %% cleanup
    lambda_async:pmap([{peer, stop, [N]} || N <- [Worker, Auth1, Auth2]]),
    ok.

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"},
        {timetrap, {seconds, 180}}].

basic(Config) when is_list(Config) ->
    Boot = proplists:get_value(boot, Config),
    %% detect low-power VM and don't run large clusters
    {Totals, Auths} =
        case erlang:system_info(schedulers) of
            Large when Large > 11 -> {[2, 4, 10, 32, 48], [1, 2, 3, 4]};
            Normal when Normal > 5 -> {[2, 4, 10], [1, 2, 3]};
            _Small -> {[2, 4], [1, 2]}
        end,
    %% run many combinations
    [
        begin
            verify_topo(Boot, Total, Auth)
        end || Total <- Totals, Auth <- Auths, Total >= Auth].

reconnect() ->
    [{doc, "Ensure that broker reconnects to authorities"}].

reconnect(Config) when is_list(Config) ->
    Boot = proplists:get_value(boot, Config),
    %% start a tier, with several authorities
    AllPeers = start_tier(reconnect, Boot, 4, 2),
    {[A1, A2 | _] = Nodes, _} = lists:unzip(AllPeers),
    Victim = lists:nth(4, Nodes),
    Auths = lists:sort([A1, A2]),
    %% wait for expected connections
    ct:pal("Attempting first connection"),
    ?assertEqual(ok, peer:call(Victim, lambda_bootstrap, discover, [])),
    ?assertEqual(ok, peer:call(Victim, lambda_test, wait_connection, [Auths])),
    ct:pal("First connection successful~n"),
    %% force disconnect last node from first 2
    ?assertEqual(Auths, lists:sort(peer:call(Victim, erlang, nodes, []))),
    true = peer:call(Victim, net_kernel, disconnect, [A1]),
    true = peer:call(Victim, net_kernel, disconnect, [A2]),
    %% verify it has been disconnected
    [] = peer:call(Victim, erlang, nodes, []),
    %% force bootstrap
    ?assertEqual(ok, peer:call(Victim, lambda_bootstrap, discover, [])),
    %% must be connected again
    ?assertEqual(ok, peer:call(Victim, lambda_test, wait_connection, [Auths])),
    %% just in case: verify CLI returns the same information
    stop_tier(AllPeers).

stop_authority() ->
    [{doc, "Tests stopping authority"}].

stop_authority(Config) when is_list(Config) ->
    Boot = proplists:get_value(boot, Config),
    W3 = start_workers(?FUNCTION_NAME, Boot, [], 3),
    A1 = start_authority(?FUNCTION_NAME, Boot, []),
    W2 = start_workers(?FUNCTION_NAME, Boot, [A1], 2),
    peer:stop(A1),
    %% expect system to come to a new state
    ?assertEqual([ok || _ <- (W3 ++ W2)],
        multi_call(W3 ++ W2, lambda_test, wait_connection, [[]])),
    %% verity all workers lost authority
    ActualAuths = multi_call(W3 ++ W2, erlang, nodes, []),
    lambda_async:pmap([{peer, stop, [W]} || W <- (W3 ++ W2)]),
    ?assertEqual([[]], lists:usort(ActualAuths)),
    ok.

proper() ->
    [{doc, "Property-based test verifying topology, mesh-connected for authorities, and none for lambdas"}].

proper(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = lambda_test:create_release(Priv),
    try proper:quickcheck(prop_self_healing(Boot),
        [{numtests, 10}, {max_size, 100}, {start_size, 10}, long_result]) of
        true ->
            ok;
        {error, Err} ->
            {fail, Err};
        CounterExample ->
            {fail, {counterexample, CounterExample}}
    catch
        error:undef ->
            {skip, "PropEr not installed"}
    end.

%%--------------------------------------------------------------------
%% Properties

prop_self_healing(Boot) ->
    proper:forall(proper_statem:commands(?MODULE, initial_state(Boot)),
        fun (Cmds) ->
            {_History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
            %%%% cleanup
            cleanup(State),
            Result =/= ok andalso io:format(standard_error, "FAIL: ~200p~n", [lists:zip(Cmds, _History)]),
            Result =:= ok
        end
    ).

start_authority(TestId, Boot, AuthNodes) ->
    %% use lambda_test:logger_config for reading extra nodes logger output
    Logs = [], %% lambda_test:logger_config([lambda_bootstrap]),
    lambda_test:start_node_link(TestId, Boot, {epmd, AuthNodes}, ["+S", "2:2"] ++ Logs, true).


start_worker(TestId, Boot, AuthNodes) ->
    Logs = [], %% lambda_test:logger_config([lambda_bootstrap]),
    lambda_test:start_node_link(TestId, Boot, {epmd, AuthNodes}, ["+S", "2:2"] ++ Logs, false).

start_workers(TestId, Boot, AuthNodes, HowMany) ->
    Workers = lambda_async:pmap(
        [fun () ->
            Worker = start_worker(TestId, Boot, AuthNodes),
            unlink(whereis(Worker)),
            Worker
         end || _ <- lists:seq(1, HowMany)]),
    [link(whereis(W)) || W <- Workers],
    Workers.

connect(Peer1, Node2) ->
    peer:call(Peer1, net_kernel, connect_node, [Node2]).

disconnect(Peer1, Node2) ->
    peer:call(Peer1, net_kernel, disconnect, [Node2]).

check_state() ->
    ok.

%%--------------------------------------------------------------------
%% PropEr state machine implementation

%% Limits
-define (MAX_AUTHORITY, 8).
-define (MAX_BROKERS, 16).

-record(cluster_state, {
    test_id :: integer(),
    boot :: file:filename_all(),
    auth = #{} :: #{node() => node()},
    brokers = #{} :: #{node() => node()}
}).

cleanup(#cluster_state{auth = Auth, brokers = Brokers}) ->
    lambda_async:pmap([{peer, stop, [Node]} || Node <- maps:keys(Brokers) ++ maps:keys(Auth)]).

initial_state(Boot) ->
    TestId = case erlang:get(test_id) of undefined -> 0; Id -> Id + 1 end,
    erlang:put(test_id, TestId),
    #cluster_state{test_id = TestId, boot = Boot}.

initial_state() ->
    %% not used, initial state is supplied externally
    #cluster_state{}.

precondition(#cluster_state{auth = Auth}, {call, ?MODULE, start_worker, [_, _, _]}) ->
    map_size(Auth) < ?MAX_AUTHORITY;

precondition(#cluster_state{brokers = Brokers}, {call, ?MODULE, start_authority, [_, _, _]}) ->
    map_size(Brokers) < ?MAX_BROKERS;

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, peer, stop, [Node]}) ->
    is_map_key(Node, Auth) orelse is_map_key(Node, Brokers);

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, connect, [Peer, Node]}) ->
    is_map_key(Peer, Auth) orelse is_map_key(Peer, Brokers) orelse
        lists:member(Node, maps:values(Auth)) orelse lists:member(Node, maps:values(Brokers));

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, disconnect, [Peer, Node]}) ->
    is_map_key(Peer, Auth) orelse is_map_key(Peer, Brokers) orelse
        lists:member(Node, maps:values(Auth)) orelse lists:member(Node, maps:values(Brokers));

precondition(_State, {call, ?MODULE, check_state, []}) ->
    true.

postcondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, check_state, []}, _Res) ->
    AuthNodes = maps:keys(Auth),
    Peers = AuthNodes ++ maps:keys(Brokers),
    %% synchronisation:
    %%  (a) bootspec can change (so does list of authorities)
    %%  (b) workers list can change
    %%  (c) connections may be started or dropped
    %% if nothing changed, no need to sync, but otherwise, all nodes need to finish
    %%  processing in the allotted amount of time
    %% update bootspec for all nodes
    multi_call(Peers, lambda_bootstrap, discover, [{epmd, AuthNodes}]),
    %% wait up to 4 sec for nodes to connect to authorities, fancy multi-call
    Connected = multi_call(AuthNodes, lambda_test, wait_connection, [Peers]),
    case lists:usort(Connected) of
        [] ->
            %% no authorities, but we can check that no brokers are connected
            Empty = multi_call(maps:keys(Brokers), erlang, nodes, []),
            lists:all(fun (Null) -> Null =:= [] end, Empty);
        [ok] ->
            %% verify brokers are connected _only_ to authorities, but never between themselves
            Auths = lists:sort(maps:values(Auth)),
            MustBeAuths = multi_call(maps:keys(Brokers), erlang, nodes, []),
            Res = lists:all(fun (MaybeAuths) -> lists:sort(MaybeAuths) =:= Auths end, MustBeAuths),
            Res orelse
                begin
                    io:format(standard_error, "Brokers ~200p~nAuthorities: ~200p~n", [maps:keys(Brokers), maps:keys(Auth)]),
                    [case lists:sort(MBA) =:= Auths of
                         false ->
                             io:format(standard_error, "Broker ~p has authorities ~200p while expecting ~200p~n", [B, MBA, Auths]);
                         true ->
                             ok
                     end || {B, MBA} <- lists:zip(maps:keys(Brokers), MustBeAuths)]
                end,
            Res;
        _NotOk ->
            [
                io:format(standard_error, "Authority ~p failed connections to brokers ~200p~nReason: ~200p~n", [A, Peers, Error])
                || {A, Error} <- lists:zip(maps:keys(Auth), Connected), Error =/= ok],
            false
    end;

postcondition(_State, _Cmd, _Res) ->
    %% io:format(user, "~200p => ~200p~n~200p~n", [_Cmd, _Res, _State]),
    true.

%% Events possible:
%%  * start/stop an authority/broker
%%  * net splits/recovers
%% * topology verification
command(#cluster_state{test_id = TestId, auth = Auth, brokers = Brokers, boot = Boot}) ->
    %% nodes starting (authority/broker)
    AuthNodes = maps:keys(Auth),
    AuthorityStart = {1, {call, ?MODULE, start_authority, [TestId, Boot, AuthNodes]}},
    BrokerStart = {5, {call, ?MODULE, start_worker, [TestId, Boot, AuthNodes]}},
    CheckState = {3, {call, ?MODULE, check_state, []}},
    %% stop: anything that is running can be stopped at will
    Stop =
        case maps:merge(Auth, Brokers) of
            Empty when Empty =:= #{} ->
                [];
            Nodes ->
                Peers = maps:keys(Nodes),
                NodeNames = maps:values(Nodes),
                [
                    {5, {call, peer, stop, [proper_types:oneof(Peers)]}},
                    {5, {call, ?MODULE, connect, [proper_types:oneof(Peers), proper_types:oneof(NodeNames)]}},
                    {5, {call, ?MODULE, disconnect, [proper_types:oneof(Peers), proper_types:oneof(NodeNames)]}}
                ]
        end,
    %% make your choice, Mr PropEr
    Choices = [AuthorityStart, BrokerStart, CheckState | Stop],
    proper_types:frequency(Choices).

next_state(_State, _Res, {init, InitialState}) ->
    InitialState;

next_state(#cluster_state{auth = Auth} = State, Res, {call, ?MODULE, start_authority, [_, _, _]}) ->
    State#cluster_state{auth = Auth#{Res => Res}};

next_state(#cluster_state{brokers = Brokers} = State, Res, {call, ?MODULE, start_worker, [_, _, _]}) ->
    State#cluster_state{brokers = Brokers#{Res => Res}};

next_state(State, _Res, {call, ?MODULE, connect, [_Peer1, _Node2]}) ->
    State;

next_state(State, _Res, {call, ?MODULE, disconnect, [_Peer1, _Node2]}) ->
    State;

next_state(State, _Res, {call, ?MODULE, check_state, []}) ->
    State;

next_state(#cluster_state{auth = Auth, brokers = Brokers} = State, _Res, {call, peer, stop, [Node]}) ->
    %% just remove Pid from all processes known
    State#cluster_state{auth = maps:remove(Node, Auth), brokers = maps:remove(Node, Brokers)}.
