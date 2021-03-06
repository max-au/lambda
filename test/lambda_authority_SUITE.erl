%% @doc
%%     Tests Lambda authority Gossip, mesh, and non-mesh brokers.
%% @end
-module(lambda_authority_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    proper/0, proper/1,
    reconnect/0, reconnect/1
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
    start_broker/3,
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

all() ->
    [basic, reconnect, proper].

%%--------------------------------------------------------------------
%% Convenience & data

start_tier(TestId, Boot, Count, AuthorityCount, ServiceLocator) when Count >= AuthorityCount ->
    %% start the nodes concurrently
    lambda_async:pmap([
        fun () ->
            Auth = Seq =< AuthorityCount,
            {Peer, Node} = lambda_test:start_node_link(TestId, Boot, [{file, ServiceLocator}], ["+S", "2:2"], Auth),
            unlink(Peer),
            {Peer, Node, Auth}
        end
        || Seq <- lists:seq(1, Count)]).

stop_tier(Peers) ->
    lambda_async:pmap([{peer, stop, [P]} || {P, _, _} <- Peers]).

wait_brokers(BrokerPeers, AuthPeers, BrokerPids, AuthPids) ->
    AuthViews = lambda_async:pmap([
        {peer, apply, [P, lambda_authority, brokers, [lambda_authority]]} || P <- AuthPeers]),
    BrokerViews = lambda_async:pmap([
        {peer, apply, [P, lambda_broker, authorities, [lambda_broker]]} || P <- BrokerPeers]),
    case lists:all(fun (V) -> lists:sort(V) =:= BrokerPids end, AuthViews) andalso
         lists:all(fun (V) -> lists:sort(V) =:= AuthPids end, BrokerViews) of
        true ->
            ok;
        false ->
            wait_brokers(BrokerPeers, AuthPeers, BrokerPids, AuthPids)
    end.

verify_topo(Boot, ServiceLocator, TotalCount, AuthCount) ->
    %% start a tier, with several authorities
    AllPeers = start_tier(integer_to_list(AuthCount) ++ "-" ++ integer_to_list(TotalCount),
        Boot, TotalCount, AuthCount, ServiceLocator),
    %% Peers running Authority
    AuthPeers = [A || {A, _, true} <- AllPeers],
    {Peers, Nodes, _AuthBit} = lists:unzip3(AllPeers),
    %% force bootstrap: bootstrap -> authority/broker
    lambda_async:pmap([{peer, apply, [P, lambda_bootstrap, discover, []]} || P <- Peers]),
    %% authorities must connect to all nodes - that's a mesh!
    lambda_async:pmap([{peer, apply, [A, lambda_test, wait_connection, [Nodes]]} || A <- AuthPeers]),
    %%
    ct:pal("Authorities connected (~b/~b)", [AuthCount, TotalCount]),
    %% Find all broker pids on all nodes
    Brokers = lists:sort(lambda_async:pmap(
        [{peer, apply, [P, erlang, whereis, [lambda_broker]]} || P <- Peers])),
    %% get auth pids
    AuthPids = lists:sort(lambda_async:pmap([{peer, apply,
        [P, erlang, whereis, [lambda_authority]]} || P <- AuthPeers])),
    %% need to flush broker -authority -broker queues reliably, but without triggering any erlang message sends
    %% anti-pattern: yes, use wait_until, as otherwise need a synchronous way to start the app/release
    wait_brokers(Peers, AuthPeers, Brokers, AuthPids),
    %% stop everything
    stop_tier(AllPeers).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"},
        {timetrap, {seconds, 180}}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = proplists:get_value(boot, Config),
    %% run many combinations
    [
        begin
            ServiceLocator = filename:join(Priv,
                lists:flatten(io_lib:format("service-~s~b_~b.loc", [?FUNCTION_NAME, Total, Auth]))),
            verify_topo(Boot, ServiceLocator, Total, Auth)
        end || Total <- [2, 4, 10, 32, 100], Auth <- [1, 2, 3, 6], Total >= Auth].

reconnect() ->
    [{doc, "Ensure that broker reconnects to authorities"}].

reconnect(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = proplists:get_value(boot, Config),
    ServiceLocator = filename:join(Priv, "reconnect-service.loc"),
    %% start a tier, with several authorities
    AllPeers = start_tier(reconnect, Boot, 4, 2, ServiceLocator),
    {Peers, [A1, A2 | _], _} = lists:unzip3(AllPeers),
    Victim = lists:nth(4, Peers),
    Auths = lists:sort([A1, A2]),
    %% wait for expected connections
    ct:pal("Attempting first connection"),
    ?assertEqual(ok, peer:apply(Victim, lambda_bootstrap, discover, [])),
    ?assertEqual(ok, peer:apply(Victim, lambda_test, wait_connection, [Auths])),
    ct:pal("First connection successful~n"),
    %% force disconnect last node from first 2
    ?assertEqual(Auths, lists:sort(peer:apply(Victim, erlang, nodes, []))),
    true = peer:apply(Victim, net_kernel, disconnect, [A1]),
    true = peer:apply(Victim, net_kernel, disconnect, [A2]),
    %% verify it has been disconnected
    [] = peer:apply(Victim, erlang, nodes, []),
    %% force bootstrap
    ?assertEqual(ok, peer:apply(Victim, lambda_bootstrap, discover, [])),
    %% must be connected again
    ct:pal("Moment of truth~n"),
    ?assertEqual(ok, peer:apply(Victim, lambda_test, wait_connection, [Auths])),
    stop_tier(AllPeers).

proper() ->
    [{doc, "Property-based test verifying topology, mesh-connected for authorities, and none for lambdas"}].

proper(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = lambda_test:create_release(Priv),
    try proper:quickcheck(prop_self_healing(Boot, Priv),
        [{numtests, 10}, {max_size, 50}, {start_size, 10}, long_result]) of
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

prop_self_healing(Boot, SL) ->
    proper:forall(proper_statem:commands(?MODULE, initial_state(Boot, SL)),
        fun (Cmds) ->
            {_History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
            %%%% cleanup
            cleanup(State),
            Result =/= ok andalso io:format(standard_error, "FAIL: ~200p~n", [lists:zip(Cmds, _History)]),
            Result =:= ok
        end
    ).

start_authority(TestId, Boot, ServiceLocator) ->
    lambda_test:start_node_link(TestId, Boot, [{file, ServiceLocator}], ["+S", "2:2"], true).
    %% use lambda_test:logger_config for reading extra nodes logger output
    %% ++ lambda_test:logger_config([lambda_broker, lambda_bootstrap, lambda_authority]), true).

start_broker(TestId, Boot, ServiceLocator) ->
    lambda_test:start_node_link(TestId, Boot, [{file, ServiceLocator}], ["+S", "2:2"], false).

connect(Peer1, Node2) ->
    peer:apply(Peer1, net_kernel, connect_node, [Node2]).

disconnect(Peer1, Node2) ->
    peer:apply(Peer1, net_kernel, disconnect, [Node2]).

check_state() ->
    ok.

%% Limits
-define (MAX_AUTHORITY, 8).
-define (MAX_BROKERS, 16).

-record(cluster_state, {
    test_id :: integer(),
    boot :: file:filename_all(),
    service_locator :: file:filename_all(),
    auth = #{} :: #{lambda:dst() => node()},
    brokers = #{} :: #{lambda:dst() => node()}
}).

cleanup(#cluster_state{auth = Auth, brokers = Brokers}) ->
    lambda_async:pmap([{peer, stop, [Pid]} || Pid <- maps:keys(Brokers) ++ maps:keys(Auth)]).

initial_state(Boot, Priv) ->
    TestId = case erlang:get(test_id) of undefined -> 0; Id -> Id + 1 end,
    erlang:put(test_id, TestId),
    Filename = lists:flatten(io_lib:format("srv-~b.loc", [TestId])),
    #cluster_state{test_id = TestId, boot = Boot, service_locator = filename:join(Priv, Filename)}.

initial_state() ->
    %% not used, initial state is supplied externally
    #cluster_state{}.

precondition(#cluster_state{auth = Auth}, {call, ?MODULE, start_broker, [_, _, _]}) ->
    map_size(Auth) < ?MAX_AUTHORITY;

precondition(#cluster_state{brokers = Brokers}, {call, ?MODULE, start_authority, [_, _, _]}) ->
    map_size(Brokers) < ?MAX_BROKERS;

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, peer, stop, [Pid]}) ->
    is_map_key(Pid, Auth) orelse is_map_key(Pid, Brokers);

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, connect, [Peer, Node]}) ->
    is_map_key(Peer, Auth) orelse is_map_key(Peer, Brokers) orelse
        lists:member(Node, maps:values(Auth)) orelse lists:member(Node, maps:values(Brokers));

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, disconnect, [Peer, Node]}) ->
    is_map_key(Peer, Auth) orelse is_map_key(Peer, Brokers) orelse
        lists:member(Node, maps:values(Auth)) orelse lists:member(Node, maps:values(Brokers));

precondition(_State, {call, ?MODULE, check_state, []}) ->
    true.

postcondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, check_state, []}, _Res) ->
    Peers = maps:keys(Auth) ++ maps:keys(Brokers),
    lambda_async:pmap([{peer, apply, [P, lambda_bootstrap, discover, []]} || P <- Peers]),
    %% authorities must connect to all nodes - that's a mesh!
    Nodes = maps:values(Auth) ++ maps:values(Brokers),
    Connected = lambda_async:pmap([{peer, apply, [A, lambda_test, wait_connection, [Nodes -- [N]]]}
        || {A, N} <- maps:to_list(Auth)]),
    case lists:usort(Connected) of
        [] ->
            %% no authorities, but we can check that no brokers are connected
            Empty = lambda_async:pmap([{peer, apply, [B, erlang, nodes, []]} || B <- maps:keys(Brokers)]),
            lists:all(fun (Null) -> Null =:= [] end, Empty);
        [ok] ->
            %% verify brokers are connected _only_ to authorities, but never between themselves
            Auths = lists:sort(maps:values(Auth)),
            MustBeAuths = lambda_async:pmap([{peer, apply, [B, erlang, nodes, []]} || B <- maps:keys(Brokers)]),
            Res = lists:all(fun (MaybeAuths) -> lists:sort(MaybeAuths) =:= Auths end, MustBeAuths),
            FromAuth = if Auth =/= #{} -> peer:apply(hd(maps:keys(Auth)), erlang, nodes, []); true -> [] end,
            Res orelse io:format(standard_error, "Actual: ~200p while expecting ~200p~nAuth knows ~200p and should know ~200p~n", [MustBeAuths, Auths, FromAuth, Nodes]),
            Res;
        _NotOk ->
            [
                io:format(standard_error, "Authority ~p failed connections to brokers ~200p~nReason: ~200p~n", [A, Nodes, Error])
                || {A, Error} <- lists:zip(maps:keys(Auth), Connected)],
            false
    end;

postcondition(_State, _Cmd, _Res) ->
    %% io:format(user, "~200p => ~200p~n~200p~n", [_Cmd, _Res, _State]),
    true.

%% Events possible:
%%  * start/stop an authority/broker
%%  * net splits/recovers
%% * topology verification
command(#cluster_state{test_id = TestId, auth = Auth, brokers = Brokers, boot = Boot, service_locator = SL}) ->
    %% nodes starting (authority/broker)
    AuthorityStart = {1, {call, ?MODULE, start_authority, [TestId, Boot, SL]}},
    BrokerStart = {5, {call, ?MODULE, start_broker, [TestId, Boot, SL]}},
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
    NewAuth = {call, erlang, element, [1, Res]},
    NewNode = {call, erlang, element, [2, Res]},
    State#cluster_state{auth = Auth#{NewAuth => NewNode}};

next_state(#cluster_state{brokers = Brokers} = State, Res, {call, ?MODULE, start_broker, [_, _, _]}) ->
    NewBroker = {call, erlang, element, [1, Res]},
    NewNode = {call, erlang, element, [2, Res]},
    State#cluster_state{brokers = Brokers#{NewBroker => NewNode}};

next_state(State, _Res, {call, ?MODULE, connect, [_Peer1, _Node2]}) ->
    State;

next_state(State, _Res, {call, ?MODULE, disconnect, [_Peer1, _Node2]}) ->
    State;

next_state(State, _Res, {call, ?MODULE, check_state, []}) ->
    State;

next_state(#cluster_state{auth = Auth, brokers = Brokers} = State, _Res, {call, peer, stop, [Pid]}) ->
    %% just remove Pid from all processes known
    State#cluster_state{auth = maps:remove(Pid, Auth), brokers = maps:remove(Pid, Brokers)}.
