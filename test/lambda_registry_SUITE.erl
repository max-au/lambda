%% @doc
%%     Tests Lambda process registry
%% @end
-module(lambda_registry_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    peer/0, peer/1
]).

%% Internal exports to spawn remotely
-export([
    wait_connection/1
]).


-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, local}, {group, cluster}].

groups() ->
    [{local, [basic]}, {cluster, [peer]}].

%%--------------------------------------------------------------------
%% Convenience

start_node(Bootstrap, Authority) ->
    Node = peer:random_name(),
    CP = filename:dirname(code:which(lambda_registry)),
    TestCP = filename:dirname(code:which(?MODULE)),
    Auth = if Authority -> ["-lambda", "authority", "true"]; true -> [] end,
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => [
            %% "-kernel", "dist_auto_connect", "never",
            "-start_epmd", "false",
            "-epmd_module", "lambda_epmd",
            %"-kernel", "logger", "[{handler, default, logger_std_h,#{config => #{type => standard_error}, formatter => {logger_formatter, #{ }}}}]",
            %"-kernel", "logger_level", "all",
            "-pa", CP, "-pa", TestCP] ++ Auth}),
    Bootstrap =/= #{} andalso
        peer:apply(Peer, application, set_env, [lambda, bootstrap, Bootstrap, [{persistent, true}]]),
    {ok, _Apps} = peer:apply(Peer, application, ensure_all_started, [lambda]),
    Bootstrap =/= #{} andalso
        begin
            {_, BootNodes} = lists:unzip(maps:keys(Bootstrap)),
            ok = peer:apply(Peer, ?MODULE, wait_connection, [BootNodes])
        end,
    unlink(Peer),
    {Peer, Node}.

-include_lib("kernel/include/logger.hrl").

%% executed in the remote node: waits until registry finds some
%%  authority
wait_connection(Nodes) ->
    net_kernel:monitor_nodes(true),
    wait_nodes(Nodes),
    net_kernel:monitor_nodes(false).

wait_nodes([]) ->
    ok;
wait_nodes([Node | Nodes]) ->
    %% subscribe to all nodeup events and wait...
    case lists:member(Node, [node() | nodes()]) of
        true ->
            wait_nodes(lists:delete(Node, Nodes));
        false ->
            receive
                {nodeup, Node} ->
                    wait_nodes(lists:delete(Node, Nodes))
            end
    end.

unreg({ok, Pid}) ->
    %% should not be needed when there is one registry per node
    erlang:unregister(lambda_registry),
    Pid.

sync([Pid]) ->
    sys:get_state(Pid);
sync([Pid | More]) ->
    sys:get_state(Pid),
    sync(More);
sync(Pid) when is_pid(Pid) ->
    sys:get_state(Pid).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Tests authority discovery within a single Erlang node"}].

basic(Config) when is_list(Config) ->
    Addr = {epmd, "localhost"}, %% use fake address for non-distributed node
    %% start discovery
    {ok, Disco} = lambda_epmd:start_link(),
    ok = lambda_epmd:set_node(node(), Addr),
    %% start root (empty) authority
    {ok, AuthPid} = lambda_authority:start_link(#{}),
    Bootstrap = #{AuthPid => Addr},
    %% start a number of (unnamed) registries
    Registries = [unreg(lambda_registry:start_link(Bootstrap)) || _ <- lists:seq(1, 8)],
    %% ensure all registries have processed everything. Twice.
    sync([AuthPid | Registries]),
    %% ensure authority has discovered all of them
    ?assertEqual(lists:sort(Registries), lists:sort(lambda_authority:registries())),
    [?assertEqual([AuthPid], lambda_registry:authorities(R)) || R <- Registries],
    %% unregister root authority (so we can start another one without name clash)
    erlang:unregister(lambda_authority),
    %% start second authority
    {ok, Auth2} = lambda_authority:start_link(Bootstrap),
    %% let authorities sync. White-box testing here, knowing the protocol.
    sync([AuthPid, Auth2, AuthPid, Auth2]),
    %% ensure both authorities have the same list of connected processes
    ?assertEqual(lists:sort(gen_server:call(AuthPid, registries)),
        lists:sort(lambda_authority:registries())),
    %% ensure authorities know each other
    ?assertEqual([Auth2], gen_server:call(AuthPid, authorities)),
    ?assertEqual([AuthPid], lambda_authority:authorities()),
    %% ensure registries know authorities too
    sync(Registries),
    Auths = lists:sort([AuthPid, Auth2]),
    [?assertEqual(Auths, lists:sort(lambda_registry:authorities(R))) || R <- Registries],
    %% all done, stop now
    [gen:stop(Pid) || Pid <- Registries ++ [AuthPid, Disco, Auth2]].


%%--------------------------------------------------------------------
%% Real Cluster Test Cases

peer() ->
    [{doc, "Tests basic cluster discovery with actual peer Erlang node"}].

peer(Config) when is_list(Config) ->
    %% do not use the test runner node for any logic, for
    %%  the sake of isolation and "leave no trace" idea
    %% start first authority node, as we need to make a bootstrap of it
    {AuthorityPeer, AuthorityNode} = start_node(#{}, true),
    %% form the bootstrap
    Addr = peer:apply(AuthorityPeer, lambda_epmd, get_node, [AuthorityNode]),
    Bootstrap = #{{lambda_authority, AuthorityNode} => Addr},
    %% start extra nodes
    %% Peers = [start_node(Bootstrap, false) || _ <- lists:seq(1, 4)],
    Peers = lambda_async:pmap([{fun start_node/2, [Bootstrap, false]} || _ <- lists:seq(1, 4)]),
    {Peers1, ExpectedWorkers} = lists:unzip(Peers),
    %% ensure they all find the authority
    WorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], ExpectedWorkers -- WorkerNodes, "missing initial nodes"),
    ?assertEqual([], WorkerNodes -- ExpectedWorkers, "unexpected initial nodes"),
    %% start more nodes, don't give them authority addresses
    NonAuth = lambda_async:pmap([{fun start_node/2, [Bootstrap, false]} || _ <- lists:seq(1, 4)]),
    {Peers2, NonAuthWN} = lists:unzip(NonAuth),
    %% verify there are 8 nodes connected to this authority
    AllWorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], (ExpectedWorkers ++ NonAuthWN) -- AllWorkerNodes, "missing extra nodes"),
    ?assertEqual([], (AllWorkerNodes -- ExpectedWorkers) -- NonAuthWN, "unexpected extra nodes"),
    %% start a second authority
    {SecondAuthPeer, SecondAuthNode} = start_node(Bootstrap, true),
    %% flush all queues from all nodes
    timer:sleep(2000),
    %% verify both authorities have 9 connected nodes (8 non-authority)
    AllNodes = peer:apply(AuthorityPeer, erlang, nodes, []) ++ [AuthorityNode],
    AllNodes2 = peer:apply(SecondAuthPeer, erlang, nodes, []) ++ [SecondAuthNode],
    ?assertEqual(lists:sort(AllNodes), lists:sort(AllNodes2)),
    ?assertEqual(length(AllNodes), 10),
    %% ensure nodes are not mesh-connected
    Authorities = [AuthorityNode, SecondAuthNode],
    [?assertEqual(Authorities, peer:apply(Peer, erlang, nodes, [])) || Peer <- Peers1 ++ Peers2],
    %% shut all down
    [peer:stop(P) || P <- Peers1 ++ Peers2],
    [peer:stop(P) || P <- [AuthorityPeer, SecondAuthPeer]],
    ok.
