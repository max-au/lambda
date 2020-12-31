%% @doc
%%     Tests Lambda broker
%% @end
-module(lambda_broker_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_group/2,
    end_per_group/2
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    trade/0, trade/1,
    peer/0, peer/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, local}, {group, cluster}].

groups() ->
    [{local, [basic, trade]}, {cluster, [peer]}].

init_per_group(local, Config) ->
    Addr = {epmd, "unused"}, %% use fake address for non-distributed node
    %% start discovery
    {ok, Disco} = lambda_epmd:start_link(),
    ok = lambda_epmd:set_node(node(), Addr),
    unlink(Disco),
    %% special resolver for boot
    {ok, RootBoot} = lambda_bootstrap:start_link(undefined),
    unlink(RootBoot),
    erlang:unregister(lambda_bootstrap),
    [{disco, Disco}, {boot, RootBoot} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(local, Config) ->
    gen_server:stop(?config(boot, Config)),
    gen_server:stop(?config(disco, Config)),
    proplists:delete(disco, proplists:delete(boot, Config));
end_per_group(_Group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Convenience

unreg({ok, Pid}) ->
    %% should not be needed when there is one broker per node,
    %%  but to simulate many nodes in one, this is quite helpful
    erlang:unregister(lambda_broker),
    Pid.

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Tests authority discovery within a single Erlang node"}].

basic(Config) when is_list(Config) ->
    %% start root (empty) authority
    {ok, AuthPid} = lambda_authority:start_link(?config(boot, Config)),
    %% less special resolver for non-root boot
    {ok, BootPid} = lambda_bootstrap:start_link(#{AuthPid => {epmd, "unused"}}),
    %% start a number of (unnamed) registries
    Registries = [unreg(lambda_broker:start_link(BootPid)) || _ <- lists:seq(1, 8)],
    %% ensure all registries have processed everything. Twice.
    lambda_test:sync([AuthPid | Registries]),
    %% ensure authority has discovered all of them
    ?assertEqual(lists:sort(Registries), lists:sort(lambda_authority:registries())),
    [?assertEqual([AuthPid], lambda_broker:authorities(R)) || R <- Registries],
    %% unregister root authority (so we can start another one without name clash)
    erlang:unregister(lambda_authority),
    %% start second authority
    {ok, Auth2} = lambda_authority:start_link(BootPid),
    %% let authorities sync. White-box testing here, knowing the protocol.
    lambda_test:sync([AuthPid, Auth2, AuthPid, Auth2]),
    %% ensure both authorities have the same list of connected processes
    ?assertEqual(lists:sort(gen_server:call(AuthPid, registries)),
        lists:sort(lambda_authority:registries())),
    %% ensure authorities know each other
    ?assertEqual([Auth2], gen_server:call(AuthPid, authorities)),
    ?assertEqual([AuthPid], lambda_authority:authorities()),
    %% ensure registries know authorities too
    lambda_test:sync(Registries),
    Auths = lists:sort([AuthPid, Auth2]),
    [?assertEqual(Auths, lists:sort(lambda_broker:authorities(R))) || R <- Registries],
    %% all done, stop now
    [gen:stop(Pid) || Pid <- Registries ++ [AuthPid, Auth2, BootPid]].


trade() ->
    [{doc, "Sell and buy the same quantity through the same broker"}].

trade(Config) when is_list(Config) ->
    Quantity = 100,
    %% start a broker with a single authority
    {ok, AuthPid} = lambda_authority:start_link(?config(boot, Config)),
    {ok, BootPid} = lambda_bootstrap:start_link(#{AuthPid => {epmd, "unused"}}),
    {ok, Reg} = lambda_broker:start_link(BootPid),
    erlang:unregister(lambda_broker),
    %% spawned process sells 100 through the broker
    Seller = spawn(
        fun () -> lambda_broker:sell(Reg, ?FUNCTION_NAME, Quantity), receive after infinity -> ok end end),
    %% buy 100 from the broker
    ?assertEqual(ok, lambda_broker:buy(Reg, ?FUNCTION_NAME, Quantity)),
    %% receive the order
    receive
        {order, [{Seller, Quantity}]} -> ok;
        Other -> ?assert(false, {"unexpected receive match", Other})
    after 2500 ->
        ?assert(false, "order was not executed in a timely fashion")
    end,
    %% exit the seller
    exit(Seller, normal),
    [gen_server:stop(P) || P <- [Reg, AuthPid, BootPid]].

%%--------------------------------------------------------------------
%% Real Cluster Test Cases

peer() ->
    [{doc, "Tests basic cluster discovery with actual peer Erlang node"}].

peer(Config) when is_list(Config) ->
    %% do not use the test runner node for any logic, for
    %%  the sake of isolation and "leave no trace" idea
    %% start first authority node, as we need to make a bootstrap of it
    {AuthorityPeer, AuthorityNode} = lambda_test:start_node_link(undefined, [], true),
    %% form the bootstrap
    Addr = peer:apply(AuthorityPeer, lambda_epmd, get_node, []),
    Bootstrap = #{{lambda_authority, AuthorityNode} => Addr},
    %% start extra nodes
    Peers = lambda_test:start_nodes(Bootstrap, 4),
    {Peers1, ExpectedWorkers} = lists:unzip(Peers),
    %% ensure they all find the authority
    WorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], ExpectedWorkers -- WorkerNodes, "missing initial nodes"),
    ?assertEqual([], WorkerNodes -- ExpectedWorkers, "unexpected initial nodes"),
    %% start more nodes, don't give them authority addresses
    NonAuth = lambda_test:start_nodes(Bootstrap, 4),
    {Peers2, NonAuthWN} = lists:unzip(NonAuth),
    %% verify there are 8 nodes connected to this authority
    AllWorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], (ExpectedWorkers ++ NonAuthWN) -- AllWorkerNodes, "missing extra nodes"),
    ?assertEqual([], (AllWorkerNodes -- ExpectedWorkers) -- NonAuthWN, "unexpected extra nodes"),
    %% start a second authority
    {SecondAuthPeer, SecondAuthNode} = lambda_test:start_node_link(Bootstrap, [], true),
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
    [peer:stop(P) || P <- Peers1 ++ Peers2 ++ [AuthorityPeer, SecondAuthPeer]],
    ok.
