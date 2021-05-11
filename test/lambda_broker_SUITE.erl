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

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, local}, {group, cluster}].

groups() ->
    [{local, [basic, trade]}, {cluster, [peer]}].

init_per_group(local, Config) ->
    %% start discovery
    {ok, Disco} = lambda_discovery:start_link(),
    lambda_discovery:set_node(node(), #{addr => {127, 0, 0, 1}, port => 1}), %% not distributed
    unlink(Disco),
    [{disco, Disco} | Config];
init_per_group(_Group, Config) ->
    Boot = lambda_test:create_release(proplists:get_value(priv_dir, Config)),
    [{boot, Boot} | Config].

end_per_group(local, Config) ->
    gen_server:stop(proplists:get_value(disco, Config)),
    proplists:delete(disco, Config);
end_per_group(_Group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Convenience

%% Starts an authority, and returns bootstrap to start the rest
%%  of the system.
start_authority(Peers) ->
    {ok, AuthPid} = gen_server:start_link(lambda_authority, [], []),
    lambda_authority:peers(AuthPid, Peers),
    {AuthPid, #{AuthPid => lambda_discovery:get_node()}}.

start_broker(Auth) ->
    {ok, BrokerPid} = gen_server:start_link(lambda_broker, [], []),
    lambda_broker:authorities(BrokerPid, Auth),
    BrokerPid.

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Tests authority discovery within a single Erlang node"}].

basic(Config) when is_list(Config) ->
    %% start root (empty) authority
    {AuthPid, Bootstrap} = start_authority(#{}),
    %% start a number of (unnamed) brokers pointing at authority
    Brokers = [start_broker(Bootstrap) || _ <- lists:seq(1, 8)],
    %% ensure authorities finished processing from broker point of view
    [lambda_test:sync_via(Broker, AuthPid) || Broker <- Brokers],
    %% ensure authority has discovered all of them
    ?assertEqual(lists:sort(Brokers), lists:sort(lambda_authority:brokers(AuthPid))),
    [?assertEqual([AuthPid], lambda_broker:authorities(R)) || R <- Brokers],
    %% start second authority
    {Auth2, _} = start_authority(Bootstrap),
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
    [gen:stop(Pid) || Pid <- Brokers ++ [AuthPid, Auth2]].


trade() ->
    [{doc, "Sell and buy the same quantity through the same broker"}].

trade(Config) when is_list(Config) ->
    Quantity = 100,
    %% start a broker with a single authority
    {AuthPid, Bootstrap} = start_authority(#{}),
    Reg = start_broker(Bootstrap),
    %% spawned process sells 100 through the broker
    Seller = spawn(
        fun () -> lambda_broker:sell(Reg, ?FUNCTION_NAME, Quantity), receive after infinity -> ok end end),
    %% buy 100 from the broker
    ?assertEqual(ok, lambda_broker:buy(Reg, ?FUNCTION_NAME, Quantity)),
    %% receive the order
    receive
        {order, [{Seller, Quantity, #{}}]} -> ok;
        Other -> ?assert(false, {"unexpected receive match", Other})
    after 2500 ->
        ?assert(false, "order was not executed in a timely fashion")
    end,
    %% exit the seller
    exit(Seller, normal),
    [gen_server:stop(P) || P <- [Reg, AuthPid]].

%%--------------------------------------------------------------------
%% Real Cluster Test Cases

peer() ->
    [{doc, "Tests basic cluster discovery with actual peer Erlang node"}].

peer(Config) when is_list(Config) ->
    %% do not use the test runner node for any logic, for
    %%  the sake of isolation and "leave no trace" idea
    %% start first authority node, as we need to make a bootstrap of it
    Boot = proplists:get_value(boot, Config),
    AuthorityNode = lambda_test:start_node_link(undefined, Boot, undefined, [], true),
    %% form the bootstrap
    Addr = peer:call(AuthorityNode, lambda_discovery, get_node, []),
    BootSpec = {static, #{{lambda_authority, AuthorityNode} => Addr}},
    %% start extra nodes
    ExpectedWorkers = lambda_test:start_nodes(undefined, Boot, BootSpec, 4),
    %% ensure they all find the authority
    WorkerNodes = peer:call(AuthorityNode, erlang, nodes, []),
    ?assertEqual([], ExpectedWorkers -- WorkerNodes, "missing initial nodes"),
    ?assertEqual([], WorkerNodes -- ExpectedWorkers, "unexpected initial nodes"),
    %% start more nodes, don't give them authority addresses
    NonAuthWN = lambda_test:start_nodes(undefined, Boot, BootSpec, 4),
    %% verify there are 8 nodes connected to this authority
    AllWorkerNodes = peer:call(AuthorityNode, erlang, nodes, []),
    ?assertEqual([], (ExpectedWorkers ++ NonAuthWN) -- AllWorkerNodes, "missing extra nodes"),
    ?assertEqual([], (AllWorkerNodes -- ExpectedWorkers) -- NonAuthWN, "unexpected extra nodes"),
    %% start a second authority
    SecondAuthNode = lambda_test:start_node_link(undefined, Boot, BootSpec, [], true),
    %% flush all queues from all nodes
    ct:sleep(2000),
    %% verify both authorities have 9 connected nodes (8 non-authority)
    AllNodes = peer:call(AuthorityNode, erlang, nodes, []) ++ [AuthorityNode],
    AllNodes2 = peer:call(SecondAuthNode, erlang, nodes, []) ++ [SecondAuthNode],
    ?assertEqual(lists:sort(AllNodes), lists:sort(AllNodes2)),
    ?assertEqual(length(AllNodes), 10),
    %% ensure nodes are not mesh-connected
    Authorities = [AuthorityNode, SecondAuthNode],
    [?assertEqual(Authorities, peer:call(Peer, erlang, nodes, [])) || Peer <- ExpectedWorkers ++ NonAuthWN],
    %% shut all down
    [peer:stop(P) || P <- ExpectedWorkers ++ NonAuthWN ++ [AuthorityNode, SecondAuthNode]],
    ok.
