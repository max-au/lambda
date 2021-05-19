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
    trade/0, trade/1,
    trade_down/0, trade_down/1,
    peer/0, peer/1,
    late_broker_start/0, late_broker_start/1
]).

%% Internal exports, only for testing
-export([
    monitored_peers/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, local}, {group, cluster}].

groups() ->
    [
        {local, [parallel], [trade, trade_down]},
        {cluster, [parallel], [late_broker_start, peer]}
    ].

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
        {complete_order, [{Seller, Quantity, #{}}]} ->
            %% ensure authority still knows about the module
            ?assertEqual([?FUNCTION_NAME], lambda_authority:modules(AuthPid)),
            ok;
        Other -> ?assert(false, {"unexpected receive match", Other})
    after 2500 ->
        ?assert(false, "order was not executed in a timely fashion")
    end,
    %% exit the seller
    exit(Seller, normal),
    [gen_server:stop(P) || P <- [Reg, AuthPid]].

trade_down() ->
    [{doc, "Test that orders are discarded when PLB/listener go down"}].

trade_down(Config) when is_list(Config) ->
    {AuthPid, Bootstrap} = start_authority(#{}),
    Broker = start_broker(Bootstrap),
    %% spawned process sells 100 through the broker
    Seller = spawn(
        fun () -> lambda_broker:sell(Broker, ?FUNCTION_NAME, 1), receive after infinity -> ok end end),
    %% ensure it reached the exchange
    lambda_test:sync([Broker, AuthPid, Broker, AuthPid]),
    [Exch] = lambda_broker:exchanges(Broker, ?FUNCTION_NAME),
    ?assertMatch([{order, _, 1, Broker, _, _, _}], lambda_exchange:orders(Exch, #{type => sell})),
    %% terminate seller
    exit(Seller, kill),
    lambda_test:sync([Broker, AuthPid, Broker]),
    %% ensure orders disappeared from broker
    ?assertEqual([], lambda_broker:orders(Broker, #{type => sell})),
    %% ... and exchange
    ?assertEqual([], lambda_exchange:orders(Exch, #{type => sell})),
    %% perform same steps, now for buy order
    Buyer = spawn(
        fun () -> lambda_broker:buy(Broker, ?FUNCTION_NAME, 2), receive after infinity -> ok end end),
    lambda_test:sync([Broker, Exch, Broker]),
    ?assertMatch([{order, _, 2, Broker, _, _, _}], lambda_exchange:orders(Exch, #{type => buy})),
    exit(Buyer, kill),
    lambda_test:sync([Broker, Exch, Broker]),
    ?assertEqual([], lambda_exchange:orders(Exch, #{type => buy})),
    [gen_server:stop(P) || P <- [Broker, AuthPid]].

%%--------------------------------------------------------------------
%% Real Cluster Test Cases

monitored_peers(Authorities) ->
    MRef = monitor(process, whereis(lambda_broker)),
    lambda_broker:authorities(lambda_broker, Authorities),
    receive
        {'DOWN', MRef, process, _Pid, Reason} ->
            {error, Reason}
    after 500 ->
        ok
    end.

late_broker_start() ->
    [{doc, "Start a broker when lambda_discovery has no local node yet"}].

late_broker_start(Config) when is_list(Config) ->
    %% authority node
    Boot = proplists:get_value(boot, Config),
    AuthorityNode = lambda_test:start_node_link(undefined, Boot, undefined,  [], true),
    LongShort = case peer:call(AuthorityNode, net_kernel, longnames, []) of
                    true -> longnames;
                    false -> shortnames
                end,
    Addr = peer:call(AuthorityNode, lambda_discovery, get_node, []),
    Authorities = #{{lambda_authority, AuthorityNode} => Addr},
    %% broker node
    CP = filename:dirname(code:which(lambda_discovery)),
    CP1 = filename:dirname(code:which(?MODULE)),
    {ok, Peer} = peer:start_link(#{
        connection => standard_io,
        args => ["-setcookie", "lambda", "-epmd_module", "lambda_discovery", "-pa", CP, "-pa", CP1]}),
    %% start lambda - should not fail!
    {ok, _Apps} = peer:call(Peer, application, ensure_all_started, [lambda]),
    %% ensure broker does not crash when some peers are pushed to it
    ?assertEqual(ok, peer:call(Peer, ?MODULE, monitored_peers, [Authorities])),
    %% make the node distributed
    Name = peer:random_name(?FUNCTION_NAME),
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[Name, LongShort]]),
    %% discover and connect
    ok = peer:call(Peer, lambda_broker, authorities, [lambda_broker, Authorities]),
    ok = peer:call(Peer, lambda_test, wait_connection, [[AuthorityNode]]),
    %% ensure authority got the broker connection
    Brokers = peer:call(AuthorityNode, lambda_authority, brokers, [lambda_authority]),
    %% must be exactly 2 brokers
    ?assertEqual(2, length(Brokers), [node(B) || B <- Brokers]),
    %% stop everything
    lambda_async:pmap([{peer, stop, [N]} || N <- [AuthorityNode, Peer]]).

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
