%% @doc
%%     Tests Lambda discovery.
%% @end
-module(lambda_discovery_SUITE).
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
    epmd_fallback/0, epmd_fallback/1,
    dynamic_restart/0, dynamic_restart/1,
    late_start/0, late_start/1,
    static_start/0, static_start/1,
    names/0, names/1,
    add_del_ip/0, add_del_ip/1,
    no_start_epmd/0, no_start_epmd/1
]).

-include_lib("stdlib/include/assert.hrl").

-behaviour(ct_suite).

suite() ->
    [{timetrap, {seconds, 10}}].

groups() ->
    [{parallel, [parallel], [basic, epmd_fallback, dynamic_restart, late_start, static_start, names, add_del_ip, no_start_epmd]}].

all() ->
    [{group, parallel}].

basic() ->
    [{doc, "Tests that get_node + set_node work together both directions"}].

basic(Config) when is_list(Config) ->
    %% start two peers and connect them
    CP = filename:dirname(code:which(lambda_discovery)),
    %% don't pmap, or linked nodes will die
    VmArgs = ["-epmd_module", "lambda_discovery", "-kernel", "dist_auto_connect" , "never", "-pa", CP],
    [{ok, One}, {ok, Two}] = [
        peer:start_link(#{name => peer:random_name(), connection => standard_io,
        args => VmArgs}) || _ <- lists:seq(1, 2)],
    %% try to connect before adding the IP/Port to epmd, ensure failure
    ?assertNot(peer:call(One, net_kernel, connect_node, [Two])),
    %% now add the port/IP and ensure connection works
    AddrOne = peer:call(One, lambda_discovery, get_node, [One]),
    ?assertNotEqual(error, AddrOne),
    ok = peer:call(Two, lambda_discovery, set_node, [One, AddrOne]),
    true = peer:call(Two, net_kernel, connect_node, [One]),
    %% testing reverse access (IP address taken from connection)
    #{port := ListenPort} = peer:call(Two, lambda_discovery, get_node, []),
    %% figure out what the peer has connected to, WARNING: system internals exposed!
    TwoPort = peer:call(One, ets, lookup_element, [sys_dist, Two, 6]),
    {ok, {Ip, _EphemeralPort}} = peer:call(One, inet, peername, [TwoPort]),
    TwoAddr = #{addr => Ip, port => ListenPort},
    %% disconnect, remove mapping
    true = peer:call(Two, net_kernel, disconnect, [One]),
    ok = peer:call(Two, lambda_discovery, del_node, [One]),
    %% verify no connection
    ?assertNot(peer:call(One, net_kernel, connect_node, [Two])),
    %% add verify connection from the other side
    ok = peer:call(One, lambda_discovery, set_node, [Two, TwoAddr]),
    true = peer:call(One, net_kernel, connect_node, [Two]),
    lambda_async:pmap([{peer, stop, [N]} || N <- [One, Two]]).

epmd_fallback() ->
    [{doc, "Tests epmd fallback"}].

epmd_fallback(Config) when is_list(Config) ->
    ?assertEqual(undefined, whereis(erl_epmd)),
    {ok, Pid} = lambda_discovery:start_link(),
    ?assertNotEqual(undefined, whereis(erl_epmd)),
    %% try out two peers discovering each other via epmd
    {ok, Peer1} = peer:start_link(#{connection => standard_io, name => peer:random_name()}),
    {ok, Peer2} = peer:start_link(#{connection => standard_io, name => peer:random_name()}),
    true = peer:call(Peer1, net_kernel, connect_node, [Peer2]),
    peer:stop(Peer2),
    peer:stop(Peer1),
    gen:stop(Pid),
    ?assertEqual(undefined, whereis(erl_epmd)).

dynamic_restart() ->
    [{doc, "Tests dynamic distribution start/stop/restart with epmd_module"}].

dynamic_restart(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer} = peer:start_link(#{connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}),
    %% make the node distributed
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[peer:random_name(?FUNCTION_NAME), shortnames]]),
    compare(Peer),
    peer:stop(Peer).

compare(Peer) ->
    Node = peer:call(Peer, erlang, node, []),
    ?assertNotEqual('nonode@nohost', Node),
    %% ensure that lambda_discovery got the node rego correctly
    Rego = peer:call(Peer, lambda_discovery, get_node, [Node]),
    %% by default, erl_epmd fallback is enabled, - check that out!
    [Name, Host] = string:lexemes(atom_to_list(Node), "@"),
    {port, Port, _} = erl_epmd:port_please(Name, Host),
    ?assertEqual(Port, maps:get(port, Rego)),
    %% stop distribution
    ok = peer:call(Peer, net_kernel, stop, []),
    ?assertEqual('nonode@nohost', peer:call(Peer, erlang, node, [])),
    %% restart distribution
    {ok, _NC1} = peer:call(Peer, net_kernel, start, [[peer:random_name(?FUNCTION_NAME), shortnames]]),
    Node1 = peer:call(Peer, erlang, node, []),
    Rego1 = peer:call(Peer, lambda_discovery, get_node, [Node1]),
    {port, Port1, _} = erl_epmd:port_please(hd(string:lexemes(atom_to_list(Node1), "@")), Host),
    ?assertEqual(Port1, maps:get(port, Rego1), Rego1),
    ok.

late_start() ->
    [{doc, "Tests lambda_discovery started at various stages"}].

late_start(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    {ok, Peer} = peer:start_link(#{connection => standard_io, args => ["-pa", CP]}),
    {ok, _Apps} = peer:call(Peer, application, ensure_all_started, [lambda]),
    %% make the node distributed
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[peer:random_name(?FUNCTION_NAME), shortnames]]),
    compare(Peer),
    peer:stop(Peer).

static_start() ->
    [{doc, "Tests static distribution with/without lambda_discovery"}].

static_start(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME),
        connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}),
    Node = peer:call(Peer, erlang, node, []),
    ?assertNotEqual('nonode@nohost', Node),
    %% ensure that lambda_discovery got the node rego
    Rego = peer:call(Peer, lambda_discovery, get_node, [Node]),
    ?assertNotEqual(error, Rego),
    peer:stop(Peer),
    %% try the same but with lambda_discovery starting later as an app
    {ok, Peer1} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME),
        connection => standard_io, args => ["-pa", CP]}),
    Node1 = peer:call(Peer1, erlang, node, []),
    {ok, _Apps} = peer:call(Peer1, application, ensure_all_started, [lambda]),
    #{port := _Port, addr := _Ip} = peer:call(Peer1, lambda_discovery, get_node, [Node1]),
    %% restart distribution
    peer:stop(Peer1).

names() ->
    [{doc, "Ensure that names() works"}].

names(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    %% start node, not distributed, but with lambda_discovery
    Name = peer:random_name(?FUNCTION_NAME),
    {ok, Peer} = peer:start_link(#{name => Name,
        connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}),
    #{port := Port} = peer:call(Peer, lambda_discovery, get_node, []),
    {port, Port, _} = peer:call(Peer, lambda_discovery, port_please, string:lexemes(atom_to_list(Peer), "@")),
    ?assertEqual({ok, [{atom_to_list(Name), Port}]}, peer:call(Peer, net_adm, names, [])),
    peer:stop(Peer).

add_del_ip() ->
    [{doc, "Verifies that set_node/del_node calls corresponding inet_db functions"}].

add_del_ip(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    {ok, Peer} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME),
        connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}),
    %% ensure epmd_fallback also worked, and inet_hosts
    %%  contains some entry about the host
    ok = peer:call(Peer, lambda_discovery, set_node, ['fake@fake', #{addr => {1, 2, 3, 4}, port => 8}]),
    HostByName = peer:call(Peer, inet_hosts, gethostbyname, ["fake"]),
    ?assertEqual({ok, {hostent, "fake", [], inet, 4, [{1,2,3,4}]}}, HostByName, HostByName),
    peer:stop(Peer).

no_start_epmd() ->
    [{doc, "Tests startup with -start_epmd false"}].

no_start_epmd(Config) when is_list(Config) ->
    CP = filename:dirname(code:which(lambda_discovery)),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer} = peer:start_link(#{connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-start_epmd", "false", "-pa", CP]}),
    %% start lambda - should not fail!
    {ok, _Apps} = peer:call(Peer, application, ensure_all_started, [lambda]),
    %% make the node distributed
    Name = peer:random_name(?FUNCTION_NAME),
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[Name, shortnames]]),
    %% no bootstrap
    ?assertEqual(undefined, peer:call(Peer, erlang, whereis, [lambda_bootstrap])),
    peer:stop(Peer).
