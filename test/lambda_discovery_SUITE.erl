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
-include_lib("common_test/include/ct.hrl").

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
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    %% don't pmap, or linked nodes will die
    VmArgs = ["-epmd_module", "lambda_discovery", "-kernel", "dist_auto_connect", "never",
        "-kernel", "epmd_fallback", "false" | CodePath],
    [{ok, PeerOne, One}, {ok, PeerTwo, Two}] = [
        ?CT_PEER(#{connection => standard_io,
        args => VmArgs}) || _ <- lists:seq(1, 2)],
    %% try to connect before adding the IP/Port to epmd, ensure failure
    ?assertNot(peer:call(PeerOne, net_kernel, connect_node, [Two])),
    %% now add the port/IP and ensure connection works
    AddrOne = peer:call(PeerOne, lambda_discovery, get_node, [One]),
    ?assertNotEqual(error, AddrOne),
    ok = peer:call(PeerTwo, lambda_discovery, set_node, [One, AddrOne]),
    true = peer:call(PeerTwo, net_kernel, connect_node, [One]),
    %% testing reverse access (IP address taken from connection)
    #{port := ListenPort} = peer:call(PeerTwo, lambda_discovery, get_node, []),
    %% figure out what the peer has connected to, WARNING: system internals exposed!
    TwoPort = peer:call(PeerOne, ets, lookup_element, [sys_dist, Two, 6]),
    {ok, {Ip, _EphemeralPort}} = peer:call(PeerOne, inet, peername, [TwoPort]),
    TwoAddr = #{addr => Ip, port => ListenPort},
    %% disconnect, remove mapping
    true = peer:call(PeerTwo, net_kernel, disconnect, [One]),
    ok = peer:call(PeerTwo, lambda_discovery, del_node, [One]),
    %% verify no connection
    ?assertNot(peer:call(PeerOne, net_kernel, connect_node, [Two])),
    %% add verify connection from the other side
    ok = peer:call(PeerOne, lambda_discovery, set_node, [Two, TwoAddr]),
    true = peer:call(PeerOne, net_kernel, connect_node, [Two]),
    lambda_async:pmap([{peer, stop, [N]} || N <- [PeerOne, PeerTwo]]).

epmd_fallback() ->
    [{doc, "Tests epmd fallback"}].

epmd_fallback(Config) when is_list(Config) ->
    ?assertEqual(undefined, whereis(erl_epmd)),
    {ok, Pid} = lambda_discovery:start_link(),
    ?assertNotEqual(undefined, whereis(erl_epmd)),
    %% try out two peers discovering each other via epmd
    {ok, Peer1, _} = ?CT_PEER(#{connection => standard_io}),
    {ok, Peer2, Node2} = ?CT_PEER(#{connection => standard_io}),
    true = peer:call(Peer1, net_kernel, connect_node, [Node2]),
    peer:stop(Peer2),
    peer:stop(Peer1),
    gen:stop(Pid),
    ?assertEqual(undefined, whereis(erl_epmd)).

dynamic_restart() ->
    [{doc, "Tests dynamic distribution start/stop/restart with epmd_module"}].

dynamic_restart(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer, _} = peer:start_link(#{connection => standard_io,
        args => ["-epmd_module", "lambda_discovery" | CodePath]}),
    %% make the node distributed
    Name = list_to_atom(peer:random_name(?FUNCTION_NAME)),
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[Name, shortnames]]),
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
    Name2 = list_to_atom(peer:random_name(?FUNCTION_NAME)),
    {ok, _NC1} = peer:call(Peer, net_kernel, start, [[Name2, shortnames]]),
    Node1 = peer:call(Peer, erlang, node, []),
    Rego1 = peer:call(Peer, lambda_discovery, get_node, [Node1]),
    {port, Port1, _} = erl_epmd:port_please(hd(string:lexemes(atom_to_list(Node1), "@")), Host),
    ?assertEqual(Port1, maps:get(port, Rego1), Rego1),
    ok.

late_start() ->
    [{doc, "Tests lambda_discovery started at various stages"}].

late_start(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    {ok, Peer, _Node} = peer:start_link(#{connection => standard_io, args => CodePath}),
    {ok, _Apps} = peer:call(Peer, application, ensure_all_started, [lambda]),
    %% make the node distributed
    Name = list_to_atom(peer:random_name(?FUNCTION_NAME)),
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[Name, shortnames]]),
    compare(Peer),
    peer:stop(Peer).

static_start() ->
    [{doc, "Tests static distribution with/without lambda_discovery"}].

static_start(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer, Node} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME),
        connection => standard_io,
        args => ["-epmd_module", "lambda_discovery" | CodePath]}),
    ?assertNotEqual('nonode@nohost', Node),
    %% ensure that lambda_discovery got the node rego
    Rego = peer:call(Peer, lambda_discovery, get_node, [Node]),
    ?assertNotEqual(error, Rego),
    peer:stop(Peer),
    %% try the same but with lambda_discovery starting later as an app
    {ok, Peer1, Node1} = ?CT_PEER(#{connection => standard_io, args => CodePath}),
    {ok, _Apps} = peer:call(Peer1, application, ensure_all_started, [lambda]),
    #{port := _Port, addr := _Ip} = peer:call(Peer1, lambda_discovery, get_node, [Node1]),
    peer:stop(Peer1).

names() ->
    [{doc, "Ensure that names() works"}].

names(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer, Node} = ?CT_PEER(#{connection => standard_io, name => peer:random_name(?FUNCTION_NAME),
        args => ["-epmd_module", "lambda_discovery" | CodePath]}),
    #{port := Port} = peer:call(Peer, lambda_discovery, get_node, []),
    [Name, Host] = string:lexemes(atom_to_list(Node), "@"),
    {port, Port, _} = peer:call(Peer, lambda_discovery, port_please, [Name, Host]),
    ?assertEqual({ok, [{Name, Port}]}, peer:call(Peer, net_adm, names, [])),
    peer:stop(Peer).

add_del_ip() ->
    [{doc, "Verifies that set_node/del_node calls corresponding inet_db functions"}].

add_del_ip(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    {ok, Peer, _Node} = ?CT_PEER(#{
        connection => standard_io,
        args => ["-epmd_module", "lambda_discovery" | CodePath]}),
    %% ensure epmd_fallback also worked, and inet_hosts
    %%  contains some entry about the host
    ok = peer:call(Peer, lambda_discovery, set_node, ['fake@fake', #{addr => {1, 2, 3, 4}, port => 8}]),
    HostByName = peer:call(Peer, inet_hosts, gethostbyname, ["fake"]),
    ?assertEqual({ok, {hostent, "fake", [], inet, 4, [{1,2,3,4}]}}, HostByName, HostByName),
    peer:stop(Peer).

no_start_epmd() ->
    [{doc, "Tests startup with -start_epmd false"}].

no_start_epmd(Config) when is_list(Config) ->
    Paths = [["-pa", filename:dirname(code:which(M))] || M <- [lambda, argparse]],
    CodePath = lists:concat(Paths),
    %% start node, not distributed, but with lambda_discovery
    {ok, Peer, _Node} = peer:start_link(#{connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-start_epmd", "false" | CodePath]}),
    %% start lambda - should not fail!
    {ok, _Apps} = peer:call(Peer, application, ensure_all_started, [lambda]),
    %% make the node distributed
    Name = list_to_atom(peer:random_name(?FUNCTION_NAME)),
    {ok, _NC} = peer:call(Peer, net_kernel, start, [[Name, shortnames]]),
    %% no bootstrap
    ?assertEqual(undefined, peer:call(Peer, erlang, whereis, [lambda_bootstrap])),
    peer:stop(Peer).
