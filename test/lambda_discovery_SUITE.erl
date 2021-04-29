%% @doc
%%     Tests Lambda discovery.
%% @end
-module(lambda_discovery_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1
]).

-include_lib("stdlib/include/assert.hrl").

-behaviour(ct_suite).

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic].

basic() ->
    [{doc, "Tests that get_node + set_node work together both directions"}].

basic(Config) when is_list(Config) ->
    %% start two peers and connect them
    CP = filename:dirname(code:which(lambda_discovery)),
    %% don't pmap, or linked nodes will die
    [{ok, One}, {ok, Two}] = [
        peer:start_link(#{name => peer:random_name(), connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}) || _ <- lists:seq(1, 2)],
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
