%% @doc
%%     Tests Lambda discovery.
%% @end
-module(lambda_discovery_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    basic/0, basic/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    [basic].

init_per_suite(Config) ->
    {ok, Disco} = lambda_discovery:start_link(),
    unlink(Disco),
    Config1 = [{disco, Disco} | Config],
    case erlang:is_alive() of
        true ->
            Config1;
        false ->
            {ok, Pid} = net_kernel:start([list_to_atom(lists:concat([?MODULE, "_", os:getpid()])), shortnames]),
            [{net_kernel, Pid} | Config1]
    end.

end_per_suite(Config) ->
    gen:stop(proplists:get_value(disco, Config)),
    Config1 = proplists:delete(disco, Config),
    case proplists:get_value(net_kernel, Config1) of
        undefined ->
            Config1;
        Pid when is_pid(Pid) ->
            net_kernel:stop(),
            lists:delete(net_kernel, Config1)
    end.

basic() ->
    [{doc, "Tests simple EPMD scenarios"}].

basic(Config) when is_list(Config) ->
    %% start a peer with no epmd whatsoever, but make it listen
    %%  to a specific port
    Node = peer:random_name(),
    CP = filename:dirname(code:which(lambda_discovery)),
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => ["-epmd_module", "lambda_discovery", "-pa", CP]}),
    %% try to connect before adding the IP/Port to epmd, ensure failure
    false = net_kernel:connect_node(Node),
    %% now add the port/IP and ensure connection works
    Addr = peer:apply(Peer, lambda_discovery, get_node, [Node]),
    ?assertNotEqual(error, Addr),
    ok = lambda_discovery:set_node(Node, Addr),
    true = net_kernel:connect_node(Node),
    %% testing reverse access (IP address taken from connection)
    {_, ListenPort} = lambda_discovery:get_node(),
    %% figure out what the peer has connected to
    SelfPort = rpc:call(Node, ets, lookup_element, [sys_dist, node(), 6]),
    {ok, {Ip, _EphemeralPort}} = rpc:call(Node, inet, peername, [SelfPort]),
    SelfAddr = {Ip, ListenPort},
    %% disconnect, remove mapping
    true = net_kernel:disconnect(Node),
    ok = lambda_discovery:del_node(Node),
    %% verify no connection
    false = net_kernel:connect_node(Node),
    %% add verify connection from the other side
    ok = peer:apply(Peer, lambda_discovery, set_node, [node(), SelfAddr]),
    true = peer:apply(Peer, net_kernel, connect_node, [node()]),
    peer:stop(Peer).
