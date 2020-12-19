%% @doc
%%     Tests Lambda discovery.
%% @end
-module(lambda_epmd_SUITE).
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
    _ = lambda_epmd:replace(),
    case erlang:is_alive() of
        true ->
            Config;
        false ->
            LongShort = case inet_db:res_option(domain) of
                            [] ->
                                shortnames;
                            Domain when is_list(Domain) ->
                                longnames
                        end,
            {ok, Pid} = net_kernel:start([list_to_atom(lists:concat(["dist_", os:getpid()])), LongShort]),
            [{net_kernel, Pid} | Config]
    end.

end_per_suite(Config) ->
    case proplists:get_value(net_kernel, Config) of
        undefined ->
            Config;
        Pid when is_pid(Pid) ->
            net_kernel:stop(),
            lists:delete(net_kernel, Config)
    end.

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Tests simple EPMD scenarios"}].

basic(Config) when is_list(Config) ->
    %% start a peer with no epmd whatsoever, but make it listen
    %%  to a specific port
    Node = peer:random_name(),
    CP = filename:dirname(code:which(lambda_epmd)),
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => ["-epmd_module", "lambda_epmd", "-pa", CP]}),
    %% try to connect before adding the IP/Port to epmd, ensure failure
    false = net_kernel:connect_node(Node),
    %% now add the port/IP and ensure connection works
    {ok, Addr} = peer:apply(Peer, lambda_epmd, get_node, [Node]),
    ok = lambda_epmd:set_node(Node, Addr),
    true = net_kernel:connect_node(Node),
    %% testing reverse access (IP address taken from connection)
    {ok, {_, _, ListenPort}} = lambda_epmd:get_node(node()),
    %% figure out what the peer has connected to
    SelfPort = rpc:call(Node, ets, lookup_element, [sys_dist, node(), 6]),
    {ok, {Ip, _EphemeralPort}} = rpc:call(Node, inet, peername, [SelfPort]),
    SelfAddr = case Ip of
                   V4 when tuple_size(V4) =:= 4 -> {inet, Ip, ListenPort};
                   V6 when tuple_size(V6) =:= 8 -> {inet6, Ip, ListenPort}
               end,
    %% disconnect, remove mapping
    true = net_kernel:disconnect(Node),
    ok = lambda_epmd:del_node(Node),
    %% verify no connection
    false = net_kernel:connect_node(Node),
    %% add verify connection from the other side
    ok = peer:apply(Peer, lambda_epmd, set_node, [node(), SelfAddr]),
    true = peer:apply(Peer, net_kernel, connect_node, [node()]),
    peer:stop(Peer).
