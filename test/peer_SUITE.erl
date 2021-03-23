%%% @doc
%%% Smoke tests for peer node controller
%%% @end
-module(peer_SUITE).
-author("maximfca@gmail.com").

%% Common Test headers
-include_lib("stdlib/include/assert.hrl").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

%% Test cases
-export([
    dist/0, dist/1,
    two_way_link/0, two_way_link/1,
    dist_io_redirect/0, dist_io_redirect/1,
    basic/0, basic/1,
    detached/0, detached/1,
    dyn_peer/0, dyn_peer/1,
    stop_peer/0, stop_peer/1,
    init_debug/0, init_debug/1,
    io_redirect/0, io_redirect/1,
    multi_node/0, multi_node/1,
    dist_up_down/0, dist_up_down/1,
    duplicate_name/0, duplicate_name/1,
    ssh/0, ssh/1,
    docker/0, docker/1
]).

suite() ->
    [{timetrap, {seconds, 5}}].

groups() ->
    [
        {dist, [parallel], [dist, two_way_link, dist_io_redirect, dist_up_down]},
        {oob, [parallel], [basic, detached, dyn_peer, stop_peer, init_debug, io_redirect, multi_node, duplicate_name]},
        {remote, [parallel], [ssh, docker]}
    ].

all() ->
    [{group, dist}, {group, oob}, {group, remote}].
    %% [{group, dist}, {group, oob}].

init_per_suite(Config) ->
    case erl_epmd:names() of
        {ok, _} -> Config;
        _X -> os:cmd("epmd -daemon"), Config
    end.

end_per_suite(Config) ->
    Config.

init_per_group(dist, Config) ->
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
            {ok, Pid} = net_kernel:start([list_to_atom("dist_" ++ os:getpid()), LongShort]),
            [{net_kernel, Pid} | Config]
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(dist, Config) ->
    case proplists:get_value(net_kernel, Config) of
        undefined ->
            Config;
        Pid when is_pid(Pid) ->
            net_kernel:stop(),
            lists:delete(net_kernel, Config)
    end;
end_per_group(_TestCase, Config) ->
    Config.

%% -------------------------------------------------------------------
%% Distribution-enabled cases

dist() ->
    [{doc, "Classic behaviour: detached peer with no OOB, connects via dist"}].

%% Classic 'slave': start new node locally, with random name.
dist(Config) when is_list(Config) ->
    {ok, Node} = peer:start_link(),
    %% distribution is expected to be connected
    ?assertEqual(Node, rpc:call(Node, erlang, node, [])),
    %% but not oob...
    ?assertException(error, noconnection, peer:call(Node, erlang, node, [])),
    %% unlink and monitor
    ?assertEqual(true, net_kernel:disconnect(Node)),
    %% ^^^ this makes the node go down
    ?assertEqual({badrpc, nodedown}, rpc:call(Node, erlang, node, [])),
    peer:stop(Node).

two_way_link() ->
    [{doc, "Tests two-way link"}].

two_way_link(Config) when is_list(Config) ->
    Name = peer:random_name(),
    {ok, Node} = peer:start_link(#{name => Name, oneway => false}),
    %% verify node started locally
    ?assertEqual(Node, rpc:call(Node, erlang, node, [])),
    %% verify there is no OOB connection
    ?assertException(error, noconnection, peer:call(Node, erlang, node, [])),
    %% unlink and monitor
    Pid = whereis(Node),
    unlink(Pid),
    MRef = monitor(process, Pid),
    ?assertEqual(true, net_kernel:disconnect(Node)),
    %% ^^^ this makes the node go down
    %% since two-way link is requested, it triggers peer to stop
    receive
        {'DOWN', MRef, process, Pid, {nodedown, Node}} ->
            ok
    after 2000 ->
        link(Pid),
        {fail, disconnect_timeout}
    end.

dist_io_redirect() ->
    [{doc, "Tests i/o redirection working for dist"}].

dist_io_redirect(Config) when is_list(Config) ->
    Name = lists:concat([?FUNCTION_NAME, "-", os:getpid()]),
    {ok, Host} = inet:gethostname(),
    Node = list_to_atom(lists:concat([Name, "@", Host])),
    {ok, Pid} = peer:start_link(#{name => Name, register => false}),
    ct:capture_start(),
    rpc:call(Node, io, format, ["test."]),
    ?assertEqual(ok, peer:send(Pid, init, {stop, stop})),
    rpc:call(Node, erlang, apply, [io, format, ["second."]]),
    peer:stop(Pid),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    ?assertEqual(["test.", "second."], Texts).

dist_up_down() ->
    [{doc, "Test that Erlang distribution can go up and down (with TCP OOB)"}].

dist_up_down(Config) when is_list(Config) ->
    %% also check that "args" work with i/o redirection
    Name = lists:concat([?FUNCTION_NAME, "-", os:getpid()]),
    ct:capture_start(),
    {ok, Node} = peer:start_link(#{name => Name, connection => {{127, 0, 0, 1}, 0},
        args => ["-eval", "io:format(\"out\")."]}),
    ?assertEqual(true, net_kernel:connect_node(Node)),
    ?assertEqual(true, net_kernel:disconnect(Node)),
    ?assertEqual(true, net_kernel:connect_node(Node)),
    ct:sleep(1000), %% without this pause, peer node is not quick enough to process printing
    peer:stop(Node),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    ?assertEqual(["out"], Texts, {node, Node}).

%% -------------------------------------------------------------------
%% OOB cases

%% @private Attempts to forward a message from peer node to origin
%%      node via OOB connection. Useful mostly for testing.
-spec forward(Dest :: pid() | atom(), Message :: term()) -> term().
forward(Dest, Message) ->
    group_leader() ! {message, Dest, Message}.

basic() ->
    [{doc, "Tests peer node start, and do some RPC via stdin/stdout"}].

basic(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io, wait_boot => false}),
    ?assertEqual(booting, peer:get_state(Pid)),
    %% boot the node
    ?assertEqual(ok, peer:wait_boot(Pid, infinity)),
    %% test the default (std) channel
    ?assertEqual('nonode@nohost', peer:call(Pid, erlang, node, [])),
    ?assertException(throw, ball, peer:call(Pid, erlang, throw, [ball])),
    %% setup code path to this module (needed to "fancy RPC")
    Path = filename:dirname(code:which(?MODULE)),
    ?assertEqual(true, peer:call(Pid, code, add_path, [Path])),
    %% fancy RPC via message exchange (uses forwarding from the peer)
    Control = self(),
    RFun = fun() -> receive do -> forward(Control, done) end end,
    RemotePid = peer:call(Pid, erlang, spawn, [RFun]),
    peer:send(Pid, RemotePid, do),
    %% wait back from that process
    receive done -> ok end,
    %% shutdown the node
    ?assertEqual(ok, peer:stop(Pid)),
    ?assertNot(is_process_alive(Pid)).

detached() ->
    [{doc, "Tests detached node (RPC via OOB connection)"}].

detached(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => 0}),
    ?assertEqual('nonode@nohost', peer:call(Pid, erlang, node, [])),
    %% check exceptions
    ?assertException(throw, ball, peer:call(Pid, erlang, throw, [ball])),
    %% check tcp forwarding
    Path = filename:dirname(code:which(?MODULE)),
    ?assertEqual(true, peer:call(Pid, code, add_path, [Path])),
    %% fancy RPC via message exchange (uses forwarding from the peer)
    Control = self(),
    RFun = fun() -> receive do -> forward(Control, done) end end,
    RemotePid = peer:call(Pid, erlang, spawn, [RFun]),
    peer:send(Pid, RemotePid, do),
    %% wait back from that process
    receive done -> ok end,
    %% logging via TCP
    ct:capture_start(),
    peer:call(Pid, io, format, ["one."]),
    peer:call(Pid, erlang, apply, [io, format, ["two."]]),
    peer:stop(Pid),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    %% just stop
    ?assertEqual(["one.", "two."], Texts).

dyn_peer() ->
    [{doc, "Origin is not distributed, and peer becomes distributed dynamically"}].

dyn_peer(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io}),
    Node = list_to_atom(lists:concat([peer:random_name(), "@forced.host"])),
    {ok, _} = peer:call(Pid, net_kernel, start, [[Node, longnames]]),
    ?assertEqual(Node, peer:call(Pid, erlang, node, [])),
    peer:stop(Pid).

stop_peer() ->
    [{doc, "Test that peer shuts down even when node sleeps, but stdin is closed"}].

stop_peer(Config) when is_list(Config) ->
    {ok, Node} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME),
        connection => standard_io, args => ["-eval", "timer:sleep(60000)."]}),
    %% shutdown node
    peer:stop(Node).

init_debug() ->
    [{doc, "Test that debug messages in init work"}].

init_debug(Config) when is_list(Config) ->
    ct:capture_start(),
    {ok, Node} = peer:start_link(#{name => peer:random_name(?FUNCTION_NAME), shutdown => 1000,
        connection => standard_io, args => ["-init_debug"]}),
    ct:sleep(1000), %% without this sleep, peer is not fast enough to print
    peer:stop(Node),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    %% every boot script starts with this
    ?assertEqual("{progress,preloaded}\r\n", hd(Texts)).

io_redirect() ->
    [{doc, "Tests i/o redirection working for std"}].

io_redirect(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io}),
    ct:capture_start(),
    peer:call(Pid, io, format, ["test."]),
    peer:call(Pid, erlang, apply, [io, format, ["second."]]),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    peer:stop(Pid),
    ?assertEqual(["test.", "second."], Texts).

multi_node() ->
    [{doc, "Tests several nodes starting concurrently"}].

multi_node(Config) when is_list(Config) ->
    Nodes = [
        peer:start_link(#{name => peer:random_name(), wait_boot => false, connection => standard_io})
        || _ <- lists:seq(1, 4)],
    ok = peer:wait_boot([Node || {ok, Node} <- Nodes], 10000),
    [?assertEqual(Node, peer:call(Node, erlang, node, [])) || {ok, Node} <- Nodes],
    [?assertEqual(ok, peer:stop(Node)) || {ok, Node} <- Nodes].

duplicate_name() ->
    [{doc, "Tests that a node with the same name fails to start"}].

duplicate_name(Config) when is_list(Config) ->
    {ok, Pid1} = peer:start_link(#{connection => standard_io, name => ?FUNCTION_NAME, register => false}),
    ?assertException(exit, _, peer:start_link(#{connection => standard_io, name => ?FUNCTION_NAME})),
    peer:stop(Pid1).

%% -------------------------------------------------------------------
%% SSH/Docker cases

ssh() ->
    [{doc, "Tests ssh (localhost) node support"}].

ssh(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{remote => {rsh, "ssh", ["localhost"]}, connection => standard_io}),
    %% nonode at nohost, but controlled!
    ?assertEqual('nonode@nohost', peer:call(Pid, erlang, node, [])),
    peer:stop(Pid).

docker() ->
    [{doc, "Tests starting peer node in Docker container"}, {timetrap, {seconds, 60}}].

docker(Config) when is_list(Config) ->

    %% fancy hack: since it's know that project is run from CWD (otherwise rebar3 won't work),
    %%  use it as a root for release build (and Dockerfile)
    %ReleaseRoot = os:getenv("PWD"),
    %file:set_cwd(ReleaseRoot),
    %os:cmd("rebar3 as prod tar"),
    %os:cmd("docker build -t lambda ."),

    {ok, Node} = peer:start_link(#{name => docker, host => "container.id.au", longnames => true,
        remote => fun (Orig, Listen, Options) -> docker_start(Orig, Listen, Options, "localhost") end,
        connection => standard_io}),
    ?assertEqual(Node, peer:call(Node, erlang, node, [])),
    peer:stop(Node).

docker_start({Exec, Args}, _ListenPort, _Options, Host) ->
    %% stub: docker support requires slightly more involvement,
    %%  and ability to create a release programmatically
    {"ssh", [Host, Exec | Args]}.
