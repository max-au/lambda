%%% @doc
%%% Smoke tests for peer node controller
%%% @end
-module(peer_SUITE).
-author("maximfca@gmail.com").

%% Common Test headers
-include_lib("common_test/include/ct.hrl").
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
        {dist, [parallel], [dist, dist_io_redirect, dist_up_down]},
        {oob, [parallel], [basic, detached, dyn_peer, stop_peer, init_debug, io_redirect, multi_node, duplicate_name]},
        {remote, [parallel], [ssh, docker]}
    ].

all() ->
    [{group, dist}, {group, oob}, {group, remote}].

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
            {ok, Pid} = net_kernel:start([lists:concat(["dist_", os:getpid()]), LongShort]),
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
    [{doc, "Tests node that is detached, and has no OOB, connects via dist"}].

dist(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(),
    Node = peer:get_node(Pid),
    %% distribution is expected to be connected
    ?assertEqual(Node, rpc:call(Node, erlang, node, [])),
    %% but not oob...
    ?assertException(error, noconnection, peer:apply(Pid, erlang, node, [])),
    %% unlink and monitor
    unlink(Pid),
    MRef = monitor(process, Pid),
    ?assertEqual(ok, peer:disconnect(Pid, 5000)),
    %% ^^^ this makes the node go down
    receive
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    after 2000 ->
        link(Pid),
        {fail, disconnect_timeout}
    end.

dist_io_redirect() ->
    [{doc, "Tests i/o redirection working for dist"}].

dist_io_redirect(Config) when is_list(Config) ->
    Node = peer:random_name(),
    {ok, Pid} = peer:start_link(Node),
    ct:capture_start(),
    rpc:call(Node, io, format, ["test."]),
    ?assertEqual(ok, peer:send(Pid, init, {stop, stop})),
    rpc:call(Node, erlang, apply, [io, format, ["second."]]),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    peer:stop(Pid),
    ?assertEqual(["test.", "second."], Texts).

dist_up_down() ->
    [{doc, "Test that Erlang distribution can go up and down (with TCP OOB)"}].

dist_up_down(Config) when is_list(Config) ->
    %% also check that "args" work with i/o redirection
    Node = peer:random_name(),
    ct:capture_start(),
    {ok, Pid} = peer:start_link(#{node => Node, connection => {{127, 0, 0, 1}, 0},
            args => ["-eval", "io:format(\"out\")."]}),
    ?assertEqual(ok, peer:connect(Pid, 5000)),
    ?assertEqual(ok, peer:disconnect(Pid, 5000)),
    ?assertEqual(ok, peer:connect(Pid, 5000)),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    peer:stop(Pid),
    ?assertEqual(["out"], Texts).

%% -------------------------------------------------------------------
%% OOB cases

basic() ->
    [{doc, "Tests peer node start, and do some RPC via stdin/stdout"}].

basic(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io, wait_boot => false}),
    ?assertEqual(booting, peer:get_state(Pid)),
    ?assertEqual('nonode@nohost', peer:get_node(Pid)),
    %% boot the node
    ?assertEqual(ok, peer:wait_boot(Pid, infinity)),
    %% test the default (std) channel
    ?assertEqual('nonode@nohost', peer:apply(Pid, erlang, node, [])),
    ?assertException(throw, ball, peer:apply(Pid, erlang, throw, [ball])),
    %% setup code path to this module (needed to "fancy RPC")
    Path = filename:dirname(code:which(?MODULE)),
    ?assertEqual(true, peer:apply(Pid, code, add_path, [Path])),
    %% fancy RPC via message exchange (uses forwarding from the peer)
    Control = self(),
    RFun = fun() -> receive do -> peer:forward(Control, done) end end,
    RemotePid = peer:apply(Pid, erlang, spawn, [RFun]),
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
    ?assertEqual('nonode@nohost', peer:apply(Pid, erlang, node, [])),
    %% check exceptions
    ?assertException(throw, ball, peer:apply(Pid, erlang, throw, [ball])),
    %% check tcp forwarding
    Path = filename:dirname(code:which(?MODULE)),
    ?assertEqual(true, peer:apply(Pid, code, add_path, [Path])),
    %% fancy RPC via message exchange (uses forwarding from the peer)
    Control = self(),
    RFun = fun() -> receive do -> peer:forward(Control, done) end end,
    RemotePid = peer:apply(Pid, erlang, spawn, [RFun]),
    peer:send(Pid, RemotePid, do),
    %% wait back from that process
    receive done -> ok end,
    %% logging via TCP
    ct:capture_start(),
    peer:apply(Pid, io, format, ["one."]),
    peer:apply(Pid, erlang, apply, [io, format, ["two."]]),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    %% just stop
    peer:stop(Pid),
    ?assertEqual(["one.", "two."], Texts).

dyn_peer() ->
    [{doc, "Origin is not distributed, and peer becomes distributed dynamically"}].

dyn_peer(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io}),
    Node = peer:random_name(),
    {ok, _} = peer:apply(Pid, net_kernel, start, [[Node, longnames]]),
    ?assertEqual(Node, peer:apply(Pid, erlang, node, [])),
    peer:stop(Pid).

stop_peer() ->
    [{doc, "Test that peer shuts down even when node sleeps, but stdin is closed"}].

stop_peer(Config) when is_list(Config) ->
    Node = peer:random_name(false), %% piggybacking, shortnames
    {ok, Pid} = peer:start_link(#{node => Node,
        connection => standard_io, args => ["-eval", "timer:sleep(60000)."]}),
    %% shutdown node
    peer:stop(Pid).

init_debug() ->
    [{doc, "Test that debug messages in init work"}].

init_debug(Config) when is_list(Config) ->
    Node = peer:random_name(false),
    ct:capture_start(),
    {ok, Pid} = peer:start_link(#{node => Node,
        connection => standard_io, args => ["-init_debug"]}),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    %% every boot script starts with this
    ?assertEqual("{progress,preloaded}\r\n", hd(Texts)),
    %% shutdown node
    peer:stop(Pid).

io_redirect() ->
    [{doc, "Tests i/o redirection working for std"}].

io_redirect(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{connection => standard_io}),
    ct:capture_start(),
    peer:apply(Pid, io, format, ["test."]),
    peer:apply(Pid, erlang, apply, [io, format, ["second."]]),
    ct:capture_stop(),
    Texts = ct:capture_get(),
    peer:stop(Pid),
    ?assertEqual(["test.", "second."], Texts).

multi_node() ->
    [{doc, "Tests several nodes starting concurrently"}].

multi_node(Config) when is_list(Config) ->
    Pids = [begin
                N = peer:random_name(),
                {ok, Pid} = peer:start_link(#{node => N, wait_boot => false, connection => standard_io}),
                {N, Pid}
            end || _ <- lists:seq(1, 4)],
    ok = peer:wait_boot([P || {_, P} <- Pids], 10000),
    [?assertEqual(N, peer:apply(Pid, erlang, node, [])) || {N, Pid} <- Pids],
    [?assertEqual(ok, peer:stop(Pid)) || {_, Pid} <- Pids].

duplicate_name() ->
    [{doc, "Tests that a node with the same name fails to start"}].

duplicate_name(Config) when is_list(Config) ->
    {ok, Pid1} = peer:start_link(#{connection => standard_io, node => ?FUNCTION_NAME}),
    ?assertException(exit, _, peer:start_link(#{connection => standard_io, node => ?FUNCTION_NAME})),
    peer:stop(Pid1).

%% -------------------------------------------------------------------
%% SSH/Docker cases

ssh() ->
    [{doc, "Tests ssh (localhost) node support"}].

ssh(Config) when is_list(Config) ->
    {ok, Pid} = peer:start_link(#{remote => {ssh, "localhost"}, connection => standard_io}),
    %% nonode at nohost, but controlled!
    ?assertEqual('nonode@nohost', peer:apply(Pid, erlang, node, [])),
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

    Node = 'docker@container.id.au',
    {ok, Pid} = peer:start_link(#{node => Node, longnames => true,
        remote => fun (Orig, Listen, Options) -> docker_start(Orig, Listen, Options, "localhost") end,
        connection => standard_io}),
    ?assertEqual(Node, peer:apply(Pid, erlang, node, [])),
    peer:stop(Pid).

docker_start({Exec, Args}, _ListenPort, _Options, Host) ->
    %% stub: docker support requires slightly more involvement,
    %%  and ability to create a release programmatically
    {"ssh", [Host, Exec | Args]}.
