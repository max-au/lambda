%% @doc
%%     Collection of test primitives shared between multiple tests.
%% @end
-module(lambda_test).
-author("maximfca@gmail.com").

%% API
-export([
    sync/1,
    sync_via/2,
    start_local/0,
    end_local/0,
    create_release/1,
    logger_config/1,
    filter_module/2,
    start_node_link/2,
    start_node_link/4,
    start_nodes/3,
    wait_connection/1
]).

-type sync_target() :: sys:name() | {lambda:dst(), sys:name()}.

%% @doc Flushes well-behaved (implementing OTP system behaviour)
%%      process queue. When a list of processes is supplied, acts
%%      sequentially.
-spec sync(sync_target() | [sync_target()]) -> ok.
sync(List) when is_list(List) ->
    [sync(El) || El <- List],
    ok;
sync({Peer, Pid}) when is_pid(Pid) ->
    peer:apply(Peer, sys, get_state, [Pid]),
    ok;
sync(Pid) when is_pid(Pid) ->
    sys:get_state(Pid),
    ok.

%% @doc Flushes OTP-behaving process queue from the point of
%%      view of another process. Needed to avoid race condition
%%      when process A needs to trigger some action in process B,
%%      which in turn expects process C to do something.
%%      This function exercises Erlang guarantees of message delivery
%%      done in the same order as messages are sent (and therefore
%%      will break is messages don't arrive in this order)
-spec sync_via(sys:name(), sys:name()) -> ok.
sync_via(Pid, Pid) ->
    error(cycle);
sync_via(Via, Pid) when is_pid(Via) ->
    sys:replace_state(Via, fun (S) -> (catch sys:get_state(Pid)), S end),
    ok;
sync_via({_Peer, Name}, Name) ->
    error(cycle);
sync_via({Peer, Via}, Name) ->
    peer:apply(Peer, sys, replace_state, [Via, fun (S) -> (catch sys:get_state(Name)), S end]).

%% @doc Start lambda application locally (in this VM). Authority is local,
%%      epmd replaced. Node is made distributed
-spec start_local() -> ok.
start_local() ->
    %% need to be alive. Using shortnames - this way it works
    %%  even when there is no network connection
    erlang:is_alive() orelse net_kernel:start([list_to_atom(lists:concat([?MODULE, "_", os:getpid()])), shortnames]),
    %% configuration: local authority
    _ = application:load(lambda),
    %% "self-signed": node runs both authority and a broker
    ok = application:set_env(lambda, authority, true),
    %% lambda application. it does not have any dependencies other than compiler,
    %%  so should "just start".
    _ = application:start(compiler),
    %% Note: this is deliberate decision, don't just change to "ensure_all_started".
    ok = application:start(lambda).

%% @doc Ends locally running lambda, restores epmd, unloads lambda app,
%%      makes node non-distributed if it was allegedly started by us.
-spec end_local() -> ok.
end_local() ->
    ok = application:stop(lambda),
    ok = application:unload(lambda),
    %% don't care about compiler app left running
    lists:prefix(?MODULE_STRING, atom_to_list(node())) andalso net_kernel:stop(),
    ok.

%% @doc Creates a boot script simulating OTP release. Recommended to use
%%      with start_node_link or start_nodes. Returns boot script location.
-spec create_release(file:filename_all()) -> file:filename_all().
create_release(Priv) ->
    _ = application:load(lambda),
    _ = application:load(sasl),
    %% write release spec, *.rel file
    Base = filename:join(Priv, "lambda"),
    Apps = [begin {ok, Vsn} = application:get_key(App, vsn), {App, Vsn} end ||
        App <- [kernel, stdlib, compiler, sasl, lambda]],
    RelSpec = {release, {"lambda", "1.0.0"}, {erts, erlang:system_info(version)}, Apps},
    AppSpec = io_lib:fwrite("~p. ", [RelSpec]),
    ok = file:write_file(Base ++ ".rel", lists:flatten(AppSpec)),
    %% don't expect any warnings, fail otherwise
    {ok, systools_make, []} = systools:make_script(Base, [silent, {outdir, Priv}, local]),
    Base.

%% @doc Helper to create command line that redirects specific logger events to stderr
%%      Convenient for debugging: add this to start_node_link command line.
%%      When non-empty list of modules provided, only these modules are allowed in the output.
%%      By default, only lambda domain messages are printed
-spec logger_config([module()]) -> [string()].
logger_config(Modules) ->
    Formatter = {logger_formatter,
        #{legacy_header => false, single_line => true,
            template => [time, " ", node, " ", pid, " ", mfa, ":", line, " ", msg, "\n"]}
    },
    Filters = [{module, {fun ?MODULE:filter_module/2, Modules}}],
    LogCfg = [
        {handler, default, logger_std_h,
            #{config => #{type => standard_error}, filters => Filters, formatter => Formatter}}],
    ["-kernel", "logger", lists:flatten(io_lib:format("~10000tp", [LogCfg])), "-kernel", "logger_level", "all"].

%% @doc Filter passing events only from a list of allowed modules, and only for lambda domain
-spec filter_module(LogEvent, Modules) -> logger:filter_return() when
    LogEvent :: logger:log_event(),
    Modules :: [module()].
filter_module(#{meta := Meta} = LogEvent, Modules) when is_list(Modules) ->
    case lists:member(lambda, maps:get(domain, Meta, [])) andalso
        (Modules =:= [] orelse lists:member(element(1, maps:get(mfa, Meta, {[]})), Modules)) of
        true ->
            LogEvent#{meta => Meta#{node => node()}};
        false ->
            stop
    end.

%% @doc Starts an extra node with specified bootstrap, no authority and default command line.
-spec start_node_link(file:filename_all(), lambda_broker:points()) -> {peer:dest(), node()}.
start_node_link(Boot, Bootstrap) ->
    start_node_link(Boot, Bootstrap, [], false).

%% @doc Starts an extra node with specified bootstrap, authority setting, and additional
%%      arguments in the command line.
-spec start_node_link(file:filename_all(), [lambda_bootstrap:bootspec()], CmdLine :: [string()], boolean()) -> {peer:dest(), node()}.
start_node_link(Boot, Bootspec, CmdLine, Authority) ->
    Node = peer:random_name(),
    TestCP = filename:dirname(code:which(?MODULE)),
    Auth = if Authority -> ["-lambda", "authority", "true"]; true -> [] end,
    ExtraArgs = if Bootspec =/= undefined -> ["-lambda", "bootspec", lists:flatten(io_lib:format("~10000tp", [Bootspec]))]; true -> [] end,
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => [
            "-boot", Boot,
            "-connect_all", "false",
            "-start_epmd", "false",
            "-epmd_module", "lambda_discovery",
            "-pa", TestCP] ++ Auth ++ ExtraArgs ++ CmdLine}),
    %% wait for connection?
    case Bootspec of
        [{static, Map}] when is_map(Map) ->
            {_, BootNodes} = lists:unzip(maps:keys(Map)),
            ok = peer:apply(Peer, ?MODULE, wait_connection, [BootNodes]);
        _ ->
            ok
    end,
    {Peer, Node}.

%% @doc Starts multiple lambda non-authority nodes concurrently.
-spec start_nodes(file:filename_all(), [lambda_bootstrap:bootspec()], pos_integer()) -> [{peer:dest(), node()}].
start_nodes(Boot, BootSpec, Count) ->
    lambda_async:pmap([
        fun () -> {Peer, Node} = start_node_link(Boot, BootSpec, [], false), unlink(Peer), {Peer, Node} end
        || _ <- lists:seq(1, Count)]).

%% @doc
%% executed in the remote node: waits until node is connected to a list of other nodes passed
-spec wait_connection([node()]) -> ok | {error, [node()]}.
wait_connection(Nodes) ->
    net_kernel:monitor_nodes(true),
    wait_nodes(Nodes, 4000), %% keep barrier shorter than gen_server:call timeout
    net_kernel:monitor_nodes(false).

wait_nodes([], _Barrier) ->
    ok;
wait_nodes([Node | Nodes], Barrier) ->
    %% subscribe to all nodeup events and wait...
    case lists:member(Node, [node() | nodes()]) of
        true ->
            wait_nodes(lists:delete(Node, Nodes), Barrier);
        false ->
            receive
                {nodeup, Node1} ->
                    wait_nodes(lists:delete(Node1, Nodes), Barrier)
            after Barrier ->
                {error, [Node | Nodes]}
            end
    end.
