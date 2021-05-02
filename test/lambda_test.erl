%% @doc
%%     Collection of test primitives shared between multiple tests.
%% @end
-module(lambda_test).
-author("maximfca@gmail.com").

%% API
-export([
    sync/1,
    sync_via/2,
    create_release/1,
    create_release/4,
    logger_config/1,
    filter_module/2,
    start_node/2,
    start_node/5,
    start_node_link/5,
    start_nodes/4,
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
    peer:call(Peer, sys, get_state, [Pid]),
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
    peer:call(Peer, sys, replace_state, [Via, fun (S) -> (catch sys:get_state(Name)), S end]).

%% @doc Creates a boot script simulating OTP release. Recommended to use
%%      with start_node_link or start_nodes. Returns boot script location.
%%      Contains lambda and sasl apps.
-spec create_release(file:filename_all()) -> file:filename_all().
create_release(Priv) ->
    create_release(Priv, [], "lambda", "1.0.0").

%% @doc Creates a boot script simulating OTP release. Recommended to use
%%      with start_node_link or start_nodes. Returns boot script location.
%%      Always adds lambda and sasl apps.
-spec create_release(DestDir :: file:filename_all(), Apps :: [atom()],
    RelName :: string(), RelVsn :: string()) -> file:filename_all().
create_release(DestDir, RelApps, RelName, RelVsn) ->
    AllApps = [kernel, stdlib, compiler, lambda, sasl | RelApps],
    %% write release spec, *.rel file
    BaseDir = filename:join([DestDir, "releases", RelVsn]),
    Boot = filename:join(BaseDir, RelName),
    ok = filelib:ensure_dir(Boot),
    {Apps, Unload} = lists:foldl(
        fun (App, {AppVsn, Unl}) ->
            Unl2 =
                case application:load(App) of
                    ok ->
                        [App | Unl];
                    {error, {already_loaded, _}} ->
                        Unl
                end,
            {ok, Vsn} = application:get_key(App, vsn),
            {[{App, Vsn} | AppVsn], Unl2}
        end, {[], []}, AllApps),
    RelSpec = {release, {RelName, RelVsn}, {erts, erlang:system_info(version)}, Apps},
    AppSpec = io_lib:fwrite("~p. ", [RelSpec]),
    ok = file:write_file(Boot ++ ".rel", lists:flatten(AppSpec)),
    %% don't expect any warnings, fail otherwise
    {ok, systools_make, []} = systools:make_script(Boot, [silent, {outdir, BaseDir}, local]),
    %% unload apps we loaded
    [application:unload(App) || App <- Unload],
    Boot.

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
    Filters = if Modules =:= [] -> #{}; true -> #{filters => [{module, {fun ?MODULE:filter_module/2, Modules}}]} end,
    LogCfg = [
        {handler, default, logger_std_h,
            Filters#{config => #{type => standard_error}, formatter => Formatter}}],
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

-type test_id() :: integer() | string() | atom().

%% @doc Starts an extra node with specified bootstrap, no authority and default command line.
-spec start_node(file:filename_all(), lambda_broker:points()) -> node().
start_node(Boot, Bootstrap) ->
    start_node(undefined, Boot, Bootstrap, [], false).

%% @doc Starts an extra node with specified bootstrap, authority setting, and additional
%%      arguments in the command line.
-spec start_node(TestId :: test_id(), Boot :: file:filename_all(),
    [lambda_bootstrap:bootspec()], CmdLine :: [string()], boolean()) -> node().
start_node(TestId, Boot, Bootspec, CmdLine, Authority) ->
    start_impl(TestId, Boot, Bootspec, CmdLine, Authority, start).

%% @doc Starts an extra node with specified bootstrap, authority setting, and additional
%%      arguments in the command line.
-spec start_node_link(TestId :: test_id(), Boot :: file:filename_all(),
    [lambda_bootstrap:bootspec()], CmdLine :: [string()], boolean()) -> node().
start_node_link(TestId, Boot, Bootspec, CmdLine, Authority) ->
    start_impl(TestId, Boot, Bootspec, CmdLine, Authority, start_link).

start_impl(TestId, Boot, Bootspec, CmdLine, Authority, StartFun) ->
    %% in tests, use short names by default
    Name = random_name(TestId, Authority),
    TestCP = filename:dirname(code:which(?MODULE)),
    ExtraArgs = if
            Bootspec =/= undefined -> ["-lambda", "bootspec", lists:flatten(io_lib:format("~10000tp", [Bootspec]))];
            true -> []
        end,
    {ok, Node} = peer:StartFun(#{connection => standard_io, name => Name,
        args => [
            "-boot", Boot,
            "-setcookie", "lambda",
            "-connect_all", "false",
            "-epmd_module", "lambda_discovery",
            "-pa", TestCP] ++ ExtraArgs ++ CmdLine}),
    %% wait for connection?
    case Bootspec of
        {static, Map} when is_map(Map) ->
            {_, BootNodes} = lists:unzip(maps:keys(Map)),
            ok = peer:call(Node, ?MODULE, wait_connection, [BootNodes]);
        _ ->
            ok
    end,
    Node.

%% @doc Starts multiple lambda non-authority nodes concurrently.
-spec start_nodes(test_id(), file:filename_all(), [lambda_bootstrap:bootspec()], pos_integer()) -> [{peer:dest(), node()}].
start_nodes(TestId, Boot, BootSpec, Count) ->
    lambda_async:pmap([
        {?MODULE, start_node, [TestId, Boot, BootSpec, [], false]}
        || _ <- lists:seq(1, Count)]).

%% @doc
%% executed in the remote node: waits until node is connected to a list of other nodes passed
-spec wait_connection([node()]) -> ok |
    {error, #{reason := timeout | {nodeup, node()} | {nodedown, node()}, disconnected => [node()], connected => [node()]}}.
wait_connection(Nodes) ->
    %% subscribe to all nodeup events and wait...
    net_kernel:monitor_nodes(true),
    Connected = nodes(),
    ToConnect = Nodes -- Connected,
    ToDisconnect = Connected -- Nodes,
    Result = wait_nodes(ToConnect, ToDisconnect, 4500), %% keep barrier shorter than gen_server:call timeout
    net_kernel:monitor_nodes(false),
    Result.

wait_nodes([], [], _TimeLeft) ->
    ok;
wait_nodes(ToConnect, ToDisconnect, TimeLeft) ->
    StartedAt = erlang:system_time(millisecond),
    receive
        {nodeup, Node} ->
            case lists:member(Node, ToConnect) of
                true ->
                    DoneAt = erlang:system_time(millisecond),
                    wait_nodes(lists:delete(Node, ToConnect), ToDisconnect, TimeLeft - (DoneAt - StartedAt));
                false ->
                    {error, #{reason => {nodeup, Node}, disconnected => ToConnect, connected => ToDisconnect}}
            end;
        {nodedown, Node} ->
            case lists:member(Node, ToConnect) of
                true ->
                    DoneAt = erlang:system_time(millisecond),
                    wait_nodes(ToConnect, lists:delete(Node, ToDisconnect), TimeLeft - (DoneAt - StartedAt));
                false ->
                    {error, #{reason => {nodedown, Node}, disconnected => ToConnect, connected => ToDisconnect}}
            end
    after TimeLeft ->
        {error, #{reason => timeout, disconnected => ToConnect, connected => ToDisconnect}}
    end.

%% @private
%% Generates a sensible node name for easier tests debugging
random_name(TestId, Auth) ->
    %% horrible hack: store mapping of "test id" to "amount of nodes already started in this test"
    %% problem is, it has to be "shared state" somewhere, saved outside of any process state
    %% Use "ac_tab" which is... unprotected
    Seq = ets:update_counter(ac_tab, {?MODULE, TestId}, 1, {{?MODULE, TestId}, 0}),
    list_to_atom(lists:flatten(io_lib:format("~s~s-~b", [format_kind(Auth), format_test_id(TestId), Seq]))).

format_kind(true) -> "authority";
format_kind(false) -> "worker".

format_test_id(undefined) -> [];
format_test_id(Int) when is_integer(Int) -> [$- | integer_to_list(Int)];
format_test_id(Atom) when is_atom(Atom) -> [$- | atom_to_list(Atom)];
format_test_id(List) when is_list(List) -> [$- | List].
