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
    start_node_link/1,
    start_node_link/3,
    start_nodes/2
]).

%% Internal exports to spawn remotely
-export([
    wait_connection/1
]).

%% @doc Flushes well-behaved (implementing OTP system behaviour)
%%      process queue.
-spec sync(sys:name() | [sys:name()]) -> ok.
sync([Pid]) ->
    sys:get_state(Pid),
    ok;
sync([Pid | More]) ->
    sys:get_state(Pid),
    sync(More);
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
sync_via(Via, Pid) ->
    sys:replace_state(Via, fun (S) -> (catch sys:get_state(Pid)), S end),
    ok.

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
    %% don't care about compiler
    lists:prefix(?MODULE_STRING, atom_to_list(node())) andalso net_kernel:stop(),
    ok.

%% @doc Starts an extra node with specified bootstrap, no authority and default command line.
-spec start_node_link(lambda_broker:points()) -> {peer:dest(), node()}.
start_node_link(Bootstrap) ->
    start_node_link(Bootstrap, [], false).

%% @doc Starts an extra node with specified bootstrap, authority setting, and additional
%%      arguments in the command line.
-spec start_node_link(lambda_broker:points(), CmdLine :: [string()], boolean()) -> {peer:dest(), node()}.
start_node_link(Bootstrap, CmdLine, Authority) ->
    Node = peer:random_name(),
    CP = code:lib_dir(lambda, ebin),
    TestCP = filename:dirname(code:which(?MODULE)),
    Auth = if Authority -> ["-lambda", "authority", "true"]; true -> [] end,
    Boot = if Bootstrap =/= undefined -> ["-lambda", "bootspec", lists:flatten(io_lib:format("[{static, ~10000tp}]", [Bootstrap]))]; true -> [] end,
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => [
            %% "-kernel", "dist_auto_connect", "never",
            "-start_epmd", "false",
            "-epmd_module", "lambda_discovery",
            %"-kernel", "logger", "[{handler, default, logger_std_h,#{config => #{type => standard_error}, formatter => {logger_formatter, #{ }}}}]",
            %"-kernel", "logger_level", "all",
            "-pa", CP, "-pa", TestCP] ++ Auth ++ Boot ++ CmdLine}),
    {ok, _Apps} = peer:apply(Peer, application, ensure_all_started, [lambda]),
    is_map(Bootstrap) andalso
        begin
            {_, BootNodes} = lists:unzip(maps:keys(Bootstrap)),
            ok = peer:apply(Peer, ?MODULE, wait_connection, [BootNodes])
        end,
    {Peer, Node}.

%% @doc Starts multiple lambda non-authority nodes concurrently.
-spec start_nodes(lambda_broker:points(), pos_integer()) -> [{peer:dest(), node()}].
start_nodes(Bootstrap, Count) ->
    lambda_async:pmap([
        fun () -> {Peer, Node} = start_node_link(Bootstrap, [], false), unlink(Peer), {Peer, Node} end
        || _ <- lists:seq(1, Count)]).

%% @private
%% executed in the remote node: waits until broker finds some
%%  authority
-spec wait_connection([node()]) -> ok.
wait_connection(Nodes) ->
    net_kernel:monitor_nodes(true),
    wait_nodes(Nodes),
    net_kernel:monitor_nodes(false).

wait_nodes([]) ->
    ok;
wait_nodes([Node | Nodes]) ->
    %% subscribe to all nodeup events and wait...
    case lists:member(Node, [node() | nodes()]) of
        true ->
            wait_nodes(lists:delete(Node, Nodes));
        false ->
            receive
                {nodeup, Node} ->
                    wait_nodes(lists:delete(Node, Nodes))
            end
    end.
