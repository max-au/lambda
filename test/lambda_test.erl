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
    start_node/2,
    start_nodes/1
]).

%% Internal exports to spawn remotely
-export([
    wait_connection/1
]).

%% @doc Flushes well-behaved (implementing OTP system behaviour)
%%      process queue.
sync([Pid]) ->
    sys:get_state(Pid);
sync([Pid | More]) ->
    sys:get_state(Pid),
    sync(More);
sync(Pid) when is_pid(Pid) ->
    sys:get_state(Pid).

%% @doc Flushes OTP-behaving process queue from the point of
%%      view of another process. Needed to avoid race condition
%%      when process A needs to trigger some action in process B,
%%      which in turn expects process C to do something.
%%      This function exercises Erlang guarantees of message delivery
%%      done in the same order as messages are sent (and therefore
%%      will break is messages don't arrive in this order)
sync_via(Pid, Pid) ->
    error(cycle);
sync_via(Via, Pid) ->
    sys:replace_state(Via, fun (S) -> sys:get_state(Pid), S end).

%% @doc Start lambda application locally (in this BEAM). Authority is local,
%%      epmd replaced.
start_local() ->
    _Physical = lambda_epmd:replace(),
    %% need to be alive
    Config1 =
        case erlang:is_alive() of
            true ->
                Config;
            false ->
                LongShort = case inet_db:res_option(domain) of
                                [] -> shortnames;
                                Domain when is_list(Domain) -> longnames
                            end,
                {ok, Pid} = net_kernel:start([master, LongShort]),
                [{net_kernel, Pid} | Config]
        end,
    %% start a number of processes linked, in a separate process that survives the entire test suite
    Saviour = proc_lib:start(?MODULE, saviour, []),
    [{level, maps:get(level, logger:get_primary_config())}, {saviour, Saviour}  | Config1].
    % logger:set_primary_config(level, all)

end_local() ->
    {ok, Saviour} = proplists:get_value(saviour, Config),
    try
        MRef = erlang:monitor(process, Saviour),
        Saviour ! stop,
        receive
            {'DOWN', MRef, process, Saviour, _} ->
                ok
        end
    catch
        error:badarg ->
            ok
    end,
    Config1 = proplists:delete(saviour, Config),
    Config2 =
        case proplists:get_value(net_kernel, Config) of
            undefined ->
                Config1;
            Pid when is_pid(Pid) ->
                net_kernel:stop(),
                proplists:delete(net_kernel, Config1)
        end,
    lambda_epmd:restore(),
    logger:set_primary_config(level, ?config(level, Config2)),
    proplists:delete(level, Config2).

o() ->
    Addr = {epmd, "unused"}, %% use fake address for non-distributed node
    %% start discovery
    {ok, Disco} = lambda_epmd:start_link(),
    ok = lambda_epmd:set_node(node(), Addr),
    unlink(Disco),
    [{disco, Disco} | Config].

%% @doc Starts an extra node with specified bootstrap.
%%      Extra node may run authority if requested.
start_node(Bootstrap, Authority) ->
    Node = peer:random_name(),
    CP = filename:dirname(code:which(lambda_registry)),
    TestCP = filename:dirname(code:which(?MODULE)),
    Auth = if Authority -> ["-lambda", "authority", "true"]; true -> [] end,
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => [
            %% "-kernel", "dist_auto_connect", "never",
            "-start_epmd", "false",
            "-epmd_module", "lambda_epmd",
            %"-kernel", "logger", "[{handler, default, logger_std_h,#{config => #{type => standard_error}, formatter => {logger_formatter, #{ }}}}]",
            %"-kernel", "logger_level", "all",
            "-pa", CP, "-pa", TestCP] ++ Auth}),
    Bootstrap =/= #{} andalso
        peer:apply(Peer, application, set_env, [lambda, bootstrap, Bootstrap, [{persistent, true}]]),
    {ok, _Apps} = peer:apply(Peer, application, ensure_all_started, [lambda]),
    Bootstrap =/= #{} andalso
        begin
            {_, BootNodes} = lists:unzip(maps:keys(Bootstrap)),
            ok = peer:apply(Peer, ?MODULE, wait_connection, [BootNodes])
        end,
    unlink(Peer),
    {Peer, Node}.

%% @doc Starts multiple lambda nodes concurrently

%% @private
%% executed in the remote node: waits until registry finds some
%%  authority
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
