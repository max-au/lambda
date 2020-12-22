%% @doc
%%     Tests Lambda process registry
%% @end
-module(lambda_registry_SUITE).
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
    peer/0, peer/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, local}, {group, cluster}].

groups() ->
    [{local, [basic]}, {cluster, [peer]}].

%%--------------------------------------------------------------------
%% Convenience

start_node(Bootstrap, Authority) ->
    Node = peer:random_name(),
    CP = filename:dirname(code:which(lambda_registry)),
    Auth = if Authority -> ["-lambda", "authority", "true"]; true -> [] end,
    {ok, Peer} = peer:start_link(#{node => Node, connection => standard_io,
        args => [
            %% "-kernel", "dist_auto_connect", "never",
            "-start_epmd", "false",
            "-epmd_module", "lambda_epmd",
            "-pa", CP] ++ Auth}),
    Bootstrap =/= #{} andalso
        peer:apply(Peer, application, set_env, [lambda, bootstrap, Bootstrap, [{persistent, true}]]),
    {ok, _Cl} = peer:apply(Peer, application, ensure_all_started, [lambda]),
    {Peer, Node}.

%% Can execute function, function with args, and MFA.
-type async_fun() :: {module(), atom(), Args :: [term()]} | function() | {function(), [term()]}.

%% @doc schedules async execution of Fun, with default context named async.
-spec async(async_fun()) -> async.
async(Fun) ->
    async(async, Fun).

%% @doc schedules async execution to the executor Id (stored in process dictionary)
-spec async(Id, async_fun()) -> Id when Id :: term().
async(Id, Fun) ->
    case erlang:get(Id) of
        undefined ->
            Trapping = erlang:process_flag(trap_exit, true),
            erlang:put(Id, {async_impl(Fun, #{}), Trapping});
        {Running, Trap} ->
            erlang:put(Id, {async_impl(Fun, Running), Trap})
    end,
    Id.

%% @doc Completes async requests for default context.
-spec async_wait(Timeout) -> ok.
async_wait(Timeout) ->
    async_wait(async, Timeout).

%% @doc Completes all async requests given a context and timeout.
%%      Resets trap_exit to the state it was before, removed context from
%%      process dictionary.
%%      In case of a timeout, remaining processes are forcefully exited (killed).
-spec async_wait(Id :: term(), timeout()) -> #{term() => term()}.
async_wait(Id, Timeout) ->
    TRef = erlang:start_timer(Timeout, self(), timeout),
    {Running, Trapping} = erlang:get(Id),
    Replies = collect_replies(Running, [], TRef),
    Trapping orelse erlang:process_flag(trap_exit, false),
    Replies.

async_impl({M, F, A}, Map) ->
    Pid = erlang:spawn_link(M, F, A),
    Map#{Pid => []};
async_impl(Fun, Map) when is_function(Fun) ->
    Pid = erlang:spawn_link(Fun),
    Map#{Pid => []};
async_impl({Fun, Args}, Map) when is_function(Fun, length(Args)) ->
    Pid = erlang:spawn_link(Fun, Args),
    Map#{Pid => []}.

collect_replies(Pids, Replies, TRef) when Pids =:= #{} ->
    TRef =/= undefined andalso
        case erlang:cancel_timer(TRef) of
            false ->
                %% timer expired before we cancelled it, so
                %%  lets flush the message in the queue
                receive
                    {timeout, TRef, timeout} -> ok
                end;
            _ ->
                ok
        end,
    Replies;
collect_replies(Pids, Replies, TRef) ->
    receive
        {timeout, TRef, timeout} ->
            %% reached the timeout. kill all children forcefully.
            [exit(Pid, kill) || Pid <- maps:keys(Pids)],
            %% continue collecting...
            collect_replies(Pids, Replies, undefined);
        {'EXIT', Pid, normal} when is_map_key(Pid, Pids) ->
            collect_replies(maps:remove(Pid, Pids), Replies, TRef);
        {'EXIT', Pid, Reason} when is_map_key(Pid, Pids) ->
            collect_replies(maps:remove(Pid, Pids), Replies, TRef)
    end.

unwrap({ok, Pid}) ->
    Pid.

sync([Pid]) ->
    sys:get_state(Pid);
sync([Pid | More]) ->
    sys:get_state(Pid),
    sync(More);
sync(Pid) when is_pid(Pid) ->
    sys:get_state(Pid).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Tests authority discovery within a single Erlang node"}].

basic(Config) when is_list(Config) ->
    Addr = {epmd, "localhost"}, %% use fake address for non-distributed node
    %% start discovery
    {ok, Disco} = lambda_epmd:start_link(),
    ok = lambda_epmd:set_node(node(), Addr),
    %% start root (empty) authority
    {ok, AuthPid} = lambda_authority:start_link(#{}),
    Bootstrap = #{AuthPid => Addr},
    %% start a number of (unnamed) registries
    Registries = [unwrap(lambda_registry:start_link(Bootstrap)) || _ <- lists:seq(1, 8)],
    %% ensure all registries have processed everything. Twice.
    sync([AuthPid | Registries]),
    %% ensure authority has discovered all of them
    ?assertEqual(lists:sort(Registries), lists:sort(lambda_authority:registries())),
    [?assertEqual([AuthPid], lambda_registry:authorities(R)) || R <- Registries],
    %% unregister root authority (so we can start another one without name clash)
    erlang:unregister(lambda_authority),
    %% start second authority
    {ok, Auth2} = lambda_authority:start_link(Bootstrap),
    %% let authorities sync. White-box testing here, knowing the protocol.
    sync([AuthPid, Auth2, AuthPid, Auth2]),
    %% ensure both authorities have the same list of connected processes
    ?assertEqual(lists:sort(gen_server:call(AuthPid, registries)),
        lists:sort(lambda_authority:registries())),
    %% ensure authorities know each other
    ?assertEqual([Auth2], gen_server:call(AuthPid, authorities)),
    ?assertEqual([AuthPid], lambda_authority:authorities()),
    %% ensure registries know authorities too
    sync(Registries),
    Auths = lists:sort([AuthPid, Auth2]),
    [?assertEqual(Auths, lists:sort(lambda_registry:authorities(R))) || R <- Registries],
    %% all done, stop now
    [gen:stop(Pid) || Pid <- Registries ++ [AuthPid, Disco, Auth2]].


%%--------------------------------------------------------------------
%% Real Cluster Test Cases

peer() ->
    [{doc, "Tests basic cluster discovery with actual peer Erlang node"}].

peer(Config) when is_list(Config) ->
    %% do not use the test runner node for any logic, for
    %%  the sake of isolation and "leave no trace" idea
    %% start first authority node
    {AuthorityPeer, AuthorityNode} = start_node(#{}, true),
    %% form the bootstrap
    Addr = peer:apply(AuthorityPeer, lambda_epmd, get_node, [AuthorityNode]),
    Bootstrap = #{{lambda_authority, AuthorityNode} => Addr},
    %% start extra nodes
    Peers = [start_node(Bootstrap, false) || _ <- lists:seq(1, 4)],
    {_, ExpectedWorkers} = lists:unzip(Peers),
    %% ensure they all find the authority
    WorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], ExpectedWorkers -- WorkerNodes, "missing nodes"),
    ?assertEqual([], WorkerNodes -- ExpectedWorkers, "unexpected nodes"),
    %% start more nodes, don't give them authority addresses
    NonAuth = [start_node(Bootstrap, false) || _ <- lists:seq(1, 4)],
    {_, NonAuthWN} = lists:unzip(NonAuth),
    %% verify there are 8 nodes connected to this authority
    AllWorkerNodes = peer:apply(AuthorityPeer, erlang, nodes, []),
    ?assertEqual([], ExpectedWorkers ++ NonAuthWN -- AllWorkerNodes, "missing nodes"),
    ?assertEqual([], AllWorkerNodes -- ExpectedWorkers -- NonAuthWN, "unexpected nodes"),
    %% start a second authority
    {SecondAuthPeer, SecondAuthNode} = start_node(Bootstrap, true),
    %% verify both authorities have 9 connected nodes (8 non-authority)
    AllNodes = peer:apply(AuthorityPeer, erlang, nodes, []) ++ [AuthorityNode],
    AllNodes2 = peer:apply(SecondAuthPeer, erlang, nodes, []) ++ [SecondAuthNode],
    ?assertEqual(lists:sort(AllNodes), lists:sort(AllNodes2)),
    ?assertEqual(length(AllNodes), 9),
    %% shut all down
    [peer:stop(P) || {P, _} <- Peers ++ NonAuth],
    [peer:stop(P) || P <- [AuthorityPeer, SecondAuthPeer]],
    ok.
