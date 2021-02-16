%% @doc
%%     Tests Lambda authority Gossip, mesh, and non-mesh brokers.
%% @end
-module(lambda_authority_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    proper/0, proper/1,
    reconnect/0, reconnect/1
]).

%% PropEr stateful testing exports
-export([
    initial_state/0,
    precondition/2,
    postcondition/3,
    command/1,
    next_state/3,
    %%
    start_authority/2,
    start_broker/2,
    disconnect/2
]).

-behaviour(proper_statem).

-include_lib("stdlib/include/assert.hrl").
-include_lib("proper/include/proper.hrl").

init_per_suite(Config) ->
    _ = application:load(lambda),
    _ = application:load(sasl),
    Priv = proplists:get_value(priv_dir, Config),
    Boot = create_release(Priv),
    [{boot, Boot} | Config].

end_per_suite(Config) ->
    Config.

all() ->
    [basic, reconnect, proper].

%%--------------------------------------------------------------------
%% Convenience & data

%% -define (SIMPLE_NODE_NAMES, true).
-ifdef (SIMPLE_NODE_NAMES).
node_name(Seq, Auth) ->
    {ok, Host} = inet:gethostname(),
    Node = list_to_atom(lists:flatten(
        io_lib:format("~s-~b@~s", [if Auth -> "authority"; true -> "lambda" end, Seq, Host]))),
    #{node => Node, longnames => false}.
-else.
node_name(_Seq, _Auth) ->
    #{node => peer:random_name()}.
-endif.

create_release(Priv) ->
    %% write release spec, *.rel file
    Base = filename:join(Priv, "lambda"),
    Apps = [begin {ok, Vsn} = application:get_key(App, vsn), {App, Vsn} end ||
        App <- [kernel, stdlib, compiler, sasl, lambda]],
    RelSpec = {release, {"massive", "1.0.0"}, {erts, erlang:system_info(version)}, Apps},
    AppSpec = io_lib:fwrite("~p. ", [RelSpec]),
    ok = file:write_file(Base ++ ".rel", lists:flatten(AppSpec)),
    %% don't expect any warnings, fail otherwise
    {ok, systools_make, []} = systools:make_script(Base, [silent, {outdir, Priv}, local]),
    Base.

start_node(Seq, Boot, ServiceLocator, Auth) ->
    CommonArgs = ["+S", "2:2", "-connect_all", "false", "-epmd_module", "lambda_discovery"],
    Extras = if Auth -> ["-lambda", "authority", "true"]; true -> [] end,
    ExtraArgs = ["-lambda", "bootspec", "[{file,\"" ++ ServiceLocator ++ "\"}]" | Extras] ++ CommonArgs,
    InitArgs = node_name(Seq, Auth),
    {ok, Peer} = peer:start_link(InitArgs#{
        args => ["-boot", Boot | ExtraArgs], connection => standard_io}),
    %% add code path to the test directory for helper functions in lambda_test
    true = peer:apply(Peer, code, add_path, [filename:dirname(code:which(?MODULE))]),
    unlink(Peer),
    {Peer, maps:get(node, InitArgs)}.

start_tier(Boot, Count, AuthorityCount, ServiceLocator) when Count >= AuthorityCount ->
    %% start the nodes concurrently
    lambda_async:pmap([
        fun () ->
            Auth = Seq =< AuthorityCount,
            {Peer, Node} = start_node(Seq, Boot, ServiceLocator, Auth),
            {Peer, Node, Auth}
        end
        || Seq <- lists:seq(1, Count)]).

stop_tier(Peers) ->
    lambda_async:pmap([{peer, stop, [P]} || {P, _, _} <- Peers]).

verify_topo(Boot, ServiceLocator, TotalCount, AuthCount) ->
    %% start a tier, with several authorities
    AllPeers = start_tier(Boot, TotalCount, AuthCount, ServiceLocator),
    %% Peers running Authority
    AuthPeers = [A || {A, _, true} <- AllPeers],
    {Peers, Nodes, _AuthBit} = lists:unzip3(AllPeers),
    %% force bootstrap: bootstrap -> authority/broker
    lambda_async:pmap([{peer, apply, [P, lambda_bootstrap, discover, []]} || P <- Peers]),
    %% authorities must connect to all nodes - that's a mesh!
    lambda_async:pmap([{peer, apply, [A, lambda_test, wait_connection, [Nodes]]} || A <- AuthPeers]),
    %%
    ct:pal("Authorities connected (~b/~b)", [AuthCount, TotalCount]),
    %% Find all broker pids on all nodes
    Brokers = lists:sort(lambda_async:pmap(
        [{peer, apply, [P, erlang, whereis, [lambda_broker]]} || P <- Peers])),
    %% flush broker -authority -broker queues
    lambda_async:pmap([{peer, apply, [P, sys, get_state, [lambda_broker]]} || P <- Peers]),
    lambda_async:pmap([{peer, apply, [A, sys, get_state, [lambda_authority]]} || A <- AuthPeers]),
    lambda_async:pmap([{peer, apply, [P, sys, get_state, [lambda_broker]]} || P <- Peers]),
    %% ask every single broker - "who are your authorities"
    BrokerKnownAuths = lambda_async:pmap([
        {peer, apply, [P, lambda_broker, authorities, [lambda_broker]]} || P <- Peers]),
    Views = lambda_async:pmap([{peer, apply, [P, lambda_authority, brokers, [lambda_authority]]}
        || P <- AuthPeers]),
    %% stop everything
    stop_tier(AllPeers),
    %% ensure both authorities are here
    [?assertEqual(AuthCount, length(A), {authorities, A}) || A <- BrokerKnownAuths],
    %% ensure they have the same view of the world, except for themselves
    [?assertEqual(Brokers, lists:sort(V), {view, V, Brokers}) || V <- Views].

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"},
        {timetrap, {seconds, 180}}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = proplists:get_value(boot, Config),
    %% run many combinations
    [
        begin
            ct:pal("Running ~b nodes with ~b authorities~n", [Total, Auth]),
            ServiceLocator = filename:join(Priv,
                lists:flatten(io_lib:format("service-~s~b_~b.loc", [?FUNCTION_NAME, Total, Auth]))),
            verify_topo(Boot, ServiceLocator, Total, Auth)
        end || Total <- [2, 4, 10, 32, 100], Auth <- [1, 2, 3, 6], Total >= Auth].

reconnect() ->
    [{doc, "Ensure that broker reconnects to authorities"}].

reconnect(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = proplists:get_value(boot, Config),
    ServiceLocator = filename:join(Priv, "reconnect-service.loc"),
    %% start a tier, with several authorities
    AllPeers = start_tier(Boot, 4, 2, ServiceLocator),
    {Peers, [A1, A2 | _], _} = lists:unzip3(AllPeers),
    Victim = lists:nth(4, Peers),
    Auths = lists:sort([A1, A2]),
    %% wait for expected connections
    ct:pal("Attempting first connection"),
    ?assertEqual(ok, peer:apply(Victim, lambda_bootstrap, discover, [])),
    ?assertEqual(ok, peer:apply(Victim, lambda_test, wait_connection, [Auths])),
    ct:pal("First connection successful~n"),
    %% force disconnect last node from first 2
    ?assertEqual(Auths, lists:sort(peer:apply(Victim, erlang, nodes, []))),
    true = peer:apply(Victim, net_kernel, disconnect, [A1]),
    true = peer:apply(Victim, net_kernel, disconnect, [A2]),
    %% verify it has been disconnected
    [] = peer:apply(Victim, erlang, nodes, []),
    %% force bootstrap
    ?assertEqual(ok, peer:apply(Victim, lambda_bootstrap, discover, [])),
    %% must be connected again
    ct:pal("Moment of truth~n"),
    ?assertEqual(ok, peer:apply(Victim, lambda_test, wait_connection, [Auths])),
    stop_tier(AllPeers).

proper() ->
    [{doc, "Property-based test verifying topology, mesh-connected for authorities, and none for lambdas"}].

proper(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = create_release(Priv),
    case proper:quickcheck(prop_self_healing(Boot, Priv),
        [{to_file, user}, {numtests, 10}, {max_size, 50}, {start_size, 10}, long_result]) of
        true ->
            ok;
        {error, Err} ->
            {fail, Err}
    end.

prop_self_healing(Boot, SL) ->
    proper:forall(proper_statem:commands(?MODULE, initial_state(Boot, SL)),
        fun (Cmds) ->
            {_History, State, Result} = proper_statem:run_commands(?MODULE, Cmds),
            %%%% cleanup
            io:format(standard_io, "H: ~200p~n", [_History]),
            cleanup(State),
            Result =:= ok
        end
    ).

start_authority(Boot, ServiceLocator) ->
    start_node(undefined, Boot, ServiceLocator, true).

start_broker(Boot, ServiceLocator) ->
    start_node(undefined, Boot, ServiceLocator, false).

disconnect(_Peer1, _Peer2) ->
    ok.

%% Limits
-define (MAX_AUTHORITY, 8).
-define (MAX_BROKERS, 16).

-record(cluster_state, {
    boot :: file:filename_all(),
    service_locator :: file:filename_all(),
    auth = #{} :: #{lambda:dst() => node()},
    brokers = #{} :: #{lambda:dst() => node()}
}).

cleanup(#cluster_state{auth = Auth, brokers = Brokers}) ->
    lambda_async:pmap([{peer, stop, [Pid]} || Pid <- maps:keys(Brokers) ++ maps:keys(Auth)]).

initial_state(Boot, Priv) ->
    Filename = lists:flatten(io_lib:format("srv-~b.loc", [erlang:system_time(millisecond)])),
    #cluster_state{boot = Boot, service_locator = filename:join(Priv, Filename)}.

initial_state() ->
    #cluster_state{}.

precondition(#cluster_state{auth = Auth}, {call, ?MODULE, start_broker, [_, _]}) ->
    map_size(Auth) < ?MAX_AUTHORITY;

precondition(#cluster_state{brokers = Brokers}, {call, ?MODULE, start_authority, [_, _]}) ->
    map_size(Brokers) < ?MAX_BROKERS;

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, peer, stop, [Pid]}) ->
    is_map_key(Pid, Auth) orelse is_map_key(Pid, Brokers);

precondition(#cluster_state{auth = Auth, brokers = Brokers}, {call, ?MODULE, disconnect, [Peer, _Node]}) ->
    is_map_key(Peer, Auth) orelse is_map_key(Peer, Brokers).

postcondition(_State, _Cmd, _Res) ->
    %% io:format(user, "~200p => ~200p~n~200p~n", [_Cmd, _Res, _State]),
    true.

%% Events possible:
%%  * start/stop an authority/broker
%%  * net splits/recovers
%% * topology verification
command(#cluster_state{auth = Auth, brokers = Brokers, boot = Boot, service_locator = SL}) ->
    %% nodes starting (authority/broker)
    AuthorityStart = {1, {call, ?MODULE, start_authority, [Boot, SL]}},
    BrokerStart = {5, {call, ?MODULE, start_broker, [Boot, SL]}},
    CheckState = {3, {?MODULE, check_state, []}},
    %% stop: anything that is running can be stopped at will
    Stop =
        case maps:merge(Auth, Brokers) of
            Empty when Empty =:= #{} ->
                [];
            Nodes ->
                Peers = maps:keys(Nodes),
                NodeNames = maps:values(Nodes),
                [
                    {5, {call, peer, stop, [proper_types:oneof(Peers)]}},
                    {5, {call, ?MODULE, disconnect, [proper_types:oneof(Peers), proper_types:oneof(NodeNames)]}}
                ]
        end,
    %% make your choice, Mr PropEr
    Choices = [AuthorityStart, BrokerStart, CheckState | Stop],
    proper_types:frequency(Choices).

next_state(#cluster_state{auth = Auth} = State, Res, {call, ?MODULE, start_authority, [_, _]}) ->
    NewAuth = {call, erlang, element, [1, Res]},
    NewNode = {call, erlang, element, [2, Res]},
    State#cluster_state{auth = Auth#{NewAuth => NewNode}};

next_state(#cluster_state{brokers = Brokers} = State, Res, {call, ?MODULE, start_broker, [_, _]}) ->
    NewBroker = {call, erlang, element, [1, Res]},
    NewNode = {call, erlang, element, [2, Res]},
    State#cluster_state{brokers = Brokers#{NewBroker => NewNode}};

next_state(State, _Res, {call, ?MODULE, disconnect, [_Peer1, _Peer2]}) ->
    State;

next_state(#cluster_state{auth = Auth, brokers = Brokers} = State, _Res, {call, peer, stop, [Pid]}) ->
    %% just remove Pid from all processes known
    State#cluster_state{auth = maps:remove(Pid, Auth), brokers = maps:remove(Pid, Brokers)}.
