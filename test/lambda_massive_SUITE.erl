%% @doc
%%     Massive test: run dozens and hundreds of Lambda nodes.
%% @end
-module(lambda_massive_SUITE).
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

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 15}}].

init_per_suite(Config) ->
    _ = application:load(lambda),
    _ = application:load(sasl),
    Config.

end_per_suite(Config) ->
    Config.

all() ->
    [basic].

%%--------------------------------------------------------------------
%% Convenience & data

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

start_tier(Boot, Count, AuthorityCount, ServiceLocator) when Count >= AuthorityCount ->
    %% start the nodes concurrently
    lambda_async:pmap([
        fun () ->
            Auth = Seq =< AuthorityCount,
            CommonArgs = ["+S", "2:2", "-connect_all", "false"],
            Extras = if Auth -> ["-lambda", "authority", "true"]; true -> [] end,
            ExtraArgs = ["-lambda", "bootspec", "[{file,\"" ++ ServiceLocator ++ "\"}]" | Extras] ++ CommonArgs,
            %% Node = peer:random_name(),
            {ok, Host} = inet:gethostname(),
            Node = list_to_atom(lists:flatten(
                io_lib:format("~s-~b@~s", [if Auth -> "authority"; true -> "lambda" end, Seq, Host]))),
            {ok, Peer} = peer:start_link(#{node => Node, longnames => false,
                args => ["-boot", Boot | ExtraArgs], connection => standard_io}),
            %% add code path to the test directory for helper functions in lambda_test
            true = peer:apply(Peer, code, add_path, [filename:dirname(code:which(?MODULE))]),
            unlink(Peer),
            {Peer, Node, Auth}
        end
        || Seq <- lists:seq(1, Count)]).

%psync(Peers, Name) ->
%    lambda_async:pmap([{peer, apply, [P, sys, get_state, [Name]]} || P <- Peers]).
%psync(Peers, Via, Name) ->
%    lambda_async:pmap([{peer, apply, [P, sys, replace_state,
%        [Via, fun (S) -> (catch sys:get_state(Name)), S end]]} || P <- Peers]).

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
    lambda_async:pmap([{peer, stop, [P]} || P <- Peers]),
    %% ensure both authorities are here
    [?assert(is_list(A), {authorities, P, A}) || {P, A} <- lists:zip(Nodes, BrokerKnownAuths)],
    [?assertEqual(AuthCount, length(A), {authorities, A}) || A <- BrokerKnownAuths],
    %% ensure they have the same view of the world, except for themselves
    [?assertEqual(Brokers, lists:sort(V), {view, V, Brokers}) || V <- Views].

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"},
        {timetrap, {seconds, 600}}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = create_release(Priv),
    %% run many combinations
    [
        begin
            ct:pal("Running ~b nodes with ~b authorities~n", [Total, Auth]),
            ServiceLocator = filename:join(Priv, "service.loc"),
            file:delete(ServiceLocator),
            verify_topo(Boot, ServiceLocator, Total, Auth)
        end || Total <- [2, 4, 10, 32, 100], Auth <- [1, 2, 3, 6], Total >= Auth].
