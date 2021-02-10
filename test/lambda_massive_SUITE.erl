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

start_tier(Boot, Count, AuthorityCount, ServiceLocator) ->
    lambda_async:pmap([
        fun () ->
            Auth = Seq rem AuthorityCount =:= 1,
            Extras = if Auth -> ["-lambda", "authority", "true"]; true -> [] end,
            ExtraArgs = ["-lambda", "bootspec", "[{file,\"" ++ ServiceLocator ++ "\"}]" | Extras],
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

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    ServiceLocator = filename:join(Priv, "service.loc"),
    %% start a tier, with several authorities
    AuthCount = 2,
    AllPeers = start_tier(create_release(Priv), 4, AuthCount, ServiceLocator),
    AuthPeers = [A || {A, _, true} <- AllPeers],
    {Peers, Nodes, _AuthBit} = lists:unzip3(AllPeers),
    %% Find all broker pids on all nodes
    Brokers = lists:sort(lambda_async:pmap(
        [{peer, apply, [P, erlang, whereis, [lambda_broker]]} || P <- Peers])),
    %% force bootstrap: bootstrap -> authority/broker
    lambda_async:pmap([{peer, apply, [P, lambda_bootstrap, discover, []]} || P <- Peers]),
    %%
    [peer:apply(A, lambda_test, wait_connection, [Nodes]) || A <- AuthPeers],
    %%
    %% ask every single broker - "who are your authorities"
    BrokerKnownAuths = lambda_async:pmap([
        {peer, apply, [P, lambda_broker, authorities, [lambda_broker]]} || P <- Peers]),
    Views = lambda_async:pmap([{peer, apply, [P, lambda_authority, brokers, [lambda_authority]]}
        || P <- AuthPeers]),
    %% stop everything
    lambda_async:pmap([{peer, stop, [P]} || P <- Peers]),
    %% ensure both authorities are here
    [?assertEqual(AuthCount, length(A), {authorities, A}) || A <- BrokerKnownAuths],
    %% ensure they have the same view of the world, except for themselves
    [?assertEqual(Brokers, lists:sort(V), {view, V, Brokers}) || V <- Views].
