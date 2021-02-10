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
            Node = list_to_atom(lists:flatten(io_lib:format("~s-~b", [if Auth -> "authority"; true -> "lambda" end, Seq]))),
            {ok, Peer} = peer:start_link(#{node => Node,
                args => ["-boot", Boot | ExtraArgs], connection => standard_io}),
            unlink(Peer),
            {Peer, Node, Auth}
        end
        || Seq <- lists:seq(1, Count)]).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    ServiceLocator = filename:join(Priv, "service.loc"),
    %% start a tier, with several authorities
    AuthCount = 2,
    {Peers, _Nodes, AuthBit} = lists:unzip3(start_tier(create_release(Priv), 4, AuthCount, ServiceLocator)),
    %% force bootstrap
    [peer:apply(P, lambda_bootstrap, discover, []) || P <- Peers],
    %% whitebox: flush bootstrap, authority, broker of all peers
    [peer:apply(P, sys, get_state, [Proc]) || Proc <- [lambda_bootstrap, lambda_broker], P <- Peers],
    %%
    %% ask every single broker - "who are your authorities"
    Authorities = [peer:apply(P, lambda_broker, authorities, [lambda_broker]) || P <- Peers],
    Brokers = lists:sort([peer:apply(P, erlang, whereis, [lambda_broker]) || P <- Peers]),
    Views = [lists:sort(peer:apply(P, lambda_authority, brokers, [lambda_authority]))
        || {P, true} <- lists:zip(Peers, AuthBit)], %% query authorities, - which brokers they know?
    %% stop everything
    lambda_async:pmap([{peer, stop, [P]} || P <- Peers]),
    %% ensure both authorities are here
    [?assertEqual(AuthCount, length(A), {authorities, A}) || A <- Authorities],
    %% ensure they have the same view of the world, except for themselves
    [?assertEqual(Brokers, V, {view, V, Brokers}) || V <- Views].
