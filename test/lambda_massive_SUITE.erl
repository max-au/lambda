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
    [{timetrap, {seconds, 10}}].

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

start_tier(Boot, Count, AuthorityCount) ->
    lambda_async:pmap([
        fun () ->
            ExtraArgs = if Seq rem AuthorityCount =:= 1 -> ["-lambda", "authority", "true"]; true -> [] end,
            {ok, Peer} = peer:start_link(#{node => peer:random_name(),
                args => ["-boot", Boot | ExtraArgs], connection => standard_io}),
            unlink(Peer),
            Peer
        end
        || Seq <- lists:seq(1, Count)]).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    %% start a tier, with several authorities
    Peers = start_tier(create_release(Priv), 4, 2),
    Authority = hd(peer:apply(hd(Peers), lambda_broker, authorities, [lambda_broker])),
    %% TODO: ensure all nodes have correct view of the world
    %% stop everything
    lambda_async:pmap([{peer, stop, [P]} || P <- Peers]),
    ?assert(is_pid(Authority), {pid, Authority}).
