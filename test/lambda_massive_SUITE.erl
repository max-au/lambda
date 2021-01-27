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

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Make a single release, start several copies of it using customised settings"}].

basic(Config) when is_list(Config) ->
    Priv = proplists:get_value(priv_dir, Config),
    Boot = create_release(Priv),
    {ok, Auth} = peer:start_link(#{node => peer:random_name(), args => ["-boot", Boot], connection => standard_io}),
    Authority = peer:apply(Auth, erlang, whereis, [lambda_authority]),
    peer:stop(Auth),
    ?assert(is_pid(Authority), {pid, Authority}).
