%% @doc
%%     Tests for examples provided in README.md and documentation
%% @end
-module(lambda_readme_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    remote_stateless_update/0, remote_stateless_update/1,
    remote_api_update/0, remote_api_update/1,
    remote_canary/0, remote_canary/1,
    version_demo/0, version_demo/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic, remote_canary].

init_per_suite(Config) ->
    Root = filename:join(proplists:get_value(priv_dir, Config), "lambda"),
    LambdaBoot = lambda_test:create_release(Root),
    [{boot, LambdaBoot} | Config].

end_per_suite(Config) ->
    proplists:delete(boot, Config).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(remote_stateless_update, Config) ->
    %% clean up code paths that a test case may set
    Priv = proplists:get_value(priv_dir, Config),
    code:del_path(filename:join(Priv, "math-1.2.3/ebin")),
    code:del_path(filename:join(Priv, "math-1.2.4/ebin")),
    Config;
end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Convenience & data
compile_code(Lines) ->
    Tokens = [begin {ok, T, _} = erl_scan:string(L), T end || L <- Lines],
    Forms = [begin {ok, F} = erl_parse:parse_form(T), F end || T <- Tokens],
    {ok, _Module, Binary} = compile:forms(Forms),
    Binary.

calc_v1() ->
    ["-module(calc).",
        "-export([pi/1]).",
        "pi(Precision) when Precision >= 1, Precision =< 10 -> pi(4, -4, 3, Precision).",
        "pi(LastResult, Numerator, Denominator, Precision) ->  NextResult = LastResult + Numerator / Denominator,"
        "Pow = math:pow(10, Precision), case trunc(LastResult * Pow) =:= trunc(NextResult * Pow) of true ->"
        "trunc(NextResult * Pow) / Pow; false -> pi(NextResult, -1 * Numerator, Denominator + 2, Precision) end."].

vdemo_v1() ->
    ["-module(vdemo).", "-export([where/0]).", "where() -> timer:sleep(10),{ok, Host} = inet:gethostname(),Host."].

vdemo_v2() ->
    ["-module(vdemo).", "-export([where/0, fqdn/0]).", "-lambda({vsn, 2}).",
        "where() -> timer:sleep(10),{ok, Host} = inet:gethostname(),Host.",
        "fqdn() -> timer:sleep(10),{ok, Host} = inet:gethostname(),Host ++ inet_db:res_option(domain)."].

dist_args(Host) ->
    %% if test host does not have IPv4 address, all peers must start with IPv6
    IPv6 = inet_db:res_option(inet6),
    case inet_res:gethostbyname(Host) of
        {ok, _HostEnt} ->
            [];
        {error, nxdomain} when IPv6 =:= false ->
            %% check whether node has IPv6 address and if yes, use IPv6
            case inet_res:gethostbyname(Host, inet6) of
                {ok, _HE} ->
                    ["-proto_dist", "inet6_tcp"];
                {error, nxdomain} ->
                    []
            end
    end.

app_spec(App, Vsn, Mod) ->
    Spec = {application, App,
        [{description, "app"}, {vsn, Vsn}, {registered, []}, {applications, [kernel, stdlib]}, {modules, [Mod]}]},
    list_to_binary(lists:flatten(io_lib:format("~tp.", [Spec]))).

make_app(Priv, Name, Vsn, Mod, Code) ->
    ModS = atom_to_list(Mod),
    NameS = atom_to_list(Name),
    Path = filename:join([Priv, NameS ++ "-" ++ Vsn]),
    ok = filelib:ensure_dir(filename:join(Path, "dummy")),
    ok = file:write_file(filename:join(Path, NameS ++ ".app"), app_spec(Name, Vsn, Mod)),
    ok = file:write_file(filename:join(Path, ModS ++ ".erl"), list_to_binary(lists:flatten(lists:join("\n", Code)))),
    {ok, Mod} = compile:file(filename:join(Path, ModS ++ ".erl"), [debug_info, {outdir, Path}]),
    code:add_path(Path),
    Path.

start_auth(TestCase, Boot) ->
    {AuthPeer, AuthNode} = lambda_test:start_node_link(TestCase, Boot, undefined, ["+S", "2:2"], true),
    Addr = peer:apply(AuthPeer, lambda_discovery, get_node, []),
    BootSpec = [{static, #{{lambda_authority, AuthNode} => Addr}}],
    {AuthPeer, AuthNode, BootSpec}.

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Basic test starting extra node, publishing/discovering lambda and running it remotely"}].

basic(Config) when is_list(Config) ->
    {ok, Host} = inet:gethostname(),
    DistArgs = dist_args(Host),
    %% Prefer longnames (for 'peer' does it too)
    SrvNode = list_to_atom(lists:concat([authority, "@", Host,
        case inet_db:res_option(domain) of [] -> ""; Domain -> [$. | Domain] end])),
    {ok, Server} = peer:start_link(#{connection => standard_io, node => SrvNode,
        args => ["-lambda", "authority", "true" | DistArgs]}),
    %% local calc module into Server (module does not exist on disk)
    Code = compile_code(calc_v1()),
    {module, calc} = peer:apply(Server, code, load_binary, [calc, nofile, Code]),
    {ok, StartedApps} = peer:apply(Server, application, ensure_all_started, [lambda]),
    %% Server: publish calc
    {ok, _Srv} = peer:apply(Server, lambda, publish, [calc, #{capacity => 3}]),
    %% Client: discover calc (using epmd)
    {ok, Client} = peer:start_link(#{connection => standard_io, node => peer:random_name(), args => DistArgs}),
    {ok, StartedApps} = peer:apply(Client, application, ensure_all_started, [lambda]),
    {ok, Plb} = peer:apply(Client, lambda, discover, [calc, #{capacity => 10}]),
    %% Execute calc remotely (on the client)
    ?assertEqual(3.14, peer:apply(Client, calc, pi, [2])),
    %% continue with capacity expansion
    %% run another server with more capacity
    {ok, Srv2} = peer:start_link(#{connection => standard_io, node => peer:random_name(), args => DistArgs}),
    {ok, StartedApps} = peer:apply(Srv2, application, ensure_all_started, [lambda]),
    {module, calc} = peer:apply(Srv2, code, load_binary, [calc, nofile, Code]),
    {ok, _Srv2Srv} = peer:apply(Srv2, lambda, publish, [calc, #{capacity => 3}]),
    %% ideally should be a whitebox flushing queues of the involved parties
    %% TODO: replace sleep with some capacity notification in PLB itself
    timer:sleep(200),
    %% ensure that client got more capacity: originally 3, 1 request executed,
    %%  2 more left, and 3 more connected = total of 5
    ?assertEqual(5, peer:apply(Client, lambda_plb, capacity, [Plb])),
    %% Shutdown
    lambda_async:pmap([{peer, stop, [S]} || S <- [Server, Srv2]]),
    %% another flaky sleep, ugh. Probably easier to solve with monitors?
    timer:sleep(200),
    %% Capacity goes down to zero
    ?assertEqual(0, peer:apply(Client, lambda_plb, capacity, [Plb])),
    %% stop PLB, which should make 'calc' module unload
    ok = peer:apply(Client, gen_server, stop, [Plb]),
    ?assertEqual(non_existing, peer:apply(Client, code, which, [calc])).

remote_stateless_update() ->
    [{doc, "Update stateless code on a remote tier"}].

%% This test simulates a deployment routine: developer creates new version of
%%  code and submits it to the repository. It triggers new release build, which
%%  then gets delivered to lambda-supported tier, and gets hot-loaded.
remote_stateless_update(Config) when is_list(Config) ->
    LambdaBoot = proplists:get_value(boot, Config),
    %% Root: RELEASES ROOT
    Root = filename:join(proplists:get_value(priv_dir, Config), ?FUNCTION_NAME),
    %% v1 release: very slow pi function
    LibDir = filename:join(Root, "lib"),
    Math1 = make_app(LibDir, math, "0.1.0", calc, calc_v1()),
    BootV1 = lambda_test:create_release(Root, [math], "math", "1.0.0"),
    %%
    %% create RELEASES file
    ?assertEqual(ok, release_handler:create_RELEASES(Root, filename:join(Root, "releases"),
        BootV1 ++ ".rel", [])),
    %% clean up
    code:del_path(Math1),
    application:unload(math),
    %%
    %% start authority and client nodes
    {AuthPeer, AuthNode} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, undefined, ["+S", "2:2"], true),
    %% make bootspec for the client & server
    Addr = peer:apply(AuthPeer, lambda_discovery, get_node, []),
    BootSpec = [{static, #{{lambda_authority, AuthNode} => Addr}}],
    {ClientPeer, _ClientNode} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec, ["+S", "2:2"], false),
    %% start server, doing v1 release, publishing calc
    {_ServerPeer, _ServerNode} = lambda_test:start_node_link(?FUNCTION_NAME, BootV1, BootSpec,
        ["+S", "2:2", "-lambda", "publish", "[calc]"], false),
    %% ensure it actually works...
    {ok, _Plb} = peer:apply(ClientPeer, lambda, discover, [calc]),
    ?assertEqual(3.14, peer:apply(ClientPeer, calc, pi, [2])),

    %% v2 release: pi gets faster for some cases
    Math2 = make_app(LibDir, math, "0.2.0", calc, calc_v1()),
    AppUp = {"0.2.0",
        [{"0.1.0", [{load_module, calc}]}],
        [{"0.1.0", [{load_module, calc}]}]},
    ok = file:write_file(filename:join(Math2, "math.appup"),
        list_to_binary(lists:flatten(io_lib:format("~tp.", [AppUp])))),
    _BootV2 = lambda_test:create_release(Root, [math], "math", "1.1.0"),
    %% add code path to v1 back, for relup/appup to work
    code:add_path(Math1),
    %%
    %%?assertEqual(ok, systools:make_relup(BootV2, [BootV1], [], [{outdir, Root}])),
    %% do the relup
    %%?assertEqual(ok, peer:apply(ServerPeer, release_handler, check_install_release, ["1.1.0"])),
    %% measure performance (which must be higher than before relup)
    ok.

remote_api_update() ->
    [{doc, "Update API of a remote tier"}].

%% This test simulates a deployment routine: developer changes export spec of a
%%  remotely executed module. In order to perform safe upgrade, lambda needs to
%%  ensure that no calls to old APIs are made, and only then perform hot code
%%  upgrade.
remote_api_update(Config) when is_list(Config) ->
    LambdaBoot = proplists:get_value(boot, Config),
    %% start authority and client nodes
    {_AuthPeer, _AuthNode, BootSpec} = start_auth(?FUNCTION_NAME, LambdaBoot),
    {ClientPeer, _ClientNode} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec, ["+S", "2:2"], false),
    {_ServerPeer, _ServerNode} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec, ["+S", "2:2"], false),
    %% canary "calc" on the server
    {ok, _Plb} = peer:apply(ClientPeer, lambda, discover, [calc]),
    ?assertEqual(3.14, peer:apply(ClientPeer, calc, pi, [2])),
    ok.

remote_canary() ->
    [{doc, "Upload and deploy a new module to the server"}].

%% This test simulates a "canary" deployment process, when new code is loaded
%%  to a selected server.
remote_canary(Config) when is_list(Config) ->
    ok.

%% This test verifies "versioning" demo to work.
version_demo() ->
    [{doc, "Demo explained in doc/VERSIONING.md"}].

version_demo(Config) when is_list(Config) ->
    LambdaBoot = proplists:get_value(boot, Config),
    Root = filename:join(proplists:get_value(priv_dir, Config), ?FUNCTION_NAME),
    LibDir = filename:join(Root, "lib"),

    %% start authority
    {_AuthPeer, _AuthNode, BootSpec} = start_auth(?FUNCTION_NAME, LambdaBoot),

    %% make server v1 release
    VDemo1 = make_app(LibDir, vdemo, "0.1.0", vdemo, vdemo_v1()),
    BootV1 = lambda_test:create_release(Root, [vdemo], "vdemo", "1.0.0"),
    code:del_path(VDemo1),
    %% start server v1
    {_Server1, _} = lambda_test:start_node_link(?FUNCTION_NAME, BootV1, BootSpec,
        ["+S", "2:2", "-lambda", "publish", "[{vdemo,#{capacity=>4}}]"], false),

    %% start client #1 (reuse lambda release, but in the demo it's rebar3 shell)
    {Client1, _ClientNode} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec,
        ["+S", "2:2", "-lambda", "discover", "[{vdemo,#{capacity=>8}}]"], false),
    timer:sleep(600),
    %% discover and ensure there is "where" but not "fqdn"
    {ok, Host} = inet:gethostname(),
    ?assertEqual(Host, peer:apply(Client1, vdemo, where, [])),
    ?assertException(error, undef, peer:apply(Client1, vdemo, fqdn, [])),
    [{_, Plb, _, _}] = peer:apply(Client1, supervisor, which_children, [lambda_client_sup]),
    InitialCapacity = peer:apply(Client1, lambda_plb, capacity, [Plb]),

    %% start client #2
    {Client2, _} = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec,
        ["+S", "2:2", "-lambda", "discover", "[{vdemo,#{vsn=>2,capacity=>2}}]"], false),

    %% start server v2
    VDemo2 = make_app(LibDir, vdemo, "0.2.0", vdemo, vdemo_v2()),
    BootV2 = lambda_test:create_release(Root, [vdemo], "vdemo", "1.1.0"),
    code:del_path(VDemo2),
    {_Server2, _} = lambda_test:start_node_link(?FUNCTION_NAME, BootV2, BootSpec,
        ["+S", "2:2", "-lambda", "publish", "[{vdemo,#{capacity=>10}}]"], false),

    timer:sleep(600),
    % ensure it actually has fqdn
    ?assertEqual(Host ++ inet_db:res_option(domain), peer:apply(Client2, vdemo, fqdn, [])),

    %% bonus: check first client capacity, which should be higher now
    NewCapacity = peer:apply(Client1, lambda_plb, capacity, [Plb]),
    ?assert(InitialCapacity < NewCapacity, {scale, InitialCapacity, NewCapacity}).