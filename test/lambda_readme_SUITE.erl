%% @doc
%%     Tests for examples provided in README.md and documentation
%% @end
-module(lambda_readme_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    remote_stateless_update/0, remote_stateless_update/1,
    remote_api_update/0, remote_api_update/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic, remote_stateless_update].

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
    Priv = proplists:get_value(priv_dir, Config),
    %% v1 release: very slow pi function
    BootV1 = lambda_test:create_release(Priv, [math]),
    {AuthPeer, AuthNode} = lambda_test:start_node_link(?FUNCTION_NAME, BootV1, undefined, ["+S", "2:2"], true),
    %% make bootspec for the client
    ClientBoot = lambda_test:create_release(Priv, []),
    Addr = peer:apply(AuthPeer, lambda_discovery, get_node, []),
    BootSpec = [{static, #{{lambda_authority, AuthNode} => Addr}}],
    {ClientPeer, ClientNode} = lambda_test:start_node_link(?FUNCTION_NAME, ClientBoot, BootSpec, ["+S", "2:2"], false),
    %% ensure it actually works...
    {ok, _Plb} = peer:apply(ClientPeer, lambda, discover, [math]),
    ?assertEqual(3.14, peer:apply(ClientPeer, math, pi, [2])),

    %% v2 release: pi gets faster for some cases
    BootV2 = lambda_test:create_release(Priv, [math]),
    %% do the relup
    %% measure performance (which must be higher than before relup)
    ok.

remote_api_update() ->
    [{doc, "Update API of a remote tier"}].

%% This test simulates a deployment routine: developer changes export spec of a
%%  remotely executed module. In order to perform safe upgrade, lambda needs to
%%  ensure that no calls to old APIs are made, and only then perform hot code
%%  upgrade.
remote_api_update(Config) when is_list(Config) ->
    ok.
