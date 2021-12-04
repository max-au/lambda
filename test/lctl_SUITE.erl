%% @doc
%%     Tests for lctl escript.
%% @end
-module(lctl_SUITE).
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
    authority_list/0, authority_list/1,
    application_lifecycle/0, application_lifecycle/1
]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [authority_list, application_lifecycle].

init_per_suite(Config) ->
    Root = filename:join(proplists:get_value(priv_dir, Config), "lambda"),
    LambdaBoot = lambda_test:create_release(Root),
    [{boot, LambdaBoot} | Config].

end_per_suite(Config) ->
    proplists:delete(boot, Config).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Convenience & data
lctl(Args, AuthNode) ->
    lctl(Args, AuthNode, "", 5000).

lctl(Args, AuthNode, Flags, Timeout) ->
    {ok, Lctl} = filelib:find_source(code:which(lctl)),
    LibRoot = filename:dirname(filename:dirname(filename:dirname(Lctl))),
    Escript = os:find_executable("escript"),
    BootStr = lists:flatten(io_lib:format("\"{epmd,['~s']}\"", [hd(string:lexemes(atom_to_list(AuthNode), "@"))])),
    %% FIXME: keep in sync with rebar.config somehow
    ErlFlags = Flags ++ "-setcookie lambda -start_epmd false -connect_all false -epmd_module lambda_discovery +swt low +sbt u +sbwt none +sbwtdcpu none +sbwtdio none -lambda bootspec " ++ BootStr,
    Port = erlang:open_port({spawn_executable, Escript},
        [{args, [Lctl | Args]}, {env, [{"ERL_LIBS", LibRoot}, {"ERL_FLAGS", ErlFlags}]},
            hide, binary, exit_status, stderr_to_stdout, {line, 1_000_000}]),
    read_full(Port, [], Timeout).

read_full(Port, IoList, Timeout) ->
    receive
        {Port, {exit_status, Status}} ->
            {ok, Status, lists:reverse(IoList)};
        {Port, {data, {AnyLine, Data}}} when AnyLine =:= eol; AnyLine =:= noeol ->
            read_full(Port, [Data | IoList], Timeout)
    after Timeout ->
        {error, timeout, lists:reverse(IoList)}
    end.


%%--------------------------------------------------------------------
%% Test Cases

authority_list() ->
    [{doc, "List running authorities"}].

authority_list(Config) when is_list(Config) ->
    LambdaBoot = proplists:get_value(boot, Config),
    %% start initial authority
    {AuthPeer, AuthNode, BootSpec} = lambda_test:start_auth(?FUNCTION_NAME, LambdaBoot),
    %% start more authorities
    Auth2 = lambda_test:start_node_link(?FUNCTION_NAME, LambdaBoot, BootSpec, ["+S", "2:2"], true),

    %% run command line
    {ok, 0, Actual} = lctl(["authority", "list"], AuthNode),

    %% cleanup to avoid spamming test logs
    lambda_async:pmap([{peer, stop, [N]} || N <- [AuthPeer, Auth2]]),

    [<<"Total: 2">>, One, Two] = Actual,

    %% sort order is not defined
    ReportedNodes = lists:sort([hd(binary:split(S, <<" ">>)) || S <- [One, Two]]),
    ExpectedNodes = lists:sort([atom_to_binary(N) || N <- [AuthNode, Auth2]]),
    ?assertEqual(ExpectedNodes, ReportedNodes).

application_lifecycle() ->
    [{doc, "Deploys a new application, adds a module to it, runs a remote function call"}].

application_lifecycle(Config) when is_list(Config) ->
    AppName = "lctl_test",
    ModName = "arith",
    Data = proplists:get_value(data_dir, Config),
    Priv = proplists:get_value(priv_dir, Config),
    {ok, _Mod, []} = compile:file(filename:join(Data, ModName ++ ".erl"), [return, {outdir, Priv}]),
    %% start the cluster, with 1 authority and 3 worker nodes
    [{Auth, true}, {W1, false}, {W2, false}, {W3, false}] = Tier = lambda_test:start_tier(?FUNCTION_NAME,
        proplists:get_value(boot, Config), 4, 1),
    %% ensure tier connects to authorities
    Expected = [W1, W2, W3],
    [peer:call(P, lambda_bootstrap, discover, []) || P <- Expected],
    ?assertEqual(ok, peer:call(Auth, lambda_test, wait_connection, [Expected])),
    %% create the new the app
    {ok, 0, CreateResponse} = lctl(["application", "create", AppName], Auth),
    ct:pal("App Created: ~p", [CreateResponse]),
    %% verify that app has actually been deployed
    {ok, 0, AppList} = lctl(["application", "list"], Auth),
    ct:pal("App List: ~p", [AppList]),
    %% deploy the new module added to the app
    {ok, 0, ModDeploy} = lctl(["module", "deploy", AppName, ModName], Auth, "-pa " ++ Priv ++ " ", 5000),
    ct:pal("Module deployed: ~p", [ModDeploy]),
    %% list module
    {ok, 0, ModList} = lctl(["module"], Auth),
    ct:pal("Modules listed: ~p", [ModList]),
    %% verify that it indeed works via lctl - dynamically!
    {ok, 0, MulResult} = lctl(["request", ModName, "mul(2, 2)"], Auth),
    ?assertEqual([<<"4">>], MulResult),
    %% delete the app
    {ok, 0, DelResult} = lctl(["application", "delete", AppName], Auth),
    ct:pal("App Deleted: ~p", [DelResult]),
    lambda_test:stop_tier(Tier).
