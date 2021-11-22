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
    authority_list/0, authority_list/1
]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [authority_list].

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

    %% set the bootspec
    ok = application:set_env(lambda, bootspec, BootSpec, [{persistent, true}]),

    logger:set_primary_config(level, debug),

    %% run command line
    Actual = cli:run(["authority", "list"], #{modules => [lctl]}),
    Expected = atom_to_list(AuthNode),
    ?assertEqual({ok, Expected}, Actual),

    %% cleanup to avoid spamming test logs
    lambda_async:pmap([{peer, stop, [N]} || N <- [AuthPeer, Auth2]]).
