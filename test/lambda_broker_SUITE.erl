%% @doc
%%     Tests broker process.
%% @end
-module(lambda_broker_SUITE).
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
    proper/0, proper/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("proper/include/proper.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic, proper].

init_per_suite(Config) ->
    logger:set_primary_config(level, all),
    {ok, Pg} = pg:start(pg),
    [{pg, Pg} | Config].

end_per_suite(Config) ->
    gen_server:stop(?config(pg, Config)),
    proplists:delete(pg, Config).

init_per_testcase(TestCase, Config) ->
    {ok, Pid} = lambda_broker:start_link(TestCase),
    [{broker, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    gen_server:stop(?config(broker, Config)),
    proplists:delete(broker, Config).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Simple test publishing and ordering capacity"}].

basic(Config) when is_list(Config) ->
    Cap = 100,
    Self = self(),
    lambda_broker:sell(?FUNCTION_NAME, Cap),
    lambda_broker:buy(?FUNCTION_NAME, Cap),
    receive
        {order, [{Self, Cap}]} ->
            ok;
        Other ->
            ?assert(false, Other)
    end.

%%--------------------------------------------------------------------
%% Property based tests
%% Broker model

proper() ->
    [{doc, "Property based test"}].

proper(Config) when is_list(Config) ->
    ok.
