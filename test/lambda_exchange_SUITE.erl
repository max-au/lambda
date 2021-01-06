%% @doc
%%     Tests Lambda exchange module.
%% @end
-module(lambda_exchange_SUITE).
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
    basic/0, basic/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic].

init_per_suite(Config) ->
    {ok, Physical} = lambda_discovery:start_link(),
    erlang:unlink(Physical),
    {ok, Broker} = lambda_broker:start_link(#{}),
    erlang:unlink(Broker),
    [{broker, Broker}, {physical, Physical} | Config].

end_per_suite(Config) ->
    gen_server:stop(?config(broker, Config)),
    gen_server:stop(?config(physical, Config)),
    proplists:delete(broker, proplists:delete(physical, Config)).

init_per_testcase(TestCase, Config) ->
    {ok, Pid} = lambda_exchange:start_link(TestCase),
    [{exchange, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    gen_server:stop(?config(exchange, Config)),
    proplists:delete(exchange, Config).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Simple test publishing and ordering capacity"}].

basic(Config) when is_list(Config) ->
    Cap = 100,
    BuyId = 1,
    lambda_exchange:sell(?config(exchange, Config), contact, 0, Cap),
    lambda_exchange:buy(?config(exchange, Config), BuyId, Cap),
    receive
        {order, BuyId, ?FUNCTION_NAME, [{contact, Cap}]} ->
            ok;
        Other ->
            ?assert(false, Other)
    end.
