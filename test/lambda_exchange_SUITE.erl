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
    basic/0, basic/1,
    broker_down/0, broker_down/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic, broker_down].

init_per_suite(Config) ->
    {ok, Physical} = lambda_discovery:start_link(),
    erlang:unlink(Physical),
    {ok, Broker} = lambda_broker:start_link(),
    erlang:unlink(Broker),
    [{broker, Broker}, {physical, Physical} | Config].

end_per_suite(Config) ->
    gen_server:stop(proplists:get_value(broker, Config)),
    gen_server:stop(proplists:get_value(physical, Config)),
    proplists:delete(broker, proplists:delete(physical, Config)).

init_per_testcase(TestCase, Config) ->
    {ok, Pid} = lambda_exchange:start_link(TestCase),
    [{exchange, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    gen_server:stop(proplists:get_value(exchange, Config)),
    proplists:delete(exchange, Config).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Simple test publishing and ordering capacity"}].

basic(Config) when is_list(Config) ->
    Cap = 100,
    BuyId = 1,
    Exch = proplists:get_value(exchange, Config),
    lambda_exchange:sell(Exch, 0, Cap, #{}, contact),
    lambda_exchange:buy(Exch, BuyId, Cap, #{}),
    receive
        {complete_order, ?FUNCTION_NAME, BuyId, [{contact, Cap, #{}}]} ->
            ok;
        Other ->
            ?assert(false, Other)
    end.

broker_down() ->
    [{doc, "Ensures that broker down wipes out orders from exchange"}].

broker_down(Config) when is_list(Config) ->
    Exch = proplists:get_value(exchange, Config),
    Broker = spawn(
        fun () -> lambda_exchange:sell(Exch, 0, 1, #{}, ?FUNCTION_NAME), receive after infinity -> ok end end),
    %% ensure exchange has the order
    lambda_test:sync(Exch),
    ?assertMatch([{order, _, 1, Broker, _, _, _}], lambda_exchange:orders(Exch, #{type => sell})),
    %% kill the broker
    exit(Broker, kill),
    lambda_test:sync(Exch),
    ?assertEqual([], lambda_exchange:orders(Exch, #{})).
