%% @doc
%%     Tests Lambda async helpers
%% @end
-module(lambda_async_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    concurrent/0, concurrent/1,
    timeout/0, timeout/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic, concurrent, timeout].

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Smoke tests"}].

basic(Config) when is_list(Config) ->
    [1, 2, 3] = lambda_async:pmap([{rand, uniform, [1]}, fun() -> 2 end, {fun (X) -> X end, [3]}]).

concurrent() ->
    [{doc, "100 requests 100 ms each should be faster than 1 second"}].

concurrent(Config) when is_list(Config) ->
    {TimeUs, _} = timer:tc(fun () -> lambda_async:pmap([{timer, sleep, [100]} || _ <- lists:seq(1, 100)]) end),
    ?assert(TimeUs < 1_000_000).

timeout() ->
    [{doc, "Verifies timeout behaviour"}].

timeout(Config) when is_list(Config) ->
    Ret = lambda_async:pmap([{timer, sleep, [Tmo]} || _ <- lists:seq(1, 100), Tmo <- [100, 5000]], 2000),
    %% 50 are 'ok', 50 killed
    {Ok, Killed} = lists:partition(fun (X) -> X =:= ok end, Ret),
    ?assertEqual(100, length(Ok)),
    ?assertEqual(100, length(Killed)).
