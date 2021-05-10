%% @doc
%%     Tests Lambda async helpers
%% @end
-module(lambda_async_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    groups/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    concurrent/0, concurrent/1,
    timeout/0, timeout/1,
    queue/0, queue/1,
    custom/0, custom/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [{group, parallel}].

groups() ->
    [{parallel, [parallel], [basic, concurrent, timeout, queue, custom]}].

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Smoke tests"}].

basic(Config) when is_list(Config) ->
    ?assertEqual([1, 2, 3],
        lambda_async:pmap([{rand, uniform, [1]}, fun() -> 2 end, {fun (X) -> X end, [3]}])).

concurrent() ->
    [{doc, "100 requests 100 ms each should be faster than 1 second"}].

concurrent(Config) when is_list(Config) ->
    {TimeUs, _} = timer:tc(fun () -> lambda_async:pmap([{timer, sleep, [100]} || _ <- lists:seq(1, 100)]) end),
    ?assert(TimeUs < 1000000).

timeout() ->
    [{doc, "Verifies timeout behaviour"}].

timeout(Config) when is_list(Config) ->
    #{ok := Ok, {'EXIT', timeout} := Timeout} =
        lambda_async:pmap([{timer, sleep, [Tmo]} || _ <- lists:seq(1, 100), Tmo <- [100, 5000]], unique, 2000),
    %% 100 are 'ok', 100 timeout
    ?assertEqual(100, length(Ok)),
    ?assertEqual(100, length(Timeout)).

queue() ->
    [{doc, "Tests async + wait combinations"}].

queue(Config) when is_list(Config) ->
    Ctx = lambda_async:start_link(unique),
    %% 10 very slow 'yes', 10 fast yes
    Ctx = lambda_async:async(Ctx, [{fun (Z) -> receive after Z -> yes end end, [T]}
        || T <- [100, 5000], _ <- lists:seq(1, 10)]),
    %% 10 quick 'okay'
    lambda_async:async(Ctx, [fun () -> ok end || _ <-lists:seq(1, 10)]),
    %% 1 crash with reason = 'test'
    lambda_async:async(Ctx, [{erlang, exit, [test]}]),
    Ret = lambda_async:wait(Ctx, 1000),
    ?assertEqual(10, length(maps:get(ok, Ret)), Ret),
    ?assertEqual(10, length(maps:get({'EXIT', timeout}, Ret)), Ret),
    ?assertEqual(10, length(maps:get(yes, Ret)), Ret),
    %% ensure ID is returned correctly
    ?assertEqual([{erlang, exit, [test]}], maps:get({'EXIT', test}, Ret), Ret).

custom() ->
    [{doc, "Test custom reduce function"}].

custom(Config) when is_list(Config) ->
    #{500 := FH, 1000 := T} =
        lambda_async:pmap([{rand, uniform, [Top]} || _ <- lists:seq(1, 100), Top <- [500, 1000]],
            {fun ({{rand, uniform, [Top]}, Res}, Acc) ->
                maps:put(Top, [Res | maps:get(Top, Acc, [])], Acc)
            end, #{}}, infinity),
    ?assertEqual(100, length(FH)),
    ?assertEqual(100, length(T)).