%% @doc
%%     Lambda channel tests.
%% @end
-module(lambda_channel_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    [basic].


%%--------------------------------------------------------------------
%% Convenience primitives

expected(M, F, A) ->
    try erlang:apply(M, F, A)
    catch Class:Reason -> {Class, Reason}
    end.

%% Message-based RPC (does not do any monitoring)
rpc(Channel, M, F, A) ->
    Expected = expected(M, F, A),
    Actual = case gen_server:call(Channel, {job, M, F, A}) of
                 {response, R} -> R;
                 {C, R, _S} -> {C, R} %% instead of catch
             end,
    ?assertEqual(Expected, Actual).

%% Same as RPC, but uses new OTP 23 "remote spawn" capability
erpc(Channel, M, F, A) ->
    Expected = expected(M, F, A),
    Actual =
        try Rid = erpc:send_request(node(Channel), lambda_channel, handle, [Channel, M, F, A]),
            erpc:wait_response(Rid, infinity) of
                {response, R} -> R
        catch Class1:{exception, Reason1, _Stk1} -> {Class1, Reason1}
        end,
    ?assertEqual(Expected, Actual).

%%--------------------------------------------------------------------
%% Smoke tests

basic() ->
    [{doc, "Tests basic functioning"}].

basic(Config) when is_list(Config) ->
    Cap = 4,
    {ok, Channel} = lambda_channel:start_link(self(), Cap),
    %% receive demand first
    receive
        {demand, Cap, Channel} ->
            ok
    end,
    %% execute a successful job
    rpc(Channel, erlang, node, []),
    erpc(Channel, erlang, node, []),
    %% execute a failing job
    rpc(Channel, erlang, node, [1]),
    erpc(Channel, erlang, node, [1]),
    %% it was 4 requests, receive more demand now
    receive
        {demand, Cap, Channel} ->
            ok
    end,
    gen:stop(Channel).
