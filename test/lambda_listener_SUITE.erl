%% @doc
%%     Lambda server tests.
%% @end
-module(lambda_listener_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1,
    broker_monitor/0, broker_monitor/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    [basic, broker_monitor].


%%--------------------------------------------------------------------
%% Convenience primitives

%%--------------------------------------------------------------------
%% Smoke tests

basic() ->
    [{doc, "Tests basic functioning"}].

basic(Config) when is_list(Config) ->
    Cap = 10,
    {ok, Server} = lambda_listener:start_link(self(), ?MODULE, #{capacity => Cap}),
    gen:stop(Server).

broker_monitor() ->
    [{doc, "Tests that listener is not locked to a broker"}].

broker_monitor(Config) when is_list(Config) ->
    %% start the server without broker
    {ok, Server} = lambda_listener:start_link(?FUNCTION_NAME, ?MODULE, #{capacity => 100}),
    %% ensure it works when broker is registered
    register(?FUNCTION_NAME, self()),
    Server ! {connect, spawn(fun () -> ok end), 10}, %% trigger "sell" message
    receive
        {'$gen_cast',{sell, ?MODULE, Server, 90, #{}}} ->
            ok
    end,
    %% check monitoring works, and we get the capacity back
    receive
        {'$gen_cast',{sell, ?MODULE, Server, 100, #{}}} ->
            ok
    end,
    gen:stop(Server).
