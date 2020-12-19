%%%-------------------------------------------------------------------
%% @doc lambda public API
%% @end
%%%-------------------------------------------------------------------

-module(lambda_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    lambda_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
