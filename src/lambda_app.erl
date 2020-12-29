%% @doc
%%  Lambda application module.
%% @end
-module(lambda_app).
-author("maximfca@gmail.com").

-export([
    start/2,
    stop/1
]).

-behaviour(application).

start(_StartType, _StartArgs) ->
    lambda_sup:start_link().

stop(_State) ->
    ok.
