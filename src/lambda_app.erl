%% @doc
%%  Lambda application callback module.
%% @end
-module(lambda_app).
-compile(warn_missing_spec).
-author("maximfca@gmail.com").

-export([
    start/2,
    stop/1
]).

-behaviour(application).

-spec start(normal, []) -> {ok, pid()} | {error, {already_started, pid()}}.
start(normal, []) ->
    lambda_sup:start_link().

-spec stop(undefined) -> ok.
stop(undefined) ->
    ok.
