%% @doc
%%  Lambda application callback module.
%% @end
-module(lambda_app).
-author("maximfca@gmail.com").

-export([
    start/2,
    stop/1
]).

-behaviour(application).

-spec start(normal, []) -> supervisor:startlink_ret().
start(normal, []) ->
    lambda_sup:start_link().

-spec stop(undefined) -> ok.
stop(undefined) ->
    ok.
