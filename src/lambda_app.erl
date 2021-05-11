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

-include_lib("kernel/include/logger.hrl").

-spec start(normal, []) -> {ok, pid()} | {error, {already_started, pid()}}.
start(normal, []) ->
    try lambda_discovery:get_node()
    catch exit:{noproc, _} ->
        ?LOG_WARNING("lambda requires '-epmd_module lambda_discovery' for alternative service discovery",
            [], #{domain => [lambda]})
    end,
    lambda_sup:start_link().

-spec stop(undefined) -> ok.
stop(undefined) ->
    ok.
