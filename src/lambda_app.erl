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

-spec start(normal, []) -> {ok, pid()} | {error, {already_started, pid()}}.
start(normal, []) ->
    try lambda_discovery:get_node()
    catch exit:{noproc, _} ->
        error("lambda requires '-epmd_module lambda_discovery' argument to erl, or 'ERL_FLAGS=\'-args_file config/shell.vm.args\' rebar3 shell'")
    end,
    lambda_sup:start_link().

-spec stop(undefined) -> ok.
stop(undefined) ->
    ok.
