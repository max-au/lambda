%% @doc
%%  Lambda: API for publishing servers and turning modules into clients.
%% @end
-module(lambda).
-author("maximfca@gmail.com").

%% API
-export([
    publish/1,
    publish/2,
    discover/1,
    discover/2
]).

%%--------------------------------------------------------------------
%% @doc Discovers a module, and starts a PLB for that module under lambda supervision.
-spec discover(module()) -> {ok, pid()} | ignore.
discover(Module) ->
    discover(Module, #{capacity => erlang:system_info(schedulers)}).

-spec discover(module(), lambda_plb:options()) -> {ok, pid()} | ignore.
discover(Module, Options) ->
    case erlang:module_loaded(Module) of
        true ->
            %% module is available locally
            ignore;
        false ->
            {ok, Plb} = lambda_client_sup:start_plb(Module, Options),
            _ = lambda_plb:meta(Plb),
            {ok, Plb}
    end.

%% @doc Publishes  a module, starting server under lambda supervision.
-spec publish(module()) -> gen:start_ret().
publish(Module) ->
    publish(Module, #{capacity => erlang:system_info(schedulers)}).

-spec publish(module(), lambda_listener:options()) -> gen:start_ret().
publish(Module, Options) ->
    lambda_listener_sup:start_listener(Module, Options).
