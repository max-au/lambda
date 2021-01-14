%% @doc
%%  Lambda: API for publishing servers and turning modules into clients.
%% @end
-module(lambda).
-author("maximfca@gmail.com").

%% API
-export([
    publish/1,
    discover/1
]).

-type meta() :: #{
    md5 := binary(),
    exports := [{atom(), pos_integer()}],
    attributes => [term()]
}.

-export_type([meta/0]).

%%--------------------------------------------------------------------
%% @doc Discovers a module, and starts a PLB for that module under lambda supervision.
-spec discover(module()) -> {ok, pid()} | ignore.
discover(Module) ->
    case erlang:module_loaded(Module) of
        true ->
            %% module is available locally
            ignore;
        false ->
            {ok, Plb} = lambda_plb_sup:start_plb(Module, #{low => 1, high => 10}),
            Module = lambda_plb:compile(Plb),
            {ok, Plb}
    end.

%% @doc Publishes  a module, starting server under lambda supervision.
-spec publish(module()) -> gen:start_ret().
publish(Module) ->
    lambda_server_sup:start_server(Module, #{capacity => 10}).
