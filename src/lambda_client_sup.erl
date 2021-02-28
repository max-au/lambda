%% @doc
%% Lambda: supervisor for plb supervised by lambda application.
%% @end
-module(lambda_client_sup).
-author("maximfca@gmail.com").

-behaviour(supervisor).

-export([
    start_plb/2,
    start_link/1
]).

-export([init/1]).

-include_lib("kernel/include/logger.hrl").

-spec start_plb(module(), lambda_plb:options()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_plb(Mod, Options) ->
    supervisor:start_child(?MODULE, mod_spec(Mod, Options)).

-spec start_link(Mods :: [{module(), pos_integer()}]) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Modules) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Modules).

-spec init([{module(), pos_integer()}]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Modules) ->
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 10},
    ChildSpec = [mod_spec(Mod, Options) || {Mod, Options} <- Modules],
    ?LOG_DEBUG("Resulting childspec: ~200p", [ChildSpec], #{domain => [lambda]}),
    {ok, {SupFlags, ChildSpec}}.

%%--------------------------------------------------------------------
%% Internal implementation

mod_spec(Mod, Options) ->
    ChildName = list_to_atom(lists:concat(["lambda_plb_", Mod])),
    #{
        id => ChildName,
        start => {lambda_plb, start_link, [lambda_broker, Mod, Options]},
        modules => [lambda_plb, Mod]
    }.
