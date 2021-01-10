%% @doc
%% Lambda: supervisor for module servers, when modules are
%%  started as a part of lambda (hosted modules), or dynamically
%%  via lambda API. If Lambda is used in a support role, your
%%  application should supervise module servers. This is only
%%  a convenience primitive.
%% @end
-module(lambda_server_sup).
-author("maximfca@gmail.com").

-behaviour(supervisor).

-export([
    start_server/2,
    start_link/1
]).

-export([init/1]).

-spec start_server(module(), lambda_server:options()) -> ok.
start_server(Mod, Options) ->
    supervisor:start_child(?MODULE, mod_spec(Mod, Options)).

-spec start_link(Mods :: [{module(), pos_integer()}]) -> supervisor:startlink_ret().
start_link(Modules) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Modules).

-spec init([{module(), pos_integer()}]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Modules) ->
    %% all children can restart independently
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 10},
    %%
    ChildSpec = [mod_spec(Mod, Options) || {Mod, Options} <- Modules],
    {ok, {SupFlags, ChildSpec}}.

%%--------------------------------------------------------------------
%% Internal implementation

mod_spec(Mod, Options) ->
    ChildName = list_to_atom(lists:concat(["lambda_server_", Mod])),
    #{
        id => ChildName,
        start => {lambda_server, start_link, [lambda_broker, Mod, Options]},
        modules => [lambda_server, Mod]
    }.
