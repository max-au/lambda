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

-export([start_link/0]).

-export([init/1]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    %% take children list from configuration (hosted deployment)
    case application:get_env(modules) of
        {ok, Mods} when is_list(Mods) ->
            %% check and expand module capacity when needed
            Modules = [check_mod(Mod) || Mod <- Mods],
            supervisor:start_link({local, ?MODULE}, ?MODULE, Modules);
        undefined ->
            ignore
    end.

-spec init([module()]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(Modules) ->
    %% all children can restart independently
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 10},
    %%
    ChildSpec = [
        #{
            id => Mod,
            start => {lambda_server, start_link, [Mod, Capacity]},
            modules => [lambda_server, Mod]
        } || {Mod, Capacity} <- Modules],
    {ok, {SupFlags, ChildSpec}}.

%%--------------------------------------------------------------------
%% Internal implementation

check_mod(Mod) when is_atom(Mod) ->
    %% by default, set the concurrency as 2x of schedulers
    %%  available.
    Sched = erlang:system_info(schedulers),
    {Mod, Sched * 2};
check_mod({Mod, Cap}) when is_atom(Mod), is_integer(Cap), Cap > 0 ->
    {Mod, Cap};
check_mod(Other) ->
    erlang:error({invalid_module_spec, Other}).
