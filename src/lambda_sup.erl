%% @doc
%%  Lambd: top-level supervisor.
%%  May start authority, if requested,
%%      and always starts registry.
%% @end
-module(lambda_sup).
-author("maximfca@gmail.com").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% allow 2 restarts every 10 seconds
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 10},
    %% Authority: start if configured
    %% Deliberately crash if misconfigured (non-boolean)
    Authority = case application:get_env(authority) of
                    true ->
                        [#{
                            id => lambda_authority,
                            start => {lambda_authority, start_link, []},
                            modules => [lambda_authority]
                        }];
                    false ->
                        [];
                    undefined ->
                        []
                end,
    %%,
    RegistrySpecs = [
        #{
            id => lambda_registry,
            start => {lambda_registry, start_link, []},
            modules => [lambda_registry]
        }
    ],
    {ok, {SupFlags, Authority ++ RegistrySpecs}}.
