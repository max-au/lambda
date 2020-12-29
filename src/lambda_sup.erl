%% @doc
%%  Lambd: top-level supervisor.
%%  May start authority, if requested,
%%      and always starts a broker.
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
    {ok, App} = application:get_application(),
    %% Bootstrap format is a map of {registered_name, node} => epmd_address.
    Bootstrap = application:get_env(App, bootstrap, #{}),
    %% Authority: start if configured
    %% Deliberately crash if misconfigured (non-boolean)
    Authority = case application:get_env(App, authority, false) of
                    true ->
                        [#{
                            id => lambda_authority,
                            start => {lambda_authority, start_link, [Bootstrap]},
                            modules => [lambda_authority]
                        }];
                    false ->
                        []
                end,
    %% Broker is always started (TODO: figure out if it's needed)
    BrokerSpecs = [
        #{
            id => lambda_broker,
            start => {lambda_broker, start_link, [Bootstrap]},
            modules => [lambda_broker]
        }
    ],
    {ok, {SupFlags, Authority ++ BrokerSpecs}}.
