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
    %% all children can restart independently
    %% allow 2 restarts every 10 seconds
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 10},
    %% just in case: lambda may run as included application
    App = case application:get_application() of {ok, A} -> A; undefined -> lambda end,
    %% child spec for bootstrap server
    {Authority, Boot} =
        case application:get_env(App, bootstrap) of
            {ok, Bootstrap} ->
                {application:get_env(App, authority, false), Bootstrap};
            undefined ->
                {true, #{}}
        end,
    %%
    BootSpec =
        #{
            id => lambda_bootstrap,
            start => {lambda_bootstrap, start_link, [Boot]},
            modules => [lambda_bootstrap]
        },
    AuthoritySpec =
        #{
            id => lambda_authority,
            start => {lambda_authority, start_link, [lambda_bootstrap]},
            modules => [lambda_authority]
        },
    BrokerSpec =
        #{
            id => lambda_broker,
            start => {lambda_broker, start_link, [lambda_bootstrap]},
            modules => [lambda_broker]
        },
    {ok, {SupFlags, if Authority -> [BootSpec, AuthoritySpec, BrokerSpec]; true -> [BootSpec, BrokerSpec] end}}.
