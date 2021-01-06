%% @doc
%%  Lambda: top-level supervisor.
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
    %% authority is not enabled by default
    Authority = application:get_env(App, authority, false),
    %% broker is enabled by default
    Broker = application:get_env(App, broker, true),
    %% initial (static) bootstrap
    Boot = application:get_env(App, bootstrap, #{}),
    %% Authority, when enabled
    AuthoritySpec = [
        #{
            id => lambda_authority,
            start => {lambda_authority, start_link, [Boot]},
            modules => [lambda_authority]
        } || true <- [Authority]],
    %% If authority is local to the node, broker should be able to use it
    %%  by default.
    BrokerBoot = if Boot =:= #{} andalso Authority -> #{{lambda_authority, node()} => not_distributed}; true -> Boot end,
    %% Broker, when enabled
    BrokerSpec = [
        #{
            id => lambda_broker,
            start => {lambda_broker, start_link, [BrokerBoot]},
            modules => [lambda_broker]
        } || true <- [Broker]],
    %% Dynamic bootstrap updates (sent to authority & broker, found by registered
    %%  process names)
    DynBoot = application:get_env(App, bootspec, undefined),
    BootSpec = [
        #{
            id => lambda_bootstrap,
            start => {lambda_bootstrap, start_link, [Boot]},
            modules => [lambda_bootstrap]
        } || Dyn <- [DynBoot], Dyn =/= undefined],
    %% Lambda discovery (epmd module, may be already used/installed via command line)
    DiscoSpec = [
        #{
            id => lambda_discovery,
            start => {lambda_discovery, start_link, []},
            modules => [lambda_discovery]
        }
    ],
    {ok, {SupFlags, DiscoSpec ++ AuthoritySpec ++ BrokerSpec ++ BootSpec}}.
