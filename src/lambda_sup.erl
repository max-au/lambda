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
    %% Lambda discovery (epmd module, may be already used/installed via command line)
    DiscoSpec = [
        #{
            id => lambda_discovery,
            start => {lambda_discovery, start_link, []},
            modules => [lambda_discovery]
        }
    ],
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

    %% Supervisors for statically published server/plb modules
    %% take children list from configuration (hosted deployment)
    Publish = [check_server_mod(Mod) || Mod <- application:get_env(App, publish, [])],
    Discover = [check_plb_mod(Mod) || Mod <- application:get_env(App, discover, [])],
    ModSup = [
        #{
            id => lambda_server_sup,
            start => {lambda_server_sup, start_link, [Publish]},
            type => supervisor,
            modules => [lambda_server_sup]
        },
        #{
            id => lambda_plb_sup,
            start => {lambda_plb_sup, start_link, [Discover]},
            type => supervisor,
            modules => [lambda_plb_sup]
        }
    ],
    {ok, {SupFlags, lists:concat([DiscoSpec, AuthoritySpec, BrokerSpec, BootSpec, ModSup])}}.


%%--------------------------------------------------------------------
%% Internal implementation

check_server_mod(Mod) when is_atom(Mod) ->
    %% by default, set the concurrency as 2x of schedulers
    %%  available.
    Sched = erlang:system_info(schedulers),
    {Mod, #{capacity => Sched * 2}};
check_server_mod({Mod, Options}) when is_atom(Mod), is_map(Options) ->
    {Mod, Options};
check_server_mod(Other) ->
    erlang:error({invalid_module_spec, Other}).

check_plb_mod(Mod) when is_atom(Mod) ->
    %% should use dynamic (determined in runtime) as default,
    %%  but this mode is not ready yet
    {Mod, #{high => 10}};
check_plb_mod({Mod, Options}) when is_atom(Mod), is_map(Options) ->
    {Mod, Options};
check_plb_mod(Other) ->
    erlang:error({invalid_module_spec, Other}).