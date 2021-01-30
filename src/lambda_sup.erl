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
    %% just in case: lambda may run under a different supervision tree
    App = case application:get_application() of {ok, A} -> A; undefined -> lambda end,
    %% authority is not enabled by default
    Authority = application:get_env(App, authority, false),
    %% broker is enabled by default
    Broker = application:get_env(App, broker, true),
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
            start => {lambda_authority, start_link, []},
            modules => [lambda_authority]
        } || true <- [Authority]],
    %% Broker, when enabled
    BrokerSpec = [
        #{
            id => lambda_broker,
            start => {lambda_broker, start_link, []},
            modules => [lambda_broker]
        } || true <- [Broker]],
    %% Bootstrap subscribers (authority & broker, if enabled)
    Subscribers = [lambda_authority || true <- [Authority]] ++ [lambda_broker || true <- [Broker]],
    %% Bootstrap processes supervised by Lambda
    %% By default, if authority is running, bootstrap points to it, otherwise,
    %%  bootstrap points to 'authority' node at local host.
    DefaultBoot = if Authority -> node();
                      true ->
                          {ok, Host} = inet:gethostname(),
                          list_to_atom(lists:concat(["authority", "@", Host, domain(net_kernel:longnames())]))
                  end,
    DynBoot = application:get_env(App, bootspec, [{static, [DefaultBoot]}]),
    BootSpec = [
        #{
            id => lambda_bootstrap,
            start => {lambda_bootstrap, start_link, [Subscribers, Dyn]},
            modules => [lambda_bootstrap]
        } || Dyn <- DynBoot],

    %% Supervisors for statically published listener/plb modules
    %% take children list from configuration (hosted deployment)
    Publish = [check_server_mod(Mod) || Mod <- application:get_env(App, publish, [])],
    Discover = [check_plb_mod(Mod) || Mod <- application:get_env(App, discover, [])],
    ModSup = [
        #{
            id => lambda_listener_sup,
            start => {lambda_listener_sup, start_link, [Publish]},
            type => supervisor,
            modules => [lambda_listener_sup]
        },
        #{
            id => lambda_client_sup,
            start => {lambda_client_sup, start_link, [Discover]},
            type => supervisor,
            modules => [lambda_client_sup]
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

domain(true) ->
    domain(inet_db:res_option(domain));
domain(ignored) ->
    domain(inet_db:res_option(domain));
domain(false) ->
    "";
domain([]) ->
    "";
domain(Domain) ->
    [$. | Domain].
