%% @doc
%% Alternative process registry, where a single Name is backed by
%%  multiple processes, performing calculations independently, to
%%  employ redundancy.
%%
%% Actual registry is distributed, and stored in "authority"
%%  processes.
%% @end
-module(lambda_registry).
-author("maximfca@gmail.com").

%% API
-export([
    start/1,
    start_link/1,
    authorities/1,
    %% alternative process registry API
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2,
    %% extended API - subscriptions for process registry changes
    publish/3,
    subscribe/2,
    unsubscribe/2
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("kernel/include/logger.hrl").

%% Process location (similar to emgr_name()). Process ID (pid)
%%  designates both node name and ID. If process ID is not known,
%%  locally registered process name can be used.
-type location() :: pid() | {atom(), node()}.

%% Points: maps location to the address
-type points() :: #{location() => lambda_epmd:address()}.

-export_type([points/0]).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server outside of supervision hierarchy.
%% Useful for testing in conjunction with 'peer'. Unlike start_link,
%%  throws immediately, without the need to unwrap the {ok, Pid} tuple.
%% Does not register name locally (testing it is!)
-spec start(points()) -> gen:start_ret().
start(Bootstrap) ->
    {ok, Pid} = gen_server:start(?MODULE, Bootstrap, []),
    Pid.

%% @doc
%% Starts the server and links it to calling process.
-spec start_link(points()) -> gen:start_ret().
start_link(Bootstrap) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Bootstrap, []).

%% @doc returns list of authorities known to this registry
-spec authorities(gen:emgr_name()) -> [pid()].
authorities(Registry) ->
    gen_server:call(Registry, authorities).

%%--------------------------------------------------------------------
%% Alternative process registry API

-type name() :: term().

%% @doc Registers Proc to have Name, and maintains this registration
%%  on all connected authorities.
%% Proc is monitored, and if termination is detected, Proc
%%  is automatically unregistered.
%% Proc must be local.
-spec register_name(Name, Pid) -> 'yes' | 'no' when
    Name :: name(),
    Pid :: pid().
register_name(Name, Proc) ->
    publish(?MODULE, Name, Proc).

%% @doc Unregisters locally running process that was previously
%%  registered under Name.
-spec unregister_name(Name) -> _ when
    Name :: name().
unregister_name(Proc) ->
    gen_server:call(?MODULE, {unregister_name, Proc}).

%% @doc Returns a process that is registered as Name.
%%  Prefers local process, if any, otherwise picks
%%  random remote process.
%% Does not subscribe (for subscription-based interface,
%%  use extended API provided).
-spec whereis_name(Name) -> pid() | 'undefined' when
    Name :: name().
whereis_name(Name) ->
    whereis(Name).

%% @doc Sends an Msg to process selected with whereis_name(Name),
%%  and returns this process ID.
-spec send(Name, Msg) -> Pid when
    Name :: name(),
    Msg :: term(),
    Pid :: pid().
send(Name, Msg) ->
    Pid = whereis_name(Name),
    Pid ! Msg,
    Pid.

%%--------------------------------------------------------------------
%% Extended API

%% @doc Publishes Name => Proc association to Srv, triggering automatic
%%      distribution to all subscribed processes in the cluster.
publish(Srv, Name, Proc) ->
    gen_server:call(Srv, {publish, Name, Proc}).

%% @doc Subscribes Proc to updates from Name, returning already
%%      known brokers serving the name.
%%      When a list of brokers in Name changes, authorities will send
%%      an update which will be channeled to Proc (once).
-spec subscribe(gen:emgr_name(), name()) -> [pid()].
subscribe(Srv, Name) ->
    %% Pass self() explicitly to allow tricks with proxy-ing gen_server calls
    gen_server:call(Srv, {subscribe, Name, self()}).

%% @doc Unsubscribes Proc from updates. Currently a single process
%%  can have only one subscription (one name subscribed to).
-spec unsubscribe(gen:emgr_name(), pid()) -> ok | not_subscribed.
unsubscribe(Srv, Proc) ->
    gen_server:call(Srv, {unsubscribe, Proc}).

%%--------------------------------------------------------------------
%% Implementation (gen_server)

-record(lambda_registry_state, {
    %% self address
    self :: lambda_epmd:address(),
    %% authority processes + authority addresses to peer discovery
    authority = #{} :: #{pid() => lambda_epmd:address()},
    %% local names registered globally. Bi-map. Only one broker is
    %%  expected to be registered with a specific name.
    published = #{} :: #{name() => pid()},
    %% map pid to name (for monitoring)
    local = #{} :: #{pid() => {name(), reference()}},
    %% subscriptions for global updates (bi-map would fit better)
    subscriptions = #{} :: #{name() => {pid(), reference()}}
}).

-type state() :: #lambda_registry_state{}.

-spec init(points()) -> {ok, state()}.
init(Bootstrap) ->
    %% bootstrap discovery: attempt to find authorities
    Self = lambda_epmd:get_node(node()),
    %% initial discovery
    discover(Bootstrap, Self),
    {ok, #lambda_registry_state{self = Self}}.

handle_call({publish, Name, Proc}, _From, #lambda_registry_state{local = Local, published = Published} = State)
    when is_map_key(Proc, Local), is_map_key(Name, Published) ->
    %% process can't be registered under many names, and a name can't be taken by more than 1 process
    {reply, no, State};
handle_call({publish, Name, Proc}, _From, #lambda_registry_state{authority = Authority, local = Local, published = Published} = State) ->
    MRef = erlang:monitor(process, Proc),
    %% just fan-out to all authorities, and remember the local mapping
    broadcast(Authority, {publish, Name, Proc}),
    {reply, yes, State#lambda_registry_state{local = Local#{Proc => {Name, MRef}}, published = Published#{Name => [Proc]}}};

handle_call({unpublish, Proc}, _From, #lambda_registry_state{authority = Authority, local = Local, published = Published} = State) ->
    case maps:take(Proc, Local) of
        {{Name, MRef}, NewLocal} ->
            %% same as if Proc is going down, but needs demonitor
            erlang:demonitor(MRef, [flush]),
            broadcast(Authority, {unpublish, Name, Proc}),
            {reply, ok, State#lambda_registry_state{local = NewLocal, published = maps:remove(Name, Published)}};
        error ->
            %% some race condition?
            {reply, ok, State}
    end;

%% Advanced process registry capable of notifying when the global name registration changed.
handle_call({subscribe, _Name, Proc}, _From, #lambda_registry_state{subscriptions = Remote} = State) when is_map_key(Proc, Remote) ->
    {reply, already_subscribed, State};
handle_call({subscribe, Name, Proc}, _From, #lambda_registry_state{authority = Authority, subscriptions = Remote, published = Published} = State) ->
    MRef = erlang:monitor(process, Proc),
    broadcast(Authority, {subscribe, Name, Proc}),
    Brokers = maps:get(Name, Published, []), %% TODO: implement global sync/monitoring
    {reply, Brokers, State#lambda_registry_state{subscriptions = Remote#{Proc => {Name, MRef}}}};

handle_call({unsubscribe, Proc}, _From, #lambda_registry_state{subscriptions = Remote} = State) ->
    case maps:take(Proc, Remote) of
        {{_Scope, MRef}, NewRemote} ->
            %% same as if Proc is going down, but also demonitor
            erlang:demonitor(MRef, [flush]),
            {reply, ok, State#lambda_registry_state{subscriptions = NewRemote}};
        error ->
            {reply, not_subscribed, State}
    end;

handle_call(authorities, _From, #lambda_registry_state{authority = Auth} = State) ->
    {reply, maps:keys(Auth), State}.

handle_cast(_Cast, _State) ->
    error(badarg).

%% authority discovered
handle_info({authority, NewAuth, AuthAddr, MoreAuth}, #lambda_registry_state{self = Self, authority = Auth} = State)
    when not is_map_key(NewAuth, Auth) ->
    _MRef = erlang:monitor(process, NewAuth),
    discover(MoreAuth, Self),
    {noreply, State#lambda_registry_state{authority = Auth#{NewAuth => AuthAddr}}};

%% subscription update: an authority sent an update about a Name registration.
%% It can be passed to a process listening for these updates, but the decision
%%  requires a quorum.
handle_info({update, _Update}, #lambda_registry_state{authority = _Authority} = State) ->
    {noreply, State#lambda_registry_state{}};

%% something went down: authority, local registered name, or locally subscribed process
%% It can change the quorum and trigger some Name updates.
handle_info({'DOWN', _Mref, _, _Pid, _Reason}, #lambda_registry_state{} = State) ->
    {noreply, State};

%% Peer discover attempt: another client tries to discover authority, but
%%  hits the client instead. Send a list of known authorities to the client.
handle_info({discover, Peer, _Port}, #lambda_registry_state{authority = Authority} = State) when is_pid(Peer) ->
    Peer ! {authority, maps:values(Authority)},
    {noreply, State}.


%%--------------------------------------------------------------------
%% Internal implementation

discover(Points, Self) ->
    maps:map(
        fun (Location, Addr) ->
            ?LOG_DEBUG("~s discovering ~p of ~p", [node(), Location, Addr]),
            lambda_epmd:set_node(Location, Addr),
            Location ! {discover, self(), Self}
        end, Points).

broadcast(Authority, Scopes) ->
    [Auth ! {publish, Scopes} || Auth <- maps:keys(Authority)].
