%% @doc
%% Lambda bootstrap, implementing a number of default strategies,
%%  or a custom callback to fetch bootstrap information. Bootstrap
%%  may block, but it should never block authority or broker.
%% Bootstrap communications are asynchronous: a process that needs
%%  bootstrapping subscribes to bootstrap process, and receives
%%  updates.
%% When there are no subscriptions, boostrap process does not attempt
%%  to do anything. However, if there is any active subscription,
%%  bootstrap will retry bootstrap sequence until there are no more
%%  subscriptions left.
%% @end
-module(lambda_bootstrap).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/1,
    subscribe/0,
    subscribe/2,
    unsubscribe/0,
    unsubscribe/1,
    bootstrap/1
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).


%%--------------------------------------------------------------------

-type bootspec() ::
    lambda:points() |           %% map of {process, node} => {ip, port}:
    {udp, inet:port()} |        %% UDP broadcast to port: {udp, 8087}
    {dns, string()} |           %% DNS resolver
    {epmd, node()} |            %% epmd + node: {epmd, 'lambda@localhost'}
    {epmd, inet:hostname()} |   %% epmd: all nodes of a host: {epmd, 'localhost'}
    {custom, module(), atom(), [term()]}. %% callback function

%% @doc
%% Starts the server and links it to calling process.
-spec start_link(bootspec()) -> gen:start_ret().
start_link(Bootspec) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Bootspec, []).

%%--------------------------------------------------------------------
%% API

%% @doc Starts bootstrap subscription for default server, using 'once'
%%      delivery mode, when subscription is removed as soon as any
%%      bootstrap is resolved successfully.
-spec subscribe() -> ok.
subscribe() ->
    subscribe(?MODULE, once).

-spec subscribe(gen:emgr_name(), Once :: boolean()) -> lambda_broker:points().
subscribe(Dest, Once) ->
    gen_server:cast(Dest, {subscribe, self(), Once}).

%% @doc Unsubscribes process from bootstrap resolver
-spec unsubscribe() -> lambda_broker:points().
unsubscribe() ->
    unsubscribe(?MODULE).

-spec unsubscribe(gen:emgr_name()) -> ok.
unsubscribe(Dest) ->
    gen_server:cast(Dest, {unsubscribe, self()}).

%% @doc Helper function for initial startup purposes.
%%      Returns already known bootstrap, or blocks until
%%      there is at least a single successful attempt.
%%      If boot spec is empty, then the node is the root authority,
%%      and bootstrap will return an empty map as well.
bootstrap(Dest) ->
    gen_server:call(Dest, bootstrap).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_bootstrap_state, {
    timer :: undefined | reference(),
    %% subscribers: temporary (once), permanent or boot-only (reference)
    subscribers = #{} :: #{pid() => boolean() | reference()},
    %% last resolved bootstrap
    bootstrap = #{} :: lambda_broker:points(),
    %% boot spec remembered
    spec :: bootspec()
}).

-type state() :: #lambda_bootstrap_state{}.

%% Attempt to resolve every 10 seconds, if there are subscribers.
-define (LAMBDA_BOOTSTRAP_RETRY_TIME, 10000).

-spec init(bootspec()) -> {ok, state()}.
init(Bootspec) ->
    {ok, resolve(#lambda_bootstrap_state{spec = Bootspec})}.

handle_call(bootstrap, _From, #lambda_bootstrap_state{bootstrap = Boot} = State) when Boot =/= #{} ->
    {reply, Boot, State};
handle_call(bootstrap, {From, CRef}, #lambda_bootstrap_state{subscribers = Subs} = State) ->
    {noreply, State#lambda_bootstrap_state{subscribers = Subs#{From => CRef}}}.

handle_cast({subscribe, From, Once}, #lambda_bootstrap_state{timer = undefined}) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_RETRY_TIME, self(), resolve),
    erlang:monitor(process, From),
    {noreply, #lambda_bootstrap_state{timer = TRef, subscribers = #{From => Once}}};
handle_cast({subscribe, From, Once}, #lambda_bootstrap_state{subscribers = Subs} = State) ->
    is_map_key(From, Subs) orelse erlang:monitor(process, From),
    {noreply, State#lambda_bootstrap_state{subscribers = Subs#{From => Once}}};

handle_cast({unsubscribe, From}, State) ->
    {noreply, cancel(From, State)}.

%% handling bootstrapping retries
handle_info(resolve, State) ->
    {noreply, resolve(State)};

%% subscriber down
handle_info({'DOWN', _Mref, process, Pid, _Reason}, State) ->
    {noreply, cancel(Pid, State)}.

%%--------------------------------------------------------------------
%% Internal implementation

cancel(Pid, #lambda_bootstrap_state{subscribers = Subs} = State) ->
    case maps:remove(Pid, Subs) of
        None when None =:= #{} ->
            erlang:cancel_timer(State#lambda_bootstrap_state.timer),
            #lambda_bootstrap_state{};
        Remaining ->
            State#lambda_bootstrap_state{subscribers = Remaining}
    end.

resolve(#lambda_bootstrap_state{spec = {epmd, Node}}) when is_atom(Node) ->
    error(notsup);
resolve(#lambda_bootstrap_state{spec = {epmd, Hostname}}) when is_list(Hostname) ->
    error(notsup);
resolve(#lambda_bootstrap_state{spec = BootMap} = State) when BootMap =:= #{} ->
    reschedule(State#lambda_bootstrap_state{bootstrap = BootMap});
resolve(#lambda_bootstrap_state{spec = BootMap} = State) when is_map(BootMap) ->
    reschedule(State#lambda_bootstrap_state{bootstrap = BootMap}).

reschedule(State) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_RETRY_TIME, self(), bootstrap),
    State#lambda_bootstrap_state{timer = TRef}.
