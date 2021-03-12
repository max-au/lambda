%% @doc
%%  Suspending scheduler implementation.
%%  Enables build systems with dynamic dependencies.
%%  Concurrent startup for multiple applications can be viewed
%%      as "building" set of targets.
%%
%%  Current implementation does not limit concurrency, relying on
%%      Erlang scheduling to do the job.
%%
%%  Usage examples:
%%
%%  1. Building a target, given resolver function:
%%      Resolver = fun (_Sched, root) -> 42 end,
%%      lambda_scheduler:build(Resolver, root).
%%
%%  2. Building a target with dependencies (note dependencies must be a list):
%%      Resolver = fun (Sched, root) ->
%%                         #{dep := {ok, DepVal}} = lambda_scheduler:require(Sched, root, [dep]),
%%                         DepVal + 10;
%%                     (_Sched, dep) ->
%%                          42
%%                     end,
%%      lambda_scheduler:build(Resolver, root).
%%
%%
%% @end
-module(lambda_scheduler).

-export([
    request/2,
    request/3,
    multi_request/2,
    multi_request/3,
    await/2,
    multi_await/2,
    require/2,
    require/3,
    build/2,
    targets/1,
    value/2,
    depends/2,
    provides/2,
    set_resolver/2
]).

-export([
    start_link/1,
    start_link/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-behaviour(gen_server).

%%--------------------------------------------------------------------
%% API

%% Scheduler process
-type dest() :: lambda:dst().

%% Target: what to schedule.
%% 'undefined' atom has a special meaning (root)
-type target() :: term().

%% Result. If resolver returned some term(), {ok, ...} is cached,
%%  and if resolver throws, {Class, Reason} is cached (stack is discarded).
%% Contains 'undefined' if dependency value is not yet known.
-type value() :: {ok, Value :: term()} | {error, Reason :: term()}.

%% Resolver function: "build a Target given a Scheduler".
%%  Returns a value that is cached as result.
%% Resolver is responsible for checking if dependencies were built
%%  correctly.
-type resolver() :: fun((Scheduler :: dest(), Target :: target()) -> term()).

%% @doc Request a new Dependency, while resolving Target.
%%  Returns value() if dependency is already built (not necessarily successfully);
%%   {started, Pid} if dependency is started calculation;
%%   and {error, {cycle, [...]}} if this dependency forms
%%   a cycle.
%%  Requesting a dependency is asynchronous. It is possible
%%   to request many discovered dependencies without waiting
%%   for any of these to be satisfied, and then wait using
%%   `multi_await/2'.
%%  Use `require' to combine request and await in a single call.
-spec request(dest(), target(), target()) ->
    value() | {started, pid()} | {error, {cycle, [target()]}}.
request(Dest, Target, Dependency) ->
    case gen_server:call(Dest, {request, Target, [Dependency]}) of
        {error, Reason} ->
            Reason;
        [Success] ->
            Success
    end.

%% @doc Request a new top-level dependency (not resolving any
%%      other target).
-spec request(dest(), target()) ->
    value() | {started, pid()} | {error, {cycle, [target()]}}.
request(Dest, Dependency) ->
    request(Dest, undefined, Dependency).

%% @doc Requests multiple dependencies at once.
%%      Returns a list (in the same as input order), or error if
%%      any dependency forms a cycle.
-spec multi_request(dest(), target(), [target()]) ->
    [value() | {started, pid()}] | {error, {cycle, [target()]}}.
multi_request(Dest, Target, Dependencies) ->
    gen_server:call(Dest, {request, Target, Dependencies}).

%% @doc Request multiple top-level dependencies at once.
-spec multi_request(dest(), [target()]) ->
    [value() | {started, pid()}] | {error, {cycle, [target()]}}.
multi_request(Dest, Dependencies) ->
    multi_request(Dest, undefined, Dependencies).

%% @doc if Target was built before, returns value().
%%  If target has never been request()-ed, returns {error, not_started}.
%%  Otherwise blocks and waits until requested dependency is built,
%%  then returns value().
-spec await(dest(), target()) -> value() | {error, not_started}.
await(Dest, Target) ->
    gen_server:call(Dest, {await, Target}, infinity).

%% @doc awaits for multiple Targets, and returns map of
%%      results, with the same meaning as `await/2'.
-spec multi_await(dest(), [target()]) -> #{target() => value()}.
multi_await(Dest, Targets) ->
    await_many(Dest, Targets, [{started, undefined} || _T <- Targets], #{}).

%% @doc Requests and await Dependencies to continue
%%  building the Target. Returns values() of built dependencies,
%%  or error if any dependency forms a cycle.
-spec require(dest(), target(), [target()]) -> #{target() => value()} | {error, {cycle, [target()]}}.
require(Dest, Target, Dependencies) when is_list(Dependencies) ->
    case multi_request(Dest, Target, Dependencies) of
        {error, Reason} ->
            {error, Reason};
        Success ->
            await_many(Dest, Dependencies, Success, #{})
    end.

%% @doc Requests and await top-level Targets build result.
-spec require(dest(), [target()]) -> #{target() => value()} | {error, {cycle, [target()]}}.
require(Dest, Targets) when is_list(Targets) ->
    require(Dest, undefined, Targets).

%% @doc Runs build system, given resolver callback and
%%  a single Target to build.
-spec build(resolver(), target()) -> value().
build(Resolver, Target) when is_function(Resolver, 2) ->
    {ok, Scheduler} = start_link(Resolver),
    try request(Scheduler, Target) of
        {started, _} ->
            await(Scheduler, Target);
        Other ->
            Other
    after
        gen_server:stop(Scheduler)
    end.

%% @doc Returns list of all known targets.
-spec targets(dest()) -> [target()].
targets(Dest) ->
    gen_server:call(Dest, targets).

%% @doc Returns current value for a Target.
-spec value(dest(), target()) -> value() | not_found.
value(Dest, Target) ->
    gen_server:call(Dest, {target, Target}).

%% @doc Returns list of dependencies of a specific Target.
-spec depends(dest(), target()) -> [target()] | not_found.
depends(Dest, Target) ->
    gen_server:call(Dest, {depends, Target}).

%% @doc Returns list of targets that depend on the specified Target.
-spec provides(dest(), target()) -> [target()] | not_found.
provides(Dest, Target) ->
    gen_server:call(Dest, {provides, Target}).

%% @doc Replaces build callback. May be used to re-run build
%%  process, or reverse build.
-spec set_resolver(dest(), resolver()) -> ok.
set_resolver(Dest, Resolver) ->
    gen_server:call(Dest, {set_resolver, Resolver}).

%% @doc Starts build system with specified resolver, and registers
%%      builder process.
-spec start_link(lambda:dst(), resolver()) -> {ok, dest()}.
start_link(Name, Resolver) when is_function(Resolver, 2) ->
    gen_server:start_link(Name, ?MODULE, [Resolver], []).

%% @doc Starts build system with specified resolver.
-spec start_link(resolver()) -> {ok, dest()}.
start_link(Resolver) when is_function(Resolver, 2) ->
    gen_server:start_link(?MODULE, [Resolver], []).

%%--------------------------------------------------------------------
%% Implementation details

-record(target_state, {
    %% build vector clock
    built :: undefined | integer(),
    %% last build result
    result :: undefined | value(),
    %% dependencies with values
    depends = #{} :: #{target() => value()},
    %% targets that depend on this
    provides = [] :: [target()],
    %% worker currently running this target
    worker = undefined :: undefined | pid(),
    %% waiting list (to be notified when target is built)
    waiting = [] :: [{{pid(), reference()}, target()}]
}).
-type target_state() :: #target_state{}.

-record(lambda_scheduler_state, {
    %% epoch: when the server was started (vector clock, counter)
    epoch :: integer(),
    %% resolver callback
    resolver :: resolver(),
    %% targets map
    store = #{} :: #{target() => target_state()},
    %% scheduled processes
    workers = #{} :: #{pid() => target()}
}).

init([Resolver]) ->
    %% need to trap exits, to save what can be saved, and also receive
    %%  'EXIT' signals from workers.
    false = process_flag(trap_exit, true),
    {ok, #lambda_scheduler_state{epoch = erlang:system_time(millisecond), resolver = Resolver}}.

handle_call({request, Target, Deps}, _From, #lambda_scheduler_state{epoch = Epoch, store = Store0} = State) ->
    try
        NewStore = store_dependencies(Store0, Epoch, Target, Deps),
        %% NewStore contains all Targets with Dependencies added, so
        %%  now we only need to start build for those dependencies
        %%  that were not yet built.
        {Reply, NewState} = lists:foldl(
            fun (Dep, {ReplyAcc, #lambda_scheduler_state{store = StoreAcc} = StateAcc}) ->
                case maps:find(Dep, StoreAcc) of
                    {ok, #target_state{built = Epoch, result = Value}} ->
                        %% already completed this run
                        {[Value | ReplyAcc], StateAcc};
                    {ok, #target_state{worker = undefined} = Task} ->
                        %% has not been run yet, no-one waits on the target
                        start_task(Dep, Task, ReplyAcc, StateAcc);
                    {ok, #target_state{worker = Pid}} ->
                        %% don't check for circular dependency, we did it before
                        {[{started, Pid} | ReplyAcc], StateAcc};
                    error ->
                        %% target is not known, let's try to build it
                        start_task(Dep, #target_state{}, ReplyAcc, StateAcc)
                end
            end, {[], State#lambda_scheduler_state{store = NewStore}}, Deps),
        {reply, lists:reverse(Reply), NewState}
    catch
        throw:{cycle, Cycle} ->
            {reply, {error, {cycle, Cycle}}, State}
    end;

%% await: request from For, wait until Target is done
handle_call({await, Target}, From, #lambda_scheduler_state{epoch = Epoch, store = Store} = State) ->
    %% check whether built was complete
    case maps:find(Target, Store) of
        {ok, #target_state{built = Epoch, result = Value}} ->
            %% already completed this run
            {reply, Value, State};
        {ok, #target_state{worker = undefined}} ->
            {reply, {error, not_started}, State};
        {ok, #target_state{waiting = Waiting} = Task} ->
            %% need to wait until this one completes
            NewTask = Task#target_state{waiting = [From | Waiting]},
            {noreply, State#lambda_scheduler_state{store = Store#{Target => NewTask}}};
        error ->
            {reply, {error, not_started}, State}
    end;

handle_call(targets, _From, #lambda_scheduler_state{store = Store} = State) ->
    {reply, maps:keys(Store), State};

handle_call({target, Target}, _From, #lambda_scheduler_state{store = Store} = State) ->
    case maps:find(Target, Store) of
        {ok, #target_state{result = Res}} ->
            {reply, Res, State};
        error ->
            {reply, not_found, State}
    end;

handle_call({depends, Target}, _From, #lambda_scheduler_state{store = Store} = State) ->
    case maps:find(Target, Store) of
        {ok, #target_state{depends = Deps}} ->
            {reply, maps:keys(Deps), State};
        error ->
            {reply, not_found, State}
    end;

handle_call({provides, Target}, _From, #lambda_scheduler_state{store = Store} = State) ->
    case maps:find(Target, Store) of
        {ok, #target_state{provides = Provs}} ->
            {reply, Provs, State};
        error ->
            {reply, not_found, State}
    end;

handle_call({set_resolver, Resolver}, _From, State) ->
    {reply, ok, State#lambda_scheduler_state{resolver = Resolver, epoch = erlang:system_time(millisecond)}}.

%% Built: some work was done and new dependencies recorded
handle_cast({done, Worker, Value}, #lambda_scheduler_state{epoch = Epoch, workers = Workers, store = Store} = State)
    when is_map_key(Worker, Workers) ->
    Target = maps:get(Worker, Workers),
    %% respond to all currently waiting
    {noreply, State#lambda_scheduler_state{store = complete_target(Store, Target, Epoch, {ok, Value})}}.

%% Handle worker exits
handle_info({'EXIT', Worker, Reason}, #lambda_scheduler_state{epoch = Epoch,
    workers = Workers, store = Store} = State) when is_map_key(Worker, Workers) ->
    {Target, NewWorkers} = maps:take(Worker, Workers),
    case Reason of
        normal ->
            OldTarget = maps:get(Target, Store),
            NewTarget = OldTarget#target_state{worker = undefined},
            {noreply, State#lambda_scheduler_state{workers = NewWorkers, store = Store#{Target => NewTarget}}};
        {Exit, _Stack} ->
            %% for non-normal reason, respond to waiting processes
            {noreply, State#lambda_scheduler_state{store = complete_target(Store, Target, Epoch, {error, Exit})}}
    end.

%%--------------------------------------------------------------------
%% Internal implementation

complete_target(Store, Target, Epoch, Result) ->
    OldTarget = maps:get(Target, Store),
    [gen:reply(To, Result) || To <- OldTarget#target_state.waiting],
    NewStore = lists:foldl(
        fun (For, StoreAcc) ->
            maps:update_with(For,
                fun (#target_state{depends = OldDeps} = TS) ->
                    TS#target_state{depends = OldDeps#{Target => Result}}
                end, StoreAcc)
        end, Store, OldTarget#target_state.provides),
    %% update the store with new target value
    NewTarget = OldTarget#target_state{waiting = [], built = Epoch, result = Result},
    NewStore#{Target => NewTarget}.

%% Awaits for dependencies in the list that need to be waited for.
-spec await_many(dest(), [target()],
    [value() | {started, pid()}], #{target() => value()}) -> #{target() => value()}.
await_many(_Dest, [], [], Acc) ->
    Acc;
await_many(Dest, [Target | TT], [{Done, Value} | VT], Acc) when Done =:= ok; Done =:= error ->
    await_many(Dest, TT, VT, Acc#{Target => {Done, Value}});
await_many(Dest, [Target | TT], [{started, _Pid} | VT], Acc) ->
    await_many(Dest, TT, VT, Acc#{Target => await(Dest, Target)}).

start_task(Dep, Task, ReplyAcc, #lambda_scheduler_state{resolver = Resolver, store = Store, workers = Workers} = State) ->
    Control = self(),
    Pid = proc_lib:spawn_link(
        fun () ->
            %% collect dependencies (for subsequent runs)
            Result = Resolver(Control, Dep),
            gen_server:cast(Control, {done, self(), Result})
        end),
    %%
    State1 = State#lambda_scheduler_state{
        store = Store#{Dep => Task#target_state{worker = Pid}},
        workers = Workers#{Pid => Dep}},
    %%
    {[{started, Pid} | ReplyAcc], State1}.


%% DFS to find a cycle in the graph.
%% Visited: contains "true" for already checked vertices,
%%  or a number - index in the stack.
ensure_no_cycle(DAG, Vertex, Visited) ->
    Visited1 = Visited#{Vertex => map_size(Visited)},
    case maps:find(Vertex, DAG) of
        {ok, #target_state{depends = Deps}} ->
            WithDeps = lists:foldl(
                fun (Next, Vis) ->
                    case maps:find(Next, Vis) of
                        {ok, true} ->
                            Vis;
                        {ok, _Index} ->
                            {Stack, _} = lists:unzip(lists:keysort(2,
                                [{V, N} || {V, N} <- maps:to_list(Visited), is_integer(N)])),
                            Cycle = Stack ++ [Vertex, Next],
                            throw({cycle, Cycle});
                        error ->
                            ensure_no_cycle(DAG, Next, Vis)
                    end
                end, Visited1, maps:keys(Deps)),
            WithDeps#{Vertex => true};
        error ->
            Visited#{Vertex => true}
    end.

store_dependencies(Store, _Epoch, _Target, []) ->
    Store;
store_dependencies(Store, _Epoch, undefined, _Deps) ->
    Store;
store_dependencies(Store, Epoch, Target, [Dep | Deps]) ->
    %% add new dependency to the Target in the Store
    Store1 = maps:update_with(Target,
        fun (#target_state{depends = OldDeps} = Task) ->
            Task#target_state{depends = OldDeps#{Dep => undefined}}
        end, #target_state{depends = #{Dep => undefined}}, Store),
    %% add reverse dependency
    Store2 = maps:update_with(Dep,
        fun (#target_state{provides = OldProv} = Task) ->
            Task#target_state{provides = [Target | OldProv]}
        end, #target_state{provides = [Target]}, Store1),
    %%
    %% check for circular dependencies
    case maps:find(Dep, Store) of
        {ok, #target_state{worker = Pid}} when is_pid(Pid) ->
            %% already running, check for circular dependency
            ensure_no_cycle(Store2, Target, #{}),
            store_dependencies(Store2, Epoch, Target, Deps);
        _ ->
            %% either Dep is not known, or
            %%  it is not running, or it was built before
            store_dependencies(Store2, Epoch, Target, Deps)
    end.
