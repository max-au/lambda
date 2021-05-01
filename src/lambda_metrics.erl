%% @doc
%% DO NOT USE: it is expected to implement a hardcoded counters indices
%%  specifically for lambda purposes, that do not clash with any other
%%  counters. Later down the road it will be either replaced, or reported
%%  to some existing library.
%%
%% Intentionally oversimplified approach to pushing out simple metrics:
%%  * counters
%%  * gauges
%%
%% Server implements a timer that sends data periodically, or when it is
%%  about to terminate.
%% @end
-module(lambda_metrics).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/0,
    register_count/2,
    count/1,
    get_count/0,
    get_count/1
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-type counter_name() :: atom() | [atom()].

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% API

%% Persistent term storage
-define (COUNT_STORAGE, {?MODULE, count}).
-define (GAUGE_STORAGE, {?MODULE, gauge}).

%% Current counters limit
-define (MAX_COUNTERS, 1024).

%% @doc
%% Starts the server and links it to calling process.
-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, dirty, []).

%% @doc
%% Registers a new counter with requested index, for a specific term (name).
%% It is an error to register the same index for a different term.
-spec register_count(Idx :: pos_integer(), Name :: counter_name()) -> ok | {registered, counter_name()}.
register_count(Idx, _Name) when Idx < 0; Idx > ?MAX_COUNTERS ->
    error(invalid_index);
register_count(Idx, Name) ->
    gen_server:call(?MODULE, {register, Idx, Name}).

%% @doc
%% Bumps counter with the specified index.
-spec count(Idx :: pos_integer()) -> ok.
count(Idx) ->
    atomics:add(persistent_term:get(?COUNT_STORAGE), Idx, 1).

%% @doc Returns all counters and their names
-spec get_count() -> [{counter_name(), integer()}].
get_count() ->
    Names = gen_server:call(?MODULE, get),
    [{Name, atomics:get(persistent_term:get(?COUNT_STORAGE), Idx)} || {Idx, Name} <- maps:to_list(Names)].

%% @doc Returns specific counter
-spec get_count(Idx :: pos_integer()) -> integer().
get_count(Idx) ->
    atomics:get(persistent_term:get(?COUNT_STORAGE), Idx).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_metrics_state, {
    %% registered counter names
    registered = #{} :: #{pos_integer() => counter_name()},
    %% time when last collection happened
    last_collection = erlang:monotonic_time(millisecond) :: integer()
}).

-type state() :: #lambda_metrics_state{}.

%% Collect data once a minute
-define (COLLECT_INTERVAL, 60000).

init(dirty) ->
    %% If somehow this gen_server dies and restarts, existing counters
    %%  are still retained in persistent_term supported storage
    try persistent_term:get(?COUNT_STORAGE),
        false = process_flag(trap_exit, true),
        {ok, schedule_collect(#lambda_metrics_state{})}
    catch
        error:badarg ->
            init(clean)
    end;
init(clean) ->
    Storage = atomics:new(?MAX_COUNTERS, []),
    persistent_term:put(?COUNT_STORAGE, Storage),
    init(dirty).

handle_call({register, Idx, Name}, _From, #lambda_metrics_state{registered = Reg} = State) ->
    case maps:find(Idx, Reg) of
        {ok, Known} ->
            {reply, {registered, Known}, State};
        error ->
            {reply, ok, State#lambda_metrics_state{registered = Reg#{Idx => Name}}}
    end;

handle_call(get, _From, #lambda_metrics_state{registered = Reg} = State) ->
    {reply, Reg, State}.

-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Req, _State) ->
    erlang:error(notsup).

handle_info(collect, State) ->
    collect(State),
    {noreply, schedule_collect(State)}.

%%--------------------------------------------------------------------
%% Internal implementation

%% absolute timer used to ensure logging is done without skewing too much
schedule_collect(#lambda_metrics_state{last_collection = LastTick} = State) ->
    NextTick = schedule_collect(LastTick + ?COLLECT_INTERVAL, erlang:monotonic_time(millisecond), ?COLLECT_INTERVAL),
    State#lambda_metrics_state{last_collection = NextTick}.

schedule_collect(NextTick, Now, _Interval) when NextTick > Now ->
    erlang:send_after(NextTick, self(), collect, [{abs, true}]),
    NextTick;
schedule_collect(NextTick, Now, Interval) ->
    % skip collection cycle (yes it can be done in 1 step, but, really, why care?)
    SafeTick = NextTick + Interval,
    schedule_collect(SafeTick, Now, Interval).

collect(_State) ->
    Atomics = persistent_term:get(?COUNT_STORAGE),
    #{size := Size} = atomics:info(Atomics),
    List = read(Size, Atomics, []),
    io_lib:format("Counters: ~p~n", [List]).

read(0, _Ref, Acc) ->
    Acc;
read(Idx, Ref, Acc) ->
    read(Idx - 1, Ref, [atomics:get(Ref, Idx) | Acc]).
