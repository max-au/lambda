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
    count/1
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

%%--------------------------------------------------------------------
%% API

%% Persistent term storage
-define (STORAGE, {?MODULE, storage}).

%% @doc
%% Starts the server and links it to calling process.
-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, dirty, []).

%% @doc
%% Bumps counter with the specified index.
-spec count(Idx :: pos_integer()) -> ok.
count(Idx) ->
    atomics:add(persistent_term:get(?STORAGE), Idx, 1).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_metrics_state, {
    %% time when last collection happened
    last_collection = erlang:monotonic_time(millisecond) :: integer()
}).

-type state() :: #lambda_metrics_state{}.

%% Post data once a minute
-define (POST_INTERVAL, 60_000).

%% Current counters limit
-define (MAX_COUNTERS, 1024).


init(dirty) ->
    %% If somehow this gen_server dies and restarts, existing counters
    %%  are still retained in persistent_term supported storage
    try persistent_term:get(?STORAGE),
        false = process_flag(trap_exit, true),
        {ok, schedule_post(#lambda_metrics_state{})}
    catch
        error:badarg ->
            init(clean)
    end;
init(clean) ->
    Storage = atomics:new(?MAX_COUNTERS, []),
    persistent_term:put(?STORAGE, Storage),
    init(dirty).

handle_call(_Req, _From, _State) ->
    erlang:error(notsup).

-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Req, _State) ->
    erlang:error(notsup).

handle_info(post, State) ->
    post(State),
    {noreply, schedule_post(State)}.

%%--------------------------------------------------------------------
%% Internal implementation

%% absolute timer used to ensure logging is done without skewing too much
schedule_post(#lambda_metrics_state{last_collection = LastTick} = State) ->
    NextTick = schedule_post(LastTick + ?POST_INTERVAL, erlang:monotonic_time(millisecond), ?POST_INTERVAL),
    State#lambda_metrics_state{last_collection = NextTick}.

schedule_post(NextTick, Now, _Interval) when NextTick > Now ->
    erlang:send_after(NextTick, self(), collect, [{abs, true}]),
    NextTick;
schedule_post(NextTick, Now, Interval) ->
    % skip collection cycle (yes it can be done in 1 step, but, really, why care?)
    SafeTick = NextTick + Interval,
    schedule_post(SafeTick, Now, Interval).

post(_State) ->
    Atomics = persistent_term:get(?STORAGE),
    #{size := Size} = atomics:info(Atomics),
    List = read(Size, Atomics, []),
    io:format("Counters: ~p~n", [List]).

read(0, _Ref, Acc) ->
    Acc;
read(Idx, Ref, Acc) ->
    read(Idx - 1, Ref, [atomics:get(Ref, Idx) | Acc]).
