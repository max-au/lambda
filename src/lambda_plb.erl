%% @doc
%%  Probabilistic Load Balancer. Schedules function
%%  execution on a node selected with randomised weighted
%%  sampling. Used credit-based approach for implementing
%%  backpressure: when a node did not provide credit, no
%%  call/cast to that node is allowed.
%%
%%  Capacity discovery: plb subscribes to module via local
%%  broker, and issues "buy" order when needed.
%%
%% @end
-module(lambda_plb).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/3,

    cast/4,
    call/4,
    capacity/1,
    meta/1,

    %% internal API for broker
    complete_order/2
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Internal exports for testing
-export([
    take/2
]).

%% Options for this plb (module).
-type options() :: #{
    %% capacity to request from all servers combined
    capacity := non_neg_integer(),
    %% version or version range to accept
    vsn => integer(),
    %% local broker process (lambda_broker by default)
    broker => lambda:dst(),
    %% disable automatic meta query/compilation
    compile => false
}.

-export_type([options/0]).


-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Starts the server and links it to calling process.
-spec start_link(lambda:dst(), module(), options()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Broker, Module, Options) when is_atom(Module) ->
    gen_server:start_link({local, Module}, ?MODULE, {Broker, Module, Options}, []).

%%--------------------------------------------------------------------
%% API

%% @doc Starts a request on a node selected with weighted random
%%      sampling. May block if no capacity left.
%%      Returns {error, suspend} if 'nosuspend' requested, but no capacity left.
-spec cast(module(), atom(), [term()], nosuspend | timeout()) -> ok | {error, suspend}.
cast(M, F, A, Timeout) ->
    case gen_server:call(M, token, infinity) of
        {suspend, _Queue} when Timeout =:= nosuspend ->
            {error, suspend};
        {suspend, Queue} ->
            gen_server:call(Queue, wait, Timeout),
            cast(M, F, A, Timeout);
        To ->
            erpc:cast(node(To), lambda_channel, handle, [To, M, F, A])
    end.

%% @doc Call executed on a node which is selected with weighted random
%%      sampling. Returns {error, suspend} if 'nosuspend' requested, but no capacity left.
-spec call(module(), atom(), [term()], nosuspend | timeout()) -> term() | {error, suspend}.
call(M, F, A, Timeout) ->
    case gen_server:call(M, token, infinity) of
        {suspend, _Queue} when Timeout =:= nosuspend ->
            {error, suspend};
        {suspend, Queue} ->
            gen_server:call(Queue, wait, Timeout),
            call(M, F, A, Timeout);
        To ->
            Rid = erpc:send_request(node(To), lambda_channel, handle, [To, M, F, A]),
            case erpc:wait_response(Rid, infinity) of
                {response, Res} ->
                    Res;
                no_response ->
                    exit(timeout)
            end
    end.

%% @doc Returns current capacity.
-spec capacity(module()) -> non_neg_integer().
capacity(Module) ->
    gen_server:call(Module, capacity).

%% @doc Waits until PLB discovers the module, compiles proxy module and
%%      returns meta information.
-spec meta(atom() | pid()) -> module().
meta(Srv) ->
    gen_server:call(Srv, meta, infinity).

%% @doc Invoked by a broker, when order has been completed
-spec complete_order(lambda:dst(), [{pid(), Quantity :: pos_integer(), Meta :: lambda:meta()}]) -> ok.
complete_order(Srv, Servers) ->
    erlang:send(Srv, {complete_order, Servers}, []).

%%--------------------------------------------------------------------
%% Weighted random sampling: each server process has a weight.
%% Fast selection implemented with Ryabko array, also known as Fenwick
%%  trees or bit-indexed tree.
%% Since Erlang does not have a notion of mutable array, and tuples
%%  are very slow to modify, process dictionary is used for Ryabko
%%  array.
%% Array is zero-indexed for faster 'select' query. Example, in
%%  process dictionary, for an array of [13, 9, 10, 5] is:
%%   {0, 13}, {1, 22}, {2, 10}, {3, 37}
%%
%% Alternative implementation with faster insert/remove operation
%%  can be done with augmented binary tree, analogous to gb_tree,
%%  but with added left/right capacity.

-record(lambda_plb_state, {
    %% remember module name for event handler
    module :: module(),
    %% meta: meta-information about currently loaded module. When 'false',
    %%  meta is not relevant, and no automatic compilation happens,
    %%  when a list of {pid, ref} - list of processes waiting for PLB to
    %%  receive meta
    meta :: false | [{pid(), reference()}] | lambda:meta(),
    %% current capacity, needed for quick reject logic
    capacity = 0 :: non_neg_integer(),
    %% high/low capacity watermarks
    high :: pos_integer(),
    %% connected servers, maps pid to an index & expected capacity
    pid_to_index = #{} :: #{pid() => {Index :: non_neg_integer(), Capacity :: non_neg_integer()}},
    %% connected servers, maps index to pid
    %% it could be faster to use process dictionary for
    %%  this as well, but to prove it, better performance
    %%  testing framework is needed
    index_to_pid = {undefined} :: tuple(),
    %% index_to_pid tuple size can be cached, proven that
    %%  it makes performance better
    %% size = 1 :: pos_integer(),
    %% list of free indices, for amortised O(1) insertion
    free = [0] :: [non_neg_integer()],
    %% queue: process that accepts 'wait' requests
    queue :: pid(),
    %% local broker, monitored for failover purposes
    broker :: lambda:dst()
}).

-type state() :: #lambda_plb_state{}.

-spec init({lambda:dst(), Module :: atom(), Options :: options()}) -> {ok, state()}.
init({Broker, Module, #{capacity := HW} = Options}) ->
    %% initial array contains a single zero element with zero weight
    put(0, 0),
    %% monitor the broker (and reconnect if it restarts)
    erlang:monitor(process, Broker),
    ?LOG_DEBUG("requesting ~b ~s from ~p", [HW, Module, Broker], #{domain => [lambda]}),
    %% not planning to cancel the order
    OrderOpts = #{module => maps:with([vsn], Options)},
    _ = lambda_broker:buy(Broker, Module, HW, OrderOpts),
    {ok, #lambda_plb_state{module = Module,
        queue = proc_lib:spawn_link(fun queue/0),
        meta = maps:get(compile, Options, []),
        broker = Broker,
        high = HW}}.

handle_call(token, _From, #lambda_plb_state{capacity = 0, queue = Queue} = State) ->
    % ?LOG_DEBUG("~p: no capacity: sending to queue ~p", [_From, Queue], #{domain => [lambda]}),
    {reply, {suspend, Queue}, State};
handle_call(token, _From, #lambda_plb_state{capacity = Cap, index_to_pid = Itp} = State) ->
    Idx = take(rand:uniform(Cap) - 1, tuple_size(Itp)),
    To = element(Idx + 1, Itp),
    Cap =:= 1 andalso begin State#lambda_plb_state.queue ! block end,
    % ?LOG_DEBUG("~p: giving token ~p", [_From, To], #{domain => [lambda]}),
    {reply, To, State#lambda_plb_state{capacity = Cap - 1}};

handle_call(meta, From, #lambda_plb_state{meta = Waiting} = State) when is_list(Waiting) ->
    %% Do not add any code that can block here
    {noreply, State#lambda_plb_state{meta = [From | Waiting]}};
handle_call(meta, _From, #lambda_plb_state{meta = Meta} = State) ->
    {reply, Meta, State};

handle_call(capacity, _From, #lambda_plb_state{capacity = Cap} = State) ->
    {reply, Cap, State}.

handle_cast(_Cast, _State) ->
    error(badarg).

%% Handles demand from the server.
%%  If it's the first time demand, start monitoring this server.
handle_info({demand, Demand, Server}, #lambda_plb_state{pid_to_index = Pti, index_to_pid = Itp,
    capacity = Cap} = State) ->
    %%
    ServerCount = tuple_size(Itp),
    %%
    ?LOG_DEBUG("received demand (~b) from ~p", [Demand, Server], #{domain => [lambda]}),
    %% unblock the queue
    Cap =:= 0 andalso begin State#lambda_plb_state.queue ! unblock end,
    %%
    case maps:find(Server, Pti) of
        {ok, {Index, _OldDemand}} ->
            %% TODO: when old and new demands are different, start capacity search
            inc(Index, Demand, ServerCount),
            {noreply, State#lambda_plb_state{capacity = Cap + Demand}};
        error ->
            erlang:monitor(process, Server),
            case State#lambda_plb_state.free of
                [Free] ->
                    %% reached maximum size: double the array size
                    Extend = ServerCount * 2,
                    %% create a list of new free cells
                    NewFree = lists:seq(ServerCount, Extend - 1),
                    %% Weights:  1  2  3  4
                    %% Arr    :  1  3  3 10
                    %% Doubled:  1  3  3 10  0  0  0  10
                    [put(Seq, 0) || Seq <- NewFree],
                    put(Extend - 1, get(ServerCount - 1)),
                    %% double the size of index_to_pid tuple
                    NewItp = list_to_tuple(tuple_to_list(setelement(Free + 1, Itp, Server)) ++ NewFree),
                    %% Now simply update new index in the double size array
                    inc(Free, Demand, Extend),
                    {noreply, State#lambda_plb_state{free = NewFree,
                        index_to_pid = NewItp,
                        pid_to_index = Pti#{Server => {Free, Demand}}, capacity = Cap + Demand}};
                [Free | More] ->
                    inc(Free, Demand, ServerCount),
                    {noreply, State#lambda_plb_state{free = More,
                        index_to_pid = setelement(Free + 1, Itp, Server),
                        pid_to_index = Pti#{Server => {Free, Demand}}, capacity = Cap + Demand}}
            end
    end;

handle_info({complete_order, [{_, _, Meta} | _] = Servers}, #lambda_plb_state{module = Module, meta = Waiting} = State) when is_list(Waiting) ->
    ?LOG_DEBUG("meta ~200p received for ~200p", [Meta, Waiting], #{domain => [lambda]}),
    %% start trapping exits just before compiling proxy, to ensure terminate/2 is
    %%  called when PLB shuts down
    process_flag(trap_exit, true),
    Module = compile_proxy(Module, Meta),
    [gen:reply(To, Meta) || To <- Waiting],
    handle_info({complete_order, Servers}, State#lambda_plb_state{meta = maps:get(module, Meta)});
handle_info({complete_order, Servers}, #lambda_plb_state{} = State) ->
    %% broker sent an update to us, order was (partially?) fulfilled, connect to provided servers
    ?LOG_DEBUG("found servers: ~200p", [Servers], #{domain => [lambda]}),
    Self = self(),
    [erlang:send(Pid, {connect, Self, Cap}, [nosuspend]) || {Pid, Cap, _Meta} <- Servers],
    {noreply, State};

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_plb_state{pid_to_index = Pti, free = Free, capacity = Cap} = State) ->
    %% server process died
    {{Index, _SrvCap}, NewPti} = maps:take(Pid, Pti),
    Removed = read(Index),
    inc(Index, -Removed, tuple_size(State#lambda_plb_state.index_to_pid)),
    %% block the queue if no capacity left
    Cap =:= Removed andalso begin State#lambda_plb_state.queue ! block end,
    %% setelement(Index, State#lambda_state.index_to_pid, undefined), %% not necessary, helps debugging
    {noreply, State#lambda_plb_state{free = [Index | Free], capacity = Cap - Removed, pid_to_index = NewPti}}.

terminate(_Reason, #lambda_plb_state{module = Mod, meta = Meta}) when is_map_key(exports, Meta) ->
    %% delete proxy code for the module
    Purged = code:purge(Mod),
    Deleted = code:delete(Mod),
    %% ignore errors, there is not much to do when terminating
    ?LOG_DEBUG("~s purge/delete (~p/~p), reason ~200p", [Mod, Purged, Deleted, _Reason], #{domain => [lambda]}),
    ok;
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal implementation

%% Blocking queue: processes are waiting until capacity is available.
queue() ->
    receive
        unblock ->
            % ?LOG_DEBUG("Queue unblocked", [], #{domain => [lambda]}),
            queue_open()
    end.

queue_open() ->
    receive
        {'$gen_call', From, wait} ->
            % ?LOG_DEBUG("Wait done for ~p", [From], #{domain => [lambda]}),
            gen:reply(From, ok),
            queue_open();
        block ->
            % ?LOG_DEBUG("Queue blocked", [], #{domain => [lambda]}),
            queue()
    end.

%% Implementation specifics:
%%  * unspecified value is returned for lower_bound when array is empty
%%  * undefined is returned for select when value is beyond max allowed

%% @doc Selects pid, and reduces weight of the pid
%%      by 1. Works in O(logN). The Heart of probabilistic
%%      load balancer.
-spec take(non_neg_integer(), non_neg_integer()) -> term().
take(Bound, Max) ->
    Idx = take_bound(Bound, 0, Max bsr 1),
    Last = Max - 1,
    put(Last, get(Last) - 1),
    Idx.

%% -------------------------------------------------------------------
%% Internal implementation details - Ryabko array primitives

%% @doc Returns value for a specific index of the array.
%%      Works in O(logN) time.
read(Idx) ->
    Val = get(Idx),
    read1(Idx, Val, 1).

read1(Idx, Val, Mask) when Idx band Mask =/= 0 ->
    read1(Idx, Val - get(Idx bxor Mask), Mask bsl 1);
read1(_Idx, Val, _Mask) ->
    Val.

%% @doc Updates weight at the specified index,
%%      adding a number (resulting weight must not
%%      be negative). Works in O(logN) time.
inc(Idx, Value, Max) when Idx =< Max ->
    put(Idx, get(Idx) + Value),
    inc(Idx bor (Idx + 1), Value, Max);
inc(_Idx, _Value, _Max) ->
    ok.

%% @doc Returns index in the array which has cumulative
%%      frequency equal or greater then requested, and
%%      reduces weight of this index by one.
%%      Works in O(logN) time.
take_bound(_Bound, Idx, 0) ->
    Idx;
take_bound(Bound, Idx, Mask) ->
    Mid = Idx + Mask - 1,
    Partial = get(Mid),
    case Bound - Partial of
        Left when Left < 0 ->
            put(Mid, Partial - 1),
            take_bound(Bound, Idx, Mask bsr 1);
        Right ->
            take_bound(Right, Idx + Mask, Mask bsr 1)
    end.

%% @private creates proxy, dynamically, when meta has been received for the first time.
compile_proxy(Module, Meta) ->
    #{module := #{exports := Exports}} = Meta,
    %% create a module (technically possible to make an AST, but for compatibility
    %%  reasons it's better to use text lines for compilation)
    ExpLine = "-export([" ++ lists:flatten(lists:join(", ", [io_lib:format("~s/~b", [F, A]) || {F, A} <- Exports])) ++ "]).",
    Cast = maps:get(cast, Meta, []),
    Impl = [proxy(Module, F, A, Cast) || {F, A} <- Exports],
    Lines = ["-module(" ++ atom_to_list(Module) ++ ").", ExpLine] ++ Impl,
    %% compile resulting proxy file
    Tokens = [begin {ok, T, _} = erl_scan:string(L), T end || L <- Lines],
    Forms = [begin {ok, F} = erl_parse:parse_form(T), F end || T <- Tokens],
    {ok, Module, Binary} = compile:forms(Forms),
    {module, Module} = code:load_binary(Module, "lambda", Binary),
    Module.

proxy(M, F, Arity, Cast) ->
    Args = lists:join(", ", ["Arg" ++ integer_to_list(Seq) || Seq <- lists:seq(1, Arity)]),
    CastCall = case lists:member(F, Cast) of true -> cast; false -> call end,
    lists:flatten(io_lib:format("~s(~s) -> lambda_plb:~s(~s, ~s, [~s], infinity).", [F, Args, CastCall, M, F, Args])).
