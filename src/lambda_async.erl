%% @doc
%%  Lambda async, collection of useful helper concurrency functions.
%%  Simple parallel map example:
%%      [Res1, Res2, Rs3] =
%%        lambda_async:pmap([
%%          {mod, fun, [Arg1, Arg2]},
%%          fun () -> ok end,
%%          {fun (X) -> X end, X}]).
%%
%%  Barrier example:
%%      Ctx = lambda_async:async(fun () -> sleep(10) end),
%%      lambda_async(Ctx, [{mymod, slowfun, [Arg]} || Arg <- Calls]),
%%      [SleepRet | More] = lambda_async:wait(Ctx).
%%
%%  Separating success from errors:
%%      #{ok := Success, timeout := Timeout} =
%%          lambda_async:pmap([fun () -> timer:sleep(rand:uniform(1000)) end || _ <- lists:seq(1, 5)], 500).
%%
%% @end
-module(lambda_async).
-compile(warn_missing_spec).
-author("maximfca@gmail.com").

-export([
    start_link/1,
    async/1,
    async/2,
    wait/2,
    pmap/1,
    pmap/2,
    pmap/3
]).

%% Can execute function, function with args, and MFA.
-type async_fun() :: {module(), atom(), Args :: [term()]} | function() | {function(), [term()]}.

%% Can execute either a list of async_fun. It is possible to assign
%%  an ID to the Fun (order is retained for subsequent reduce).
%% When ID is not assigned, Fun itself serves as a non-unique ID.
%% Internally an artificial integer ID is used.
-type async_funs() :: [async_fun()].

%% Reduce methods:
%% * order: return in the same order as added
%% * unique: return a map of #{result => [ID]}
%% * custom reduce function: reduce(Fun, Result, Acc) -> NewAcc
-type reduce() :: order | unique |
    {fun (({ID :: async_fun(), Result :: term()}, Acc :: term()) -> term()), Initial :: term()}.

%% Result is returned depending on the reduce method.
%% May return term(), {'EXIT', timeout} or {'EXIT', term()}
-type async_result() ::
    [term()] |
    #{term() => [term()]}.

%% Context
-type context() :: pid().

%% @doc Starts an empty context with specific reduce.
-spec start_link(reduce()) -> context().
start_link(Reduce) ->
    proc_lib:spawn_link(
        fun () ->
            process_flag(trap_exit, true),
            loop(1, #{}, [], undefined, Reduce)
        end).

%% @doc Schedules async execution of Fun with a new context.
-spec async(async_funs()) -> context().
async(Funs) ->
    async(start_link(order), Funs).

%% @doc Schedules async execution with existing context.
-spec async(context(), async_funs()) -> context().
async(Ctx, Funs) when is_list(Funs) ->
    gen_server:cast(Ctx, {run, Funs}),
    Ctx;
async(Ctx, Funs) ->
    gen_server:cast(Ctx, {run, [Funs]}),
    Ctx.

%% @doc Completes all async requests for a given context.
%%      Funs that are still running will be terminated (killed),
%%      and reported as "timeout".
-spec wait(Ctx :: context(), timeout()) -> async_result().
wait(Ctx, infinity) ->
    gen_server:call(Ctx, {collect, infinity}, infinity);
wait(Ctx, Timeout) ->
    gen_server:call(Ctx, {collect, Timeout}, Timeout * 2).

%% @doc Execute Funs concurrently, with 5 second timeout.
-spec pmap(async_funs()) -> async_result().
pmap(Funs) ->
    pmap(Funs, order, 5000).

%% @doc Parallel map: executes all Funs concurrently, waits at most Timeout,
%%      returns 'killed' for those exceeding timeout, uses specified async ID.
-spec pmap(async_funs(), timeout()) -> async_result().
pmap(Funs, Timeout) ->
    pmap(Funs, order, Timeout).

-spec pmap(async_funs(), reduce(), timeout()) -> async_result().
pmap(Funs, Reduce, Timeout) ->
    wait(async(start_link(Reduce), Funs), Timeout).

%%--------------------------------------------------------------------
%% Internal implementation
%% This is technically a gen_server, but without extra scaffolding.

%% accumulation phase, when new jobs are allowed
loop(NextId, Running, Complete, ReplyTo, Reduce) ->
    receive
        {'EXIT', Pid, Value} ->
            case maps:take(Pid, Running) of
                {Id, Remain} when Remain =:= #{}, ReplyTo =/= undefined ->
                    %% last response, and collection was ongoing
                    gen:reply(ReplyTo, reduce(Reduce, complete(Value, Id, Complete)));
                {Id, Remain} ->
                    %% spawned child
                    loop(NextId, Remain, complete(Value, Id, Complete), ReplyTo, Reduce);
                error ->
                    %% something else, likely parent process
                    exit(Value)
            end;
        {'$gen_cast', {run, Funs}} ->
            %% is it okay to keep accepting new jobs after calling 'collect'?
            loop(NextId + length(Funs), start_funs(Reduce, NextId, Funs, Running), Complete, ReplyTo, Reduce);
        {'$gen_call', From, {collect, Timeout}} when ReplyTo =:= undefined ->
            Timeout /= infinity andalso erlang:send_after(Timeout, self(), timeout),
            loop(NextId, Running, Complete, From, Reduce);
        timeout ->
            %% cancel outstanding Funs, reduce what we already have
            AllDone = Complete ++ [{ID, {'EXIT', timeout}} || ID <- maps:keys(Running)],
            gen:reply(ReplyTo, reduce(Reduce, AllDone))
    end.

start_funs(_Reduce, _Next, [], Running) ->
    Running;
start_funs(order, Next, [H | T], Running) ->
    Pid = start_fun(H),
    start_funs(order, Next + 1, T, Running#{Pid => Next});
start_funs(NoOrder, Next, [H | T], Running) ->
    Pid = start_fun(H),
    start_funs(NoOrder, Next + 1, T, Running#{Pid => H}).

start_fun({M, F, A}) when is_atom(M), is_atom(F), is_list(A) ->
    spawn_link(fun () -> erlang:exit({ok, erlang:apply(M, F, A)}) end);
start_fun(Fun) when is_function(Fun, 0) ->
    spawn_link(fun () -> erlang:exit({ok, Fun()}) end);
start_fun({Fun, Args}) when is_function(Fun, length(Args)) ->
    spawn_link(fun () -> erlang:exit({ok, erlang:apply(Fun, Args)}) end).

complete({ok, Ret}, Id, Complete) ->
    [{Id, Ret} | Complete];
complete(Error, Id, Complete) ->
    [{Id, {'EXIT', Error}} | Complete].

reduce(order, Complete) ->
    element(2, lists:unzip(lists:keysort(1, Complete)));
reduce(unique, Complete) ->
    lists:foldl(
        fun ({ID, Result}, Acc) ->
            Existing = maps:get(Result, Acc, []),
            RealId = ID,
            maps:put(Result, [RealId | Existing], Acc)
        end, #{}, Complete);
reduce({Fun, Initial}, Complete) ->
    lists:foldl(Fun, Initial, Complete).
