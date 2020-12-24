%% @doc
%%  Lambda async, collection of useful helper concurrency functions.
%%  All processes spawned within this module are linked to avoid
%%   rogues running away. This however means that caller is going to
%%   trap exits while executing anything async. It is guaranteed that
%%   async will not swallow any unintended messages.
%%
%% @end
-module(lambda_async).
-author("maximfca@gmail.com").

-export([
    async/1,
    async/2,
    wait/1,
    wait/2,
    pmap/1,
    pmap/2
]).

%% Can execute function, function with args, and MFA.
-type async_fun() :: {module(), atom(), Args :: [term()]} | function() | {function(), [term()]}.

%% @doc Parallel map: executes al Funs concurrently, waits infinitely for result.
%%      Spawns additional process even if only one Fun is passed. This protects
%%      the caller from unintentional garbage left in process dictionary.
-spec pmap([async_fun()]) -> [term()].
pmap(Funs) ->
    pmap(Funs, infinity).

%% @doc Parallel map: executes al Funs concurrently, waits at most Timeout,
%%      returns 'killed' for those exceeding timeout.
-spec pmap([async_fun()], timeout()) -> [term()].
pmap(Funs, Timeout) ->
    pmap(async, Funs, Timeout).

%% @doc Parallel map: executes al Funs concurrently, waits at most Timeout,
%%      returns 'killed' for those exceeding timeout, uses specified async ID.
-spec pmap(term(), [async_fun()], timeout()) -> [term()].
pmap(Id, Funs, Timeout) ->
    [async(Id, Fun) || Fun <- Funs],
    wait(Id, Timeout).

%% @doc schedules async execution of Fun, with default context named async.
-spec async(async_fun() | [async_fun()]) -> async.
async(Fun) ->
    async(async, Fun).

%% @doc schedules async execution to the executor Id (stored in process dictionary)
-spec async(Id, async_fun() | [async_fun()]) -> Id when Id :: term().
async(Id, Fun) ->
    case erlang:get(Id) of
        undefined ->
            %% need to trap exits, otherwise if a child process
            %%  exits with an error, we'll exit too
            erlang:put(Id, {spawn_impl(Fun, []), erlang:process_flag(trap_exit, true)});
        {Running, Trap} ->
            erlang:put(Id, {spawn_impl(Fun, Running), Trap})
    end.

%% @doc Completes async requests for default context.
-spec wait(timeout()) -> ok.
wait(Timeout) ->
    wait(async, Timeout).

%% @doc Completes all async requests given a context and timeout.
%%      Resets trap_exit to the state it was before, removed context from
%%      process dictionary.
%%      In case of a timeout, remaining processes are forcefully exited (killed).
-spec wait(Id :: term(), timeout()) -> #{term() => term()}.
wait(Id, infinity) ->
    wait_impl(Id, undefined);
wait(Id, Timeout) ->
    wait_impl(Id, erlang:start_timer(Timeout, self(), timeout)).

%%--------------------------------------------------------------------
%% Internal implementation

spawn_impl({M, F, A}, Prev) when is_atom(M), is_atom(F), is_list(A) ->
    [spawn_link(fun () -> erlang:exit(erlang:apply(M, F, A)) end) | Prev];
spawn_impl(Fun, Prev) when is_function(Fun, 0) ->
    [spawn_link(fun () -> erlang:exit(Fun()) end) | Prev];
spawn_impl({Fun, Args}, Prev) when is_function(Fun, length(Args)) ->
    [spawn_link(fun () -> erlang:exit(erlang:apply(Fun, Args)) end) | Prev];
spawn_impl([], Prev) ->
    Prev;
spawn_impl([Fun | Tail], Prev) ->
    spawn_impl(Tail, spawn_impl(Fun, Prev)).

collect_replies([], Replies, undefined) ->
    lists:reverse(Replies);
collect_replies([], Replies, TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            %% timer expired before we canceled it, so
            %%  lets flush the message in the queue
            receive
                {timeout, TRef, timeout} ->
                    collect_replies([], Replies, undefined)
            end;
        _ ->
            collect_replies([], Replies, undefined)
    end;
collect_replies([Pid | Tail] = All, Replies, TRef) ->
    receive
        {timeout, TRef, timeout} ->
            %% reached the timeout. stop all children forcefully.
            [exit(P, kill) || P <- All],
            %% continue collecting...
            collect_replies(All, Replies, undefined);
        {'EXIT', Pid, Value} ->
            collect_replies(Tail, [Value | Replies], TRef)
    end.

wait_impl(Id, TRef) ->
    {Running, Trapping} = erlang:get(Id),
    Replies = collect_replies(lists:reverse(Running), [], TRef),
    %% reset trap_exit flag if it was set
    Trapping orelse erlang:process_flag(trap_exit, false),
    erlang:erase(Id),
    Replies.
