%% @doc
%% WARNING: this is only the second iteration of bootstrap design,
%%  so it's likely to change in the future.
%%
%% Bootstrap is a way to find initial list of authorities to
%%  connect to. More authorities will be discovered when at least
%%  one is connected.
%%
%% Lambda bootstrap, implementing a number of default strategies,
%%  or a custom callback to fetch bootstrap information. Bootstrap
%%  may block, but it should never block authority or broker.
%% Bootstrap communications are asynchronous: a process that needs
%%  bootstrapping subscribes to bootstrap process, and receives
%%  updates.
%% When there are no subscriptions, boostrap process does not attempt
%%  to resolve anything. However, if there is an active subscription,
%%  bootstrap will go over bootstrap sequence.
%% @end
-module(lambda_bootstrap).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/1,
    subscribe/0,
    subscribe/2,
    unsubscribe/0,
    unsubscribe/1
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------

-type bootspec() ::
    undefined |                 %% root authority (same as empty boot map)
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

%% @doc Starts bootstrap subscription for default server, using 'temporary'
%%      delivery mode, when subscription is removed as soon as any
%%      bootstrap is resolved successfully.
-spec subscribe() -> ok.
subscribe() ->
    subscribe(?MODULE, temporary).

-spec subscribe(gen:emgr_name(), Temporary :: temporary | permanent) -> ok.
subscribe(Dest, Temporary) ->
    gen_server:cast(Dest, {subscribe, self(), Temporary}).

%% @doc Unsubscribes process from bootstrap resolver
-spec unsubscribe() ->ok.
unsubscribe() ->
    unsubscribe(?MODULE).

-spec unsubscribe(gen:emgr_name()) -> ok.
unsubscribe(Dest) ->
    gen_server:cast(Dest, {unsubscribe, self()}).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_bootstrap_state, {
    timer :: undefined | reference(),
    %% subscribers: temporary or permanent
    subscribers = #{} :: #{pid() => temporary | permanent},
    %% last resolved bootstrap (if any)
    bootstrap :: undefined | lambda_broker:points(),
    %% boot spec remembered
    spec :: bootspec()
}).

-type state() :: #lambda_bootstrap_state{}.

-define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: exchange " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

%% Attempt to resolve every 10 seconds, if there are subscribers.
-define (LAMBDA_BOOTSTRAP_RETRY_TIME, 10000).

-spec init(bootspec()) -> {ok, state()}.
init(Bootspec) ->
    {ok, #lambda_bootstrap_state{spec = Bootspec}, {continue, init}}.

handle_continue(init, State) ->
    {noreply, handle_resolve(State)}.

handle_call(_Req, _From, _State) ->
    erlang:error(notsup).

handle_cast({subscribe, From, Temp}, #lambda_bootstrap_state{timer = undefined}) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_RETRY_TIME, self(), resolve),
    erlang:monitor(process, From),
    {noreply, handle_resolve(#lambda_bootstrap_state{timer = TRef, subscribers = #{From => Temp}})};
handle_cast({subscribe, From, Temp}, #lambda_bootstrap_state{subscribers = Subs} = State) ->
    is_map_key(From, Subs) orelse erlang:monitor(process, From),
    {noreply, State#lambda_bootstrap_state{subscribers = Subs#{From => Temp}}};

handle_cast({unsubscribe, From}, State) ->
    {noreply, cancel(From, State)}.

%% handling bootstrapping retries
handle_info(resolve, State) ->
    {noreply, handle_resolve(State)};

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

handle_resolve(#lambda_bootstrap_state{spec = Spec, subscribers = Subs} = State) ->
    try
        BootMap = resolve(Spec),
        %% notify subscribers, potentially removing subscribed temporarily
        NewSubs = maps:fold(
            fun (Sub, temporary, Acc) ->
                    Sub ! {?MODULE, boot, BootMap},
                    Acc;
                (Sub, permanent, Acc) ->
                    Sub ! {?MODULE, boot, BootMap},
                    Acc#{Sub => permanent}
            end, #{}, Subs),
        %% if an empty map was resolved, and boot spec is defined, retry the attempt
        reschedule(State#lambda_bootstrap_state{bootstrap = BootMap, subscribers = NewSubs})
    catch
        Class:Reason:Stack ->
            ?LOG_NOTICE("Lambda bootstrap failed, ~s:~p (~200p)", [Class, Reason, Stack]),
            reschedule(State)
    end.

reschedule(#lambda_bootstrap_state{spec = undefined, subscribers = Empty} = State) when Empty =:= #{} ->
    State;
reschedule(State) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_RETRY_TIME, self(), resolve),
    State#lambda_bootstrap_state{timer = TRef}.

%%--------------------------------------------------------------------
%% Built-in resolvers

resolve({epmd, Node}) when is_atom(Node) ->
    error(notsup);
resolve({epmd, Hostname}) when is_list(Hostname) ->
    error(notsup);
resolve(undefined) ->
    #{};
resolve(BootMap) when BootMap =:= #{} ->
    #{};
resolve(BootMap) when is_map(BootMap) ->
    BootMap;
resolve(_Any) ->
    error(notsup).
