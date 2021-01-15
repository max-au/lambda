%% @doc
%% lambda_bootstrap is a process continuously resolving
%%  list of "bootstrap" authorities, and updating broker/authority
%%  processes when the list changes.
%%
%% Lambda bootstrap implements a number of default strategies,
%%  and a custom callback. Bootstrap may block, but it can't
%%  block any other process since they aren't waiting anything
%%  from bootstrap.
%% @end
-module(lambda_bootstrap).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/2
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

-type bootspec() ::
    {static, {lambda_discovery:location(), lambda_discovery:address()}} |
    {custom, module(), atom(), [term()]}. %% callback function

%% @doc
%% Starts the server and links it to calling process.
%% Needs at least one subscriber (otherwise boostrap has no use)
-spec start_link([pid() | atom()], bootspec()) -> gen:start_ret().
start_link([_ | _] = Subs, Bootspec) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Subs, Bootspec}, []).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_bootstrap_state, {
    %% subscribers
    subscribers :: [pid() | atom()],
    %% boot spec remembered
    spec :: [bootspec()],
    %% last resolved bootstrap (if any)
    bootstrap = [] :: [{lambda_discovery:location(), lambda_discovery:address()}],
    %% timer for regular updates
    timer :: undefined | reference()
}).

-type state() :: #lambda_bootstrap_state{}.

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: bootstrap " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

%% Attempt to resolve every 10 seconds
-define (LAMBDA_BOOTSTRAP_INTERVAL, 10000).

init({Subs, Spec}) ->
    {ok, handle_resolve(#lambda_bootstrap_state{subscribers = Subs, spec = Spec})}.

handle_call(_Req, _From, _State) ->
    erlang:error(notsup).

-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Req, _State) ->
    erlang:error(notsup).

%% timer handler
handle_info(resolve, State) ->
    {noreply, handle_resolve(State)}.

%%--------------------------------------------------------------------
%% Internal implementation

handle_resolve(#lambda_bootstrap_state{bootstrap = Prev, spec = Spec, subscribers = Subs} = State) ->
    case resolve(Spec) of
        Prev ->
            reschedule(State);
        New ->
            ?dbg("resolved ~p for ~p into ~200p", [Spec, Subs, Prev]),
            %% notify subscribers of changes, TODO: make diff Prev/New
            [gen_server:cast(Sub, {peers, New}) || Sub <- Subs],
            reschedule(State#lambda_bootstrap_state{bootstrap = New})
    end.

reschedule(State) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_INTERVAL, self(), resolve),
    State#lambda_bootstrap_state{timer = TRef}.

%%--------------------------------------------------------------------
%% Built-in resolvers

-include_lib("kernel/include/inet.hrl").

resolve({static, Static}) when is_map(Static) ->
    Static; %% preprocessed map of location => address()
resolve({static, Static}) when is_list(Static) ->
    %% list of Erlang node names, assuming epmd, and resolve
    %%  using erl_epmd
    resolve_epmd(Static, #{}).

resolve_epmd([], Acc) ->
    Acc;
resolve_epmd([Node | Tail], Acc) when Node =:= node() ->
    resolve_epmd(Tail, Acc#{{lambda_authority, Node} => lambda_discovery:get_node()});
resolve_epmd([Node | Tail], Acc) ->
    try
        [Name, Host] = string:split(atom_to_list(Node), "@", all),
        {ok, #hostent{ h_addr_list = [Ip | _]}} = inet:gethostbyname(Host),
        {port, Port, _Version} =
            try erl_epmd:port_please(Name, Ip)
            catch error:undef -> erl_epmd_ORIGINAL:port_please(Name, Ip) end,
        resolve_epmd(Tail, Acc#{{lambda_authority, Node} => {Ip, Port}})
    catch
        badmatch:noport ->
            %% node is not running, which isn't an error
            resolve_epmd(Tail, Acc);
        Class:Reason:Stack ->
            ?LOG_DEBUG("failed to resolve ~s, ~s:~p~n~200p", [Node, Class, Reason, Stack]),
            resolve_epmd(Tail, Acc)
    end.
