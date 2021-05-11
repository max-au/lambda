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
    start_link/2,
    discover/0,
    discover/1
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
    {static, #{lambda:location() => lambda_discovery:address()}} |
    {epmd, [node()]} |
    {custom, module(), atom(), [term()]}. %% callback function

%% @doc
%% Starts the server and links it to calling process.
%% Needs at least one subscriber (otherwise boostrap has no use)
-spec start_link([pid() | atom()], bootspec()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link([_ | _] = Subs, Bootspec) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Subs, Bootspec}, []).

%% @doc Forces discovery. Useful for testing.
-spec discover() -> ok.
discover() ->
    gen_server:call(?MODULE, discover).

%% @doc Replaces bootspec and forces discovery.
-spec discover(BootSpec :: bootspec()) -> ok.
discover(BootSpec) ->
    gen_server:call(?MODULE, {discover, BootSpec}).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_bootstrap_state, {
    %% subscribers
    subscribers :: [pid() | atom()],
    %% boot spec remembered
    spec :: bootspec(),
    %% last resolved bootstrap (if any)
    bootstrap = #{} :: #{lambda:location() => lambda_discovery:address()},
    %% timer for regular updates
    timer :: undefined | reference()
}).

-type state() :: #lambda_bootstrap_state{}.

%% By default, attempt to resolve every 10 seconds
-define (LAMBDA_BOOTSTRAP_INTERVAL, 10000).

init({Subs, Spec}) ->
    {ok, handle_resolve(#lambda_bootstrap_state{subscribers = Subs, spec = Spec})}.

handle_call(discover, _From, #lambda_bootstrap_state{timer = Timer} = State) ->
    Timer =/= undefined andalso erlang:cancel_timer(Timer),
    {reply, ok, handle_resolve(State)};
handle_call({discover, NewSpec}, _From, State) ->
    handle_call(discover, _From, State#lambda_bootstrap_state{spec = NewSpec}).

-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Req, _State) ->
    erlang:error(notsup).

%% timer handler
handle_info(resolve, State) ->
    {noreply, handle_resolve(State)}.

%%--------------------------------------------------------------------
%% Internal implementation

handle_resolve(#lambda_bootstrap_state{spec = Spec, subscribers = Subs} = State) ->
    New = resolve(Spec),
    ?LOG_DEBUG("resolved ~p for ~200p into ~200p", [Spec, Subs, New], #{domain => [lambda]}),
    %% notify subscribers of changes
    %% TODO: think how to avoid bootstrapping over and over
    Diff = New, %% maps:without(maps:keys(Prev), New),
    [Sub ! {peers, Diff} || Sub <- Subs],
    reschedule(State#lambda_bootstrap_state{bootstrap = New}).

reschedule(State) ->
    TRef = erlang:send_after(?LAMBDA_BOOTSTRAP_INTERVAL, self(), resolve),
    State#lambda_bootstrap_state{timer = TRef}.

%%--------------------------------------------------------------------
%% Built-in resolvers

-include_lib("kernel/include/inet.hrl").

resolve({static, Static}) when is_map(Static) ->
    Static; %% preprocessed map of location => address()
resolve({epmd, Epmd}) ->
    %% list of Erlang node names, assuming epmd, and resolve
    %%  using erl_epmd. Must know distribution family, take it
    %%  from undocumented net_kernel structure (although could use
    %%  command line, -proto_dist)
    Family = try
        {state, _Node, _ShortLong, _Tick, _, _SysDist, _, _, _,
            [{listen, _Pid, _Proc, {net_address, {_Ip, _Port}, _HostName, _Proto, Fam}, _Mod}],
            _, _, _, _, _} = sys:get_state(net_kernel),
            Fam
    catch _:_ ->
        case inet_db:res_option(inet6) of true -> inet6; false -> inet end
    end,
    resolve_epmd(Epmd, Family, #{}).

resolve_epmd([], _Family, Acc) ->
    Acc;
resolve_epmd([Node | Tail], Family, Acc) when Node =:= node() ->
    resolve_epmd(Tail, Family, Acc#{{lambda_authority, Node} => lambda_discovery:get_node()});
resolve_epmd([Node | Tail], Family, Acc) ->
    try
        {FullNode, Addr} = resolve_ip(string:split(atom_to_list(Node), "@", all), Family),
        resolve_epmd(Tail, Family, Acc#{{lambda_authority, FullNode} => Addr})
    catch
        badmatch:noport ->
            %% node is not running, which isn't an error
            resolve_epmd(Tail, Family, Acc);
        Class:Reason:Stack ->
            ?LOG_DEBUG("failed to resolve ~s, ~s:~p~n~200p", [Node, Class, Reason, Stack], #{domain => [lambda]}),
            resolve_epmd(Tail, Family, Acc)
    end.

-define (INET_LOCALHOST, {127, 0, 0, 1}).
-define (INET6_LOCALHOST, {0, 0, 0, 0, 0, 0, 0, 1}).

resolve_ip([Name], inet) ->
    resolve_local(Name, ?INET_LOCALHOST);
resolve_ip([Name], inet6) ->
    resolve_local(Name, ?INET6_LOCALHOST);
resolve_ip([Name, Host], Family) ->
    {ok, #hostent{h_addr_list = [Ip | _]}} = inet:gethostbyname(Host, Family),
    {list_to_atom(lists:concat([Name, "@", Host])), #{addr => Ip, port => resolve_port(Name, Ip)}}.

resolve_local(Name, Ip) ->
    {ok, HostName} = inet:gethostname(),
    Host =
        case net_kernel:longnames() of
            true ->
                HostName ++ case inet_db:res_option(domain) of [] -> []; D -> [$. | D] end;
            false ->
                HostName;
            ignored ->
                "nohost"
        end,
    {list_to_atom(lists:concat([Name, "@", Host])), #{addr => Ip, port => resolve_port(Name, Ip)}}.

%% This is a temporary suppression needed only until OTP is updated to 24 RC1,
%%  where bug in erl_epmd:port_please/2 spec is fixed.
-dialyzer([{no_return, [resolve_port/2, resolve_ip/2, resolve_local/2]}, {no_fail_call, [resolve_port/2]}]).
resolve_port(Name, Ip) ->
    %% dialyzer for OTP before 24 will complain if real spec is used
    {port, Port, _Version} = erl_epmd:port_please(Name, Ip),
    Port.
