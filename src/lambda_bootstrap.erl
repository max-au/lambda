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
    discover/0
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

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% API

-type bootspec() ::
    {static, #{lambda_discovery:location() => lambda_discovery:address()}} |
    {epmd, [node()]} |
    {file, file:filename_all()} | %% DEBUG only: file on a file system contains the static map
    {custom, module(), atom(), [term()]}. %% callback function

%% @doc
%% Starts the server and links it to calling process.
%% Needs at least one subscriber (otherwise boostrap has no use)
-spec start_link([pid() | atom()], bootspec()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link([_ | _] = Subs, Bootspec) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Subs, Bootspec}, []).

%% @doc Forces discovery without waiting for timeout.
%%      Useful for testing.
-spec discover() -> ok.
discover() ->
    gen_server:cast(?MODULE, discover).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_bootstrap_state, {
    %% subscribers
    subscribers :: [pid() | atom()],
    %% boot spec remembered
    spec :: bootspec(),
    %% last resolved bootstrap (if any)
    bootstrap = #{} :: #{lambda_discovery:location() => lambda_discovery:address()},
    %% timer for regular updates
    timer :: undefined | reference()
}).

-type state() :: #lambda_bootstrap_state{}.

%% By default, attempt to resolve every 10 seconds
-define (LAMBDA_BOOTSTRAP_INTERVAL, 10000).

init({Subs, Spec}) ->
    %% DEBUG: for {file, ...} bootspec, if this node runs authority, trap exit
    %%  to ensure terminate/2 is called
    is_tuple(Spec) andalso element(1, Spec) =:= file andalso is_pid(whereis(lambda_authority)) andalso
        erlang:process_flag(trap_exit, true),
    {ok, handle_resolve(#lambda_bootstrap_state{subscribers = Subs, spec = Spec})}.

handle_call(_Req, _From, _State) ->
    erlang:error(notsup).

-spec handle_cast(discover, state()) -> {noreply, state()}.
handle_cast(discover, #lambda_bootstrap_state{timer = Timer} = State) ->
    Timer =/= undefined andalso erlang:cancel_timer(Timer),
    {noreply, handle_resolve(State)}.

%% timer handler
handle_info(resolve, State) ->
    {noreply, handle_resolve(State)}.

terminate(_Reason, #lambda_bootstrap_state{spec = {file, _File}}) ->
    is_pid(whereis(lambda_authority)) andalso
        ?LOG_DEBUG("deregister authority", #{domain => [lambda]});
terminate(_Reason, _State) ->
    ok.

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
    resolve_epmd(Epmd, Family, #{});

resolve({file, File}) ->
    case is_pid(whereis(lambda_authority)) of
        true ->
            Self = {lambda_authority, node()},
            Peers = case file:consult(File) of
                {ok, PeersList} ->
                    maps:from_list(PeersList);
                _ -> #{}
            end,
            SelfAddr = lambda_discovery:get_node(),
            case maps:get(Self, Peers, false) of
                SelfAddr ->
                    Peers;
                _ ->
                    ?LOG_DEBUG("registering authority ~200p => ~200p", [Self, SelfAddr], #{domain => [lambda]}),
                    ok = file:write_file(File, lists:flatten(io_lib:format("~tp.~n", [{Self, SelfAddr}])), [append]),
                    Peers#{Self => SelfAddr}
                end;
        false ->
            case file:consult(File) of
                {ok, [_|_] = Auths} ->
                    maps:from_list(Auths);
                _ ->
                    #{}
            end
    end.

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
    {ok, HostName} = inet:gethostname(),
    {list_to_atom(lists:concat([Name, "@", HostName])), #{addr => ?INET_LOCALHOST, port => resolve_port(Name, ?INET_LOCALHOST)}};
resolve_ip([Name], inet6) ->
    {ok, HostName} = inet:gethostname(),
    {list_to_atom(lists:concat([Name, "@", HostName])), #{addr => ?INET6_LOCALHOST, port => resolve_port(Name, ?INET6_LOCALHOST)}};
resolve_ip([Name, Host], Family) ->
    {ok, #hostent{h_addr_list = [Ip | _]}} = inet:gethostbyname(Host, Family),
    {list_to_atom(lists:concat([Name, "@", Host])), #{addr => Ip, port => resolve_port(Name, Ip)}}.

resolve_port(Name, Ip) ->
    {port, Port, _Version} = erl_epmd:port_please(Name, Ip),
    Port.
