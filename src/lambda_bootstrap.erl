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
    handle_info/2
]).

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% API

-type bootspec() ::
    {static, #{lambda_discovery:location() => lambda_discovery:address()}} |
    {epmd, [node()]} |
    {file, file:filename_all()} | %% consult() file on a file system (for test purposes)
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

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: bootstrap " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

%% By default, attempt to resolve every 10 seconds
-define (LAMBDA_BOOTSTRAP_INTERVAL, 10000).

init({Subs, Spec}) ->
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

%%--------------------------------------------------------------------
%% Internal implementation

handle_resolve(#lambda_bootstrap_state{spec = Spec, subscribers = Subs} = State) ->
    New = resolve(Spec),
    ?dbg("resolved for ~200p into ~200p", [Subs, New]),
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
            case file:open(File, [exclusive]) of
                {ok, Fd} ->
                    SelfAddr = lambda_discovery:get_node(),
                    Self = {lambda_authority, node()},
                    ?dbg("creating authority ~200p => ~200p", [Self, SelfAddr]),
                    ok = file:write(Fd, lists:flatten(io_lib:format("~tp.~n", [{Self, SelfAddr}]))),
                    file:close(Fd),
                    #{Self => SelfAddr};
                {error, eexist} ->
                    retry_file(File)
            end;
        false ->
            %% debug only: retry until there is something in the file
            retry_file(File)
    end.

retry_file(File) ->
    case file:consult(File) of
        {ok, [_|_] = Auths} ->
            maps:from_list(Auths);
        _ ->
            receive after 10 -> retry_file(File) end
    end.

resolve_epmd([], _Family, Acc) ->
    Acc;
resolve_epmd([Node | Tail], Family, Acc) when Node =:= node() ->
    resolve_epmd(Tail, Family, Acc#{{lambda_authority, Node} => lambda_discovery:get_node()});
resolve_epmd([Node | Tail], Family, Acc) ->
    try
        [Name, Host] = string:split(atom_to_list(Node), "@", all),
        {ok, #hostent{h_addr_list = [Ip | _]}} = inet:gethostbyname(Host, Family),
        Port = resolve_port(Name, Ip),
        resolve_epmd(Tail, Family, Acc#{{lambda_authority, Node} => {Ip, Port}})
    catch
        badmatch:noport ->
            %% node is not running, which isn't an error
            resolve_epmd(Tail, Family, Acc);
        Class:Reason:Stack ->
            ?LOG_DEBUG("failed to resolve ~s, ~s:~p~n~200p", [Node, Class, Reason, Stack]),
            resolve_epmd(Tail, Family, Acc)
    end.

resolve_port(Name, Ip) ->
    {port, Port, _Version} =
        try erl_epmd:port_please(Name, Ip)
        catch error:undef ->
            %% the "original" module is created dynamically in runtime
            AntiDialyzer = list_to_atom("erl_epmd_ORIGINAL"),
            AntiDialyzer:port_please(Name, Ip) end,
    Port.
