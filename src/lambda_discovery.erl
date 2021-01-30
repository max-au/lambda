%% @doc
%% Lambda discovery: a way to inject external node name address resolution
%%  into Erlang distribution.
%% Implements a simple mapping, exposed via EPMD API.
%% @end
-module(lambda_discovery).
-author("maximfca@gmail.com").

%% API
-export([
    set_node/2,
    del_node/1,
    get_node/1,
    get_node/0
]).

%% Callbacks required for epmd module implementation
-export([
    start_link/0,
    names/1,
    register_node/2,
    register_node/3,
    port_please/3,
    listen_port_please/2,
    address_please/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-behaviour(gen_server).

-include_lib("kernel/include/inet.hrl").

%%--------------------------------------------------------------------
%% Public API

%% Full set of information needed to establish network connection
%%  to a specific host, with no external services required.
%% TODO: extend to add TLS support
-type address() :: {inet:ip_address(), inet:port_number()}.

%% Process location (similar to emgr_name()). Process ID (pid)
%%  designates both node name and ID. If process ID is not known,
%%  locally registered process name can be used.
-type location() :: pid() | {atom(), node()}.

-export_type([address/0, location/0]).

%% @doc
%% Starts the server and links it to calling process. Required for
%%  epmd interface.
-spec start_link() -> {ok, pid()} | ignore | {error,term()}.
start_link() ->
    case is_replaced() of
        true ->
            ignore;
        false ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, [], [])
    end.

%% @doc Sets the mapping between node name and address to access the node.
-spec set_node(location(), address()) -> ok.
set_node(Pid, Address) when is_pid(Pid) ->
    set_node(node(Pid), Address);
set_node({RegName, Node}, Address) when is_atom(RegName), is_atom(Node) ->
    set_node(Node, Address);
set_node(Node, _Address) when Node =:= node() ->
    %% ignore attempts to set this node address
    ok;
set_node(Node, {_Ip, _Port} = Address) ->
    gen_server:call(?MODULE, {set, Node, Address}).

%% @doc Removes the node name from the map. Does nothing if node was never added.
-spec del_node(node()) -> ok.
del_node(Node) ->
    gen_server:call(?MODULE, {del, Node}).

%% @doc Returns mapping of the node name to an address.
-spec get_node(node()) -> lambda:address() | error.
get_node(Node) ->
    gen_server:call(?MODULE, {get, Node}).

%% @doc Returns this node distribution connectivity info, that is
%%      expected to work when sent to other nodes via distribution
%%      channels. It is expected to return external node address,
%%      if host is behind NAT.
-spec get_node() -> address() | not_distributed.
get_node() ->
    gen_server:call(?MODULE, {get, node()}).

%%--------------------------------------------------------------------
%% EPMD implementation

-define (EPMD_VERSION, 6).

%% @doc Lookup a node Name at Host, returns {port, P, Version} | noport
-spec port_please(Name, Host, Timeout) -> {port, Port, Version} | noport when
    Name :: atom() | string(),
    Host :: atom() | string() | inet:ip_address(),
    Timeout :: timeout(),
    Port :: inet:port_number(),
    Version :: non_neg_integer().

port_please(Name, Host, _Timeout) ->
    case get_node(make_node(Name, Host)) of
        {_Family, _Addr, Port} ->
            {port, Port, ?EPMD_VERSION}; %% hardcode EPMD version to 6
        error ->
            noport
    end.

-spec names(Host) -> {ok, [{Name, Port}]} | {error, Reason} when
    Host :: atom() | string() | inet:ip_address(),
    Name :: string(),
    Port :: inet:port_number(),
    Reason :: address | file:posix().

names(HostName) ->
    gen_server:call(?MODULE, {names, HostName}).

-spec register_node(Name, Port) -> Result when
    Name :: string(),
    Port :: inet:port_number(),
    Creation :: non_neg_integer(),
    Result :: {ok, Creation} | {error, already_registered} | term().

register_node(Name, PortNo) ->
    register_node(Name, PortNo, inet).

-spec register_node(Name, Port, Driver) -> Result when
    Name :: string(),
    Port :: inet:port_number(),
    Driver :: inet_tcp | inet6_tcp | inet | inet6,
    Creation :: non_neg_integer() | -1,
    Result :: {ok, Creation} | {error, already_registered} | term().

register_node(Name, PortNo, inet_tcp) ->
    register_node(Name, PortNo, inet);
register_node(Name, PortNo, inet6_tcp) ->
    register_node(Name, PortNo, inet6);
register_node(Name, PortNo, Family) ->
    gen_server:call(?MODULE, {register, Name, PortNo, Family}).

-spec listen_port_please(Name, Host) -> {ok, Port} when
    Name :: atom() | string(),
    Host :: atom() | string() | inet:ip_address(),
    Port :: non_neg_integer().
listen_port_please(_Name, _Host) ->
    {ok, 0}.

-spec address_please(Name, Host, AddressFamily) -> Success | {error, term()} when
    Name :: string(),
    Host :: string() | inet:ip_address(),
    AddressFamily :: inet | inet6,
    Port :: inet:port_number(),
    Version :: non_neg_integer(),
    Success :: {ok, inet:ip_address(), Port, Version}.

address_please(Name, Host, AddressFamily) ->
    case get_node(make_node(Name, Host)) of
        {Addr, Port} when AddressFamily =:= inet, tuple_size(Addr) =:= 4 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        {Addr, Port} when AddressFamily =:= inet6, tuple_size(Addr) =:= 8 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        error ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Server implementation

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: discovery " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

%% Discovery state: maps node names to addresses.
%% Current limitation: a node can only have a single address,
%%  either IPv4 or IPv6.
-type state() :: #{node() => lambda:address()}.

-spec init([]) -> {ok, state()}.
init([]) ->
    is_replaced() orelse
        begin
            %% Hot code load erl_epmd to replace address_please/3.
            true = code:unstick_mod(erl_epmd),
            rename(erl_epmd, erl_epmd_ORIGINAL),
            copy(lambda_discovery, erl_epmd),
            true = code:stick_mod(erl_epmd),
            false = code:purge(erl_epmd)
        end,
    %% need to trap exit for terminate/2 to be called
    erlang:process_flag(trap_exit, true),
    %% always populate the local node address
    try
        {state, _Node, _ShortLong, _Tick, _, _SysDist, _, _, _,
            [{listen, _Port, _Proc, {net_address, {_Ip, Port}, HostName, _Proto, Fam}, _Mod}],
            _, _, _, _, _} = sys:get_state(net_kernel),
        {ok, #{node() => {local_addr(HostName, Fam), Port}}}
    catch
        _:_ ->
            {ok, #{node() => not_distributed}}
    end.

handle_call({set, Node, Address}, _From, State) ->
    ?dbg("set ~s to ~200p", [Node, Address]),
    {reply, ok, State#{Node => Address}};

handle_call({del, Node}, _From, State) ->
    {reply, ok, maps:remove(Node, State)};

handle_call({get, Node}, _From, State) ->
    ?dbg("asking for ~s (~200p)", [case Node =:= node() of true -> "self"; _ -> Node end, maps:get(Node, State, error)]),
    {reply, maps:get(Node, State, error), State};

handle_call({names, HostName}, _From, State) ->
    %% find all Nodes of a HostName - need to iterate the entire node
    %%  map. This is a very rare request, so can be slow
    Nodes = lists:filter(fun (Full) -> tl(string:lexemes(Full, "@")) =:= HostName end, maps:keys(State)),
    {reply, Nodes, State};

handle_call({register, Name, PortNo, Family}, _From, State) ->
    %% get the local hostname
    {ok, Host} = inet:gethostname(),
    Domain = case inet_db:res_option(domain) of [] -> []; D -> [$. | D] end,
    Long = make_node(Name, Host ++ Domain),
    Short = make_node(Name, Host),
    Addr = local_addr(Host, Family),
    %% register both short and long names
    {reply, {ok, 1}, State#{Long => {Addr, PortNo}, Short => {Addr, PortNo}}}.

handle_cast(_Cast, _State) ->
    error(badarg).

terminate(_Reason, _State) ->
    %% if erl_epmd code was replaced, get old code back
    is_replaced() andalso
        begin
            BeamFile = filename:join(code:lib_dir(kernel, ebin), "erl_epmd"),
            true = code:unstick_mod(erl_epmd),
            {module, erl_epmd} = code:load_abs(BeamFile, erl_epmd),
            %% we're still running old code (this code!), so can't purge
            %%  effectively
            true = code:stick_mod(erl_epmd),
            false = code:purge(erl_epmd_ORIGINAL),
            true = code:delete(erl_epmd_ORIGINAL)
        end.

%%--------------------------------------------------------------------
%% Internal implementation

is_replaced() ->
    Kernel = code:lib_dir(kernel, ebin),
    filename:dirname(code:which(erl_epmd)) =/= Kernel.

copy(From, To) ->
    {Mod, Binary, _Filename} = code:get_object_code(From),
    {ok, {Mod, [{abstract_code, {_, Forms}}]}} = beam_lib:chunks(Binary, [abstract_code]),
    Expanded = erl_expand_records:module(Forms, [strict_record_tests]),
    Replaced = [
        case Form of
            {attribute, Ln, module, From} ->
                {attribute, Ln, module, To};
            Other ->
                Other
        end || Form <- Expanded],
    {ok, To, Bin} = compile:forms(Replaced, [silent]),
    {module, To} = code:load_binary(To, code:which(From), Bin).

rename(From, To) ->
    copy(From, To),
    false = code:purge(From).

local_addr(Host, Family) ->
    AddrLen = case Family of inet -> 4; inet6 -> 8 end,
    case application:get_env(kernel, inet_dist_use_interface) of
        {ok, Addr1} when tuple_size(Addr1) =:= AddrLen ->
            Addr1;
        undefined ->
            %% native resolver cannot be used because discovery process
            %%  is started under kernel_sup when inet_native is not yet
            %%  available.
            %% use one written in pure Erlang
            case inet_res:gethostbyname(Host, Family) of
                {ok, #hostent{h_addr_list = Addrs}} ->
                    hd(Addrs);
                {error, nxdomain} ->
                    %% attempt to guess external IP address enumerating interfaces that are up
                    {ok, Ifs} = inet:getifaddrs(),
                    LocalUp = [proplists:get_value(addr, Opts) || {_, Opts} <- Ifs, lists:member(up, proplists:get_value(flags, Opts, []))],
                    Local = [Valid || Valid <- LocalUp, tuple_size(Valid) =:= AddrLen],
                    %% TODO: localhost should have lower priority
                    hd(Local)
            end
    end.

make_node(Name, Host) ->
    list_to_atom(lists:concat([Name, "@", Host])).
