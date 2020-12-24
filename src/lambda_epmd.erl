%% @doc
%% Lambda discovery: a way to inject external node name address resolution
%%  into Erlang distribution.
%% Implements a simple mapping, exposed via EPMD API.
%% @end
-module(lambda_epmd).
-author("maximfca@gmail.com").

%% API
-export([
    replace/0,
    restore/0,
    set_node/2,
    del_node/1,
    get_node/1
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
    handle_cast/2
]).

%% Erlang node address. Expected to be extended to add DNS and other discovery layers.
-type address() ::
    {ip, inet:ip_address(), inet:port_number()} |   %% IP address and port number
    {dns, string(), inet:port_number()} |           %% DNS name and port number
    {epmd, string()}.                               %% DNS name, epmd

-export_type([address/0]).

-behaviour(gen_server).

-include_lib("kernel/include/inet.hrl").

%%--------------------------------------------------------------------
%% @doc Test/debug only: replaces running erl_epmd. Should only be used
%%      for cases when it is not possible to control command line, so
%%      -epmd_module argument cannot be overridden.
-spec replace() -> pid().
replace() ->
    %% if erl_epmd is already running, start the additional server (runs separately from erl_epmd)
    Pid = case whereis(erl_epmd) of
              undefined ->
                  undefined;
              Pid1 when is_pid(Pid1) ->
                  {ok, Pid2} = gen_server:start({local, ?MODULE}, ?MODULE, [], []),
                  Pid2
          end,
    %% ALERT: Code below is doing a dirty trick:
    %%  reload binary code for this module renamed to "erl_epmd"
    EpmdMod = erl_epmd,
    {Mod, Binary, _Filename} = code:get_object_code(?MODULE),
    {ok, {Mod, [{abstract_code, {_, Forms}}]}} = beam_lib:chunks(Binary, [abstract_code]),
    Expanded = erl_expand_records:module(Forms, [strict_record_tests]),
    Replaced = [
        case Form of
            {attribute, Ln, module, ?MODULE} ->
                {attribute, Ln, module, EpmdMod};
            Other ->
                Other
        end || Form <- Expanded],
    {ok, EpmdMod, Bin} = compile:forms(Replaced),
    true = code:unstick_mod(EpmdMod),
    {module, EpmdMod} = code:load_binary(EpmdMod, code:which(EpmdMod), Bin),
    Pid.

%%--------------------------------------------------------------------
%% @doc Test/debug only: restores erl_epmd previously running to revert
%%      previous 'replace' operation
-spec restore() -> true.
restore() ->
    %% if this server is running, stop it
    is_pid(whereis(?MODULE)) andalso gen_server:stop(?MODULE),
    %% ALERT: Code below is for recovering after hacky replace()
    BeamFile = filename:join(code:lib_dir(kernel, ebin), "erl_epmd"),
    {module, erl_epmd} = code:load_abs(BeamFile, erl_epmd),
    true = code:stick_mod(erl_epmd).


%% @doc
%% Starts the server and links it to calling process. Required for
%%  epmd interface.
-spec start_link() -> {ok, pid()} | ignore | {error,term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Sets the mapping between node name and address to access the node.
-spec set_node(node() | pid(), address()) -> ok.
set_node(Pid, Address) when is_pid(Pid) ->
    set_node(node(Pid), Address);
set_node({RegName, Node}, Address) when is_atom(RegName), is_atom(Node) ->
    set_node(Node, Address);
set_node(Node, {ip, _Ip, _Port} = Address) ->
    gen_server:call(?MODULE, {set, Node, Address});
set_node(Node, {epmd, Hostname} = Address) when is_list(Hostname) ->
    gen_server:call(?MODULE, {set, Node, Address}).

%% @doc Removes the node name from the map. Does nothing if node was never added.
-spec del_node(node()) -> ok.
del_node(Node) ->
    gen_server:call(?MODULE, {del, Node}).

%% @doc Returns mapping of the node name to an address.
-spec get_node(node()) -> address() | error.
get_node(Node) ->
    gen_server:call(?MODULE, {get, Node}).

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
        {ip, Addr, Port} when AddressFamily =:= inet, tuple_size(Addr) =:= 4 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        {ip, Addr, Port} when AddressFamily =:= inet6, tuple_size(Addr) =:= 8 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        error ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Server implementation

%% EPMD state: maps node names to addresses.
%% Current limitation: a node can only have a single address,
%%  either IPv4 or IPv6.
-type state() :: #{node() => address()}.

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #{}}.

handle_call({set, Node, Address}, _From, State) ->
    {reply, ok, State#{Node => Address}};

handle_call({del, Node}, _From, State) ->
    {reply, ok, maps:remove(Node, State)};

handle_call({get, Node}, _From, State) ->
    {reply, case maps:find(Node, State) of {ok, Ret} -> Ret; error -> error end, State};

handle_call({names, HostName}, _From, State) ->
    %% find all Nodes of a HostName - need to iterate the entire node
    %%  map. This is a very rare request, so can be slow
    Nodes = lists:filter(fun (Full) -> tl(string:lexemes(Full, "@")) =:= HostName end, maps:keys(State)),
    {reply, Nodes, State};

handle_call({register, Name, PortNo, Family}, _From, State) ->
    %% get the local hostname
    {ok, Host} = inet:gethostname(),
    Domain = case inet_db:res_option(domain) of [] -> []; D -> [$. | D] end,
    Node = make_node(Name, Host ++ Domain),
    Addr =
        case application:get_env(kernel, inet_dist_use_interface) of
            {ok, Addr1} when Family =:= inet, tuple_size(Addr1) =:= 4 ->
                Addr1;
            {ok, Addr1} when Family =:= inet6, tuple_size(Addr1) =:= 8 ->
                Addr1;
            undefined ->
                %% attempt to guess external IP address:
                %% {ok, Ifs} = inet:getifaddrs(),
                %% LocalUp = [proplists:get_value(addr, Opts) || {_, Opts} <- Ifs, lists:member(up, proplists:get_value(flags, Opts, []))],
                %% Local = [Valid || Valid <- LocalUp, is_list(inet:ntoa(Valid))],
                %% native resolver cannot be used, so use one written in pure Erlang
                {ok, #hostent{h_addr_list = Addrs}} = inet_res:gethostbyname(Host, Family),
                hd(Addrs)
        end,
    {reply, {ok, 1}, State#{Node => {ip, Addr, PortNo}}}.

handle_cast(_Cast, _State) ->
    error(badarg).

%%--------------------------------------------------------------------
%% Internal implementation

make_node(Name, Host) ->
    list_to_atom(lists:concat([Name, "@", Host])).