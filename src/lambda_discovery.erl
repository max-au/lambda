%% @doc
%% Lambda discovery: a way to inject external node name address resolution
%%  into Erlang distribution.
%% Implements a simple mapping, exposed via EPMD API.
%%
%% The release (or erl) should be started with "-epmd_module lambda_discovery".
%% If it was not, lambda_discovery will use inet_db to inject IP addresses into
%%  erl_epmd that is expected to be started.
%%
%% lambda_discovery may start original erl_epmd as a fallback.
%% Use application configuration to disable this behaviour:
%%  {epmd_fallback, false}.
%% @end
-module(lambda_discovery).
-author("maximfca@gmail.com").
-compile(warn_missing_spec).

%% External API
-export([
    start_link/1,
    set_node/2,
    del_node/1,
    get_node/1,
    get_node/0
]).

%% Callbacks required for epmd module implementation
%% Not intended to be used by developers, it is only for OTP kernel application!
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
    handle_info/2,
    handle_continue/2,
    terminate/2
]).

-behaviour(gen_server).

-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% Public API

%% Full set of information needed to establish network connection
%%  to a specific host, with no external services required.
-type address() :: #{
    addr := inet:ip_address(), %% family is clear from IP address size
    port := inet:port_number()
    %% proto => tcp | {tls, [ssl:tls_client_option()]}
}.

%% Internally, hostname can be atom, string, or IP address.
-type hostname() :: atom() | string() | inet:ip_address().

-export_type([address/0]).

%% @doc
%% Starts the server and links it to calling process. Required for
%%  epmd interface, and called when lambda_discovery is started
%%  using "-epmd_module lambda_discovery" command line.
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{epmd_fallback => epmd_fallback()}, []).

%% @doc
%% Starts the server, linked to calling process. Expected to be
%%  used by "lambda" application supervisor.
-spec start_link(lambda) -> {ok, pid()} | ignore | {error, term()}.
start_link(lambda) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{mode => late, epmd_fallback => epmd_fallback()}, []).

%% @doc Sets the mapping between node name and address to access the node.
-spec set_node(node(), address()) -> ok.
set_node(Node, #{addr := _Ip, port := _Port} = Address) when is_atom(Node) ->
    gen_server:call(?MODULE, {set, Node, Address}).

%% @doc Removes the node name from the map. Does nothing if node was never added.
-spec del_node(node()) -> ok.
del_node(Node) ->
    gen_server:call(?MODULE, {del, Node}).

%% @doc Returns mapping of the node name to an address, using the
%%      default selector, or a boolean value requesting erl_epmd
%%      fallback.
-spec get_node(node()) -> address() | boolean().
get_node(Node) ->
    gen_server:call(?MODULE, {get, Node}).

%% @doc Returns this node distribution connectivity info, that is
%%      expected to work when sent to other nodes via distribution
%%      channels. It is expected to return external node address,
%%      if host is behind NAT.
-spec get_node() -> address() | boolean().
get_node() ->
    get_node(node()).

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

port_please(_Name, _Host, _Timeout) ->
    error(notup).

-spec names(Host) -> {ok, [{Name, Port}]} | {error, Reason} when
    Host :: atom() | string() | inet:ip_address(),
    Name :: string(),
    Port :: inet:port_number(),
    Reason :: address | file:posix().

names(HostName) ->
    try gen_server:call(?MODULE, {names, HostName})
    catch
        exit:{noproc, _} ->
            %% this code is necessary for `rebar3 shell` to work with vm.args that
            %%  specify lambda_discovery as -epmd_module. Rebar starts distribution
            %%  dynamically, after checking that epmd is running, but it does not
            %%  understand that epmd may not even be needed. This should be fixed
            %%  in rebar3 at some point.
            {error, {?MODULE, not_running}}
    end.

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
    Host :: hostname(),
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
        #{addr := Addr, port := Port} when AddressFamily =:= inet, tuple_size(Addr) =:= 4 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        #{addr := Addr, port := Port} when AddressFamily =:= inet6, tuple_size(Addr) =:= 8 ->
            {ok, Addr, Port, ?EPMD_VERSION};
        true ->
            erl_epmd:address_please(Name, Host, AddressFamily);
        false ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Server implementation

%% Discovery state: maps node names to addresses.
%% TODO: support multiple endpoints (e.g. inet, inet6, ...)
-record(lambda_discovery_state, {
    nodes = #{} :: #{node() => address()},
    %% erl_epmd started/linked to this process (when fallback is
    %%  requested and lambda_discovery started by net_kernel)
    erl_epmd = undefined :: undefined | pid(),
    %% when distribution is running dynamically, net_kernel process
    %%  is monitored to detect node becoming non-distributed
    net_kernel = undefined :: undefined | pid(),
    %% when epmd_fallback is true, lambda_discovery maintains IP
    %%  address mappings in inet_db
    epmd_fallback :: boolean()
}).

-type state() :: #lambda_discovery_state{}.

%% @private
-spec init(#{mode => late, epmd_fallback := boolean()}) -> {ok, state()}.
init(#{mode := late, epmd_fallback := Fallback}) ->
    %% starting as a normal process (not with "-epmd_module")
    case is_alive() of
        true ->
            %% net_kernel should be listening
            NetKernel = whereis(net_kernel),
            monitor(process, NetKernel),
            {ok, #lambda_discovery_state{nodes = update_local_address(#{}),
                net_kernel = NetKernel, epmd_fallback = Fallback}};
        false ->
            %% not (yet?) alive, need to be aware when node gets distributed, to grab the
            %%  registration details
            ok = net_kernel:monitor_nodes(true),
            {ok, #lambda_discovery_state{epmd_fallback = Fallback}}
    end;

init(#{epmd_fallback := true}) ->
    %% starting by erl_distribution, epmd fallback enabled
    {ok, Pid} = erl_epmd:start_link(),
    %% node will get it's own address during registration
    process_flag(trap_exit, true), %% otherwise terminate/2 is not called
    {ok, #lambda_discovery_state{erl_epmd = Pid, epmd_fallback = true}};

init(#{epmd_fallback := false}) ->
    %% starting by erl_distribution, no epmd fallback
    {ok, #lambda_discovery_state{epmd_fallback = false}}.

%% @private
-spec handle_call(
    {set, node(), address()} |
    {del, node()} | {get, node()} |
    {names, hostname()} |
    {register, string(), inet:port_number(), inet | inet6}, {pid(), reference()}, state()) -> {reply, term(), state()}.
handle_call({set, Node, Address}, _From, #lambda_discovery_state{nodes = Nodes, epmd_fallback = EpmdFallback} = State) ->
    ?LOG_DEBUG("set ~s to ~200p", [Node, Address], #{domain => [lambda]}),
    EpmdFallback andalso inet_db:add_host(hostname(Node), maps:get(addr, Address)),
    {reply, ok, State#lambda_discovery_state{nodes = Nodes#{Node => Address}}};

handle_call({del, Node}, _From, #lambda_discovery_state{nodes = Nodes, epmd_fallback = EpmdFallback} = State) ->
    %% epmd fallback: if erl_epmd was started before
    %%  lambda_discovery, we can still trick erl_epmd into
    %%  using it by adjusting inet_db
    EpmdFallback andalso case maps:find(Node, Nodes) of
                             {ok, #{addr := Ip}} ->
                                 inet_db:del_host(Ip);
                             error ->
                                 ok
                         end,
    {reply, ok, State#lambda_discovery_state{nodes = maps:remove(Node, Nodes)}};

handle_call({get, Node}, _From, #lambda_discovery_state{nodes = Nodes, epmd_fallback = EpmdFallback} = State) ->
    ?LOG_DEBUG("~p asking for ~s (~200p)", [element(1, _From),
        case Node =:= node() of true -> "self"; _ -> Node end, maps:get(Node, Nodes, error)], #{domain => [lambda]}),
    {reply, maps:get(Node, Nodes, EpmdFallback), State};

handle_call({names, HostName}, _From, #lambda_discovery_state{nodes = Nodes} = State) ->
    %% find all Nodes of a HostName - need to iterate the entire node
    %%  map. This is a very rare request, so can be slow
    Filtered = lists:filter(
        fun (Full) ->
            hostname(Full) =:= HostName
        end, maps:keys(Nodes)),
    {reply, Filtered, State};

handle_call({register, Name, PortNo, Family}, _From, #lambda_discovery_state{erl_epmd = ErlEpmd} = State) ->
    ?LOG_DEBUG("registering ~s (~s) port ~b", [Name, Family, PortNo], #{domain => [lambda]}),
    %% when erl_epmd is started by lambda_discovery, need to proxy the registration
    Creation =
        case ErlEpmd of
            undefined ->
                1;
            _Pid ->
                try
                    {ok, Creation1} = erl_epmd:register_node(Name, PortNo, Family),
                    Creation1
                catch Class:Reason ->
                    ?LOG_ERROR("epmd registration fallback failed with ~s:~p", [Class, Reason], #{domain => [lambda]}),
                    1
                end
        end,
    {reply, {ok, Creation}, State, {continue, {register, PortNo, Family}}}.

-spec handle_continue({register, inet:port_number(), inet | inet6}, state()) -> {noreply, state()}.
handle_continue({register, PortNo, Family}, #lambda_discovery_state{nodes = Nodes} = State) ->
    %% populate the local node address
    {ok, Host} = inet:gethostname(),
    Addr = local_addr(Host, Family),
    Node = net_kernel:nodename(),
    {noreply, State#lambda_discovery_state{nodes = Nodes#{Node => #{addr => Addr, port => PortNo, family => Family}}}}.

%% @private
-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Cast, _State) ->
    error(badarg).

%% @private
-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({nodeup, Node}, #lambda_discovery_state{nodes = Nodes, net_kernel = undefined} = State) when Node =:= node() ->
    net_kernel:monitor_nodes(false),
    NetKernel = whereis(net_kernel),
    _MRef = monitor(process, NetKernel),
    {noreply, State#lambda_discovery_state{nodes = update_local_address(Nodes), net_kernel = NetKernel}};
handle_info({'DOWN', _MRef, process, NetKernel, _Reason}, #lambda_discovery_state{net_kernel = NetKernel} = State) ->
    net_kernel:monitor_nodes(true),
    {noreply, State#lambda_discovery_state{net_kernel = undefined}};
%% ignore "late sends", when monitor_nodes(false) wasn't fast enough to switch it off
handle_info({UpDown, _Node}, State) when UpDown =:= nodeup; UpDown =:= nodedown ->
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> ok.
terminate(_Reason, #lambda_discovery_state{erl_epmd = ErlEpmd}) when is_pid(ErlEpmd) ->
    gen:stop(ErlEpmd).

%%--------------------------------------------------------------------
%% Internal implementation

update_local_address(Nodes) ->
    {state, _Node, _ShortLong, _Tick, _, _SysDist, _, _, _,
        [{listen, _, _Proc, {net_address, {_Ip, Port}, _HostName, _Proto, Family}, _Mod}],
        _, _, _, _, _} = sys:get_state(net_kernel),
    {ok, #hostent{h_addr_list = [Ip | _]}} = inet:gethostbyname(hostname(node()), Family),
    Nodes#{node() => #{addr => Ip, port => Port}}.

hostname(Node) ->
    [_, Host] = string:lexemes(atom_to_list(Node), "@"),
    Host.

epmd_fallback() ->
    application:get_env(lambda, epmd_fallback, true).

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
