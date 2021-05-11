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
%%  epmd interface.
-spec start_link() -> {ok, pid()} | ignore | {error,term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Sets the mapping between node name and address to access the node.
-spec set_node(node(), address()) -> ok.
set_node(Node, #{addr := _Ip, port := _Port} = Address) when is_atom(Node) ->
    gen_server:call(?MODULE, {set, Node, Address}).

%% @doc Removes the node name from the map. Does nothing if node was never added.
-spec del_node(node()) -> ok.
del_node(Node) ->
    gen_server:call(?MODULE, {del, Node}).

%% @doc Returns mapping of the node name to an address, using the
%%      default selector.
-spec get_node(node()) -> address() | error.
get_node(Node) ->
    gen_server:call(?MODULE, {get, Node}).

%% @doc Returns this node distribution connectivity info, that is
%%      expected to work when sent to other nodes via distribution
%%      channels. It is expected to return external node address,
%%      if host is behind NAT.
-spec get_node() -> address() | error.
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
        error ->
            case epmd_fallback() of
                true ->
                    erl_epmd:address_please(Name, Host, AddressFamily);
                false ->
                    {error, not_found}
            end
    end.

%%--------------------------------------------------------------------
%% Server implementation

%% Discovery state: maps node names to addresses.
%% TODO: support multiple endpoints (e.g. inet, inet6, ...)
-type state() :: #{node() => address()}.

%% @private
-spec init([]) -> {ok, state()}.
init([]) ->
    InitialState =
        case epmd_fallback() of
            true ->
                %% epmd fallback enabled
                case erl_epmd:start_link() of
                    {ok, Pid} ->
                        %% node will get it's own address during registration
                        process_flag(trap_exit, true), %% otherwise terminate/2 is not called
                        put(erl_epmd, Pid),
                        #{};
                    {error, {already_started, _Pid}} ->
                        %% may be already registered, need to ask net_kernel
                        try
                            {state, _Node, _ShortLong, _Tick, _, _SysDist, _, _, _,
                                [{listen, _, _Proc, {net_address, {_Ip, Port}, _HostName, _Proto, Family}, _Mod}],
                                _, _, _, _, _} = sys:get_state(net_kernel),
                            {ok, #hostent{h_addr_list = [Ip | _]}} = inet:gethostbyname(hostname(node()), Family),
                            #{node() => #{addr => Ip, port => Port}}
                        catch _:_ ->
                            %% distribution not started yet
                            #{}
                        end
                end;
            false ->
                #{}
        end,
    {ok, InitialState}.

%% @private
-spec handle_call(
    {set, node(), address()} |
    {del, node()} | {get, node()} |
    {names, hostname()} |
    {register, string(), inet:port_number(), inet | inet6}, {pid(), reference()}, state()) -> {reply, term(), state()}.
handle_call({set, Node, Address}, _From, State) ->
    ?LOG_DEBUG("set ~s to ~200p", [Node, Address], #{domain => [lambda]}),
    epmd_fallback() andalso inet_db:add_host(hostname(Node), maps:get(addr, Address)),
    {reply, ok, State#{Node => Address}};

handle_call({del, Node}, _From, State) ->
    %% epmd fallback: if erl_epmd was started before
    %%  lambda_discovery, we can still trick erl_epmd into
    %%  using it by adjusting inet_db
    epmd_fallback() andalso case maps:find(Node, State) of
                                {ok, #{addr := Ip}} ->
                                    inet_db:del_host(Ip);
                                error ->
                                    ok
                            end,
    {reply, ok, maps:remove(Node, State)};

handle_call({get, Node}, _From, State) ->
    ?LOG_DEBUG("~p asking for ~s (~200p)", [element(1, _From),
        case Node =:= node() of true -> "self"; _ -> Node end, maps:get(Node, State, error)], #{domain => [lambda]}),
    {reply, maps:get(Node, State, error), State};

handle_call({names, HostName}, _From, State) ->
    %% find all Nodes of a HostName - need to iterate the entire node
    %%  map. This is a very rare request, so can be slow
    Nodes = lists:filter(
        fun (Full) ->
            hostname(Full) =:= HostName
        end, maps:keys(State)),
    {reply, Nodes, State};

handle_call({register, Name, PortNo, Family}, _From, State) ->
    ?LOG_DEBUG("registering ~s (~s) port ~bp", [Name, Family, PortNo], #{domain => [lambda]}),
    %% fallback, registering in epmd
    Creation =
        case epmd_fallback() of
            true ->
                try
                    {ok, Creation1} = erl_epmd:register_node(Name, PortNo, Family),
                    Creation1
                catch Class:Reason ->
                    ?LOG_ERROR("epmd fallback failed with ~s:~p", [Class, Reason], #{domain => [lambda]}),
                    1
                end;
            false ->
                1
        end,
    {reply, {ok, Creation}, State, {continue, {register, PortNo, Family}}}.

-spec handle_continue({register, inet:port_number(), inet | inet6}, state()) -> {noreply, state()}.
handle_continue({register, PortNo, Family}, State) ->
    %% populate the local node address
    {ok, Host} = inet:gethostname(),
    Addr = local_addr(Host, Family),
    {noreply, State#{node() => #{addr => Addr, port => PortNo, family => Family}}}.

%% @private
-spec handle_cast(term(), state()) -> no_return().
handle_cast(_Cast, _State) ->
    error(badarg).

%% @private
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    epmd_fallback() andalso get(erl_epmd) =/= undefined andalso
        gen:stop(get(erl_epmd)),
    ok.

%%--------------------------------------------------------------------
%% Internal implementation

hostname(Node) ->
    tl(string:lexemes(atom_to_list(Node), "@")).

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
