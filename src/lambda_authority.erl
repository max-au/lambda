%% @doc
%% Lambda authority: process groups implementation,
%%  providing subscription-based mechanism to update group membership.
%%
%% Every authority keeps the state independently of all other authorities.
%% Client is responsible for merging the responses.
%%
%% @end
-module(lambda_authority).
-author("maximfca@gmail.com").

%% API
-export([
    start/1,
    start_link/1,
    authorities/0,
    registries/0
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
%% @doc
%% Starts the server outside of supervision hierarchy.
%% Useful for testing in conjunction with peer.
-spec start(gen:emgr_name()) -> gen:start_ret().
start(BootProc) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [BootProc], []).

%% @doc
%% Starts the server and links it to calling process.
-spec start_link(gen:emgr_name()) -> gen:start_ret().
start_link(BootProc) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [BootProc], []).

%%--------------------------------------------------------------------
%% API

%% @doc returns a list of authorities currently connected.
authorities() ->
    gen_server:call(?MODULE, authorities).

%% @doc returns a list of registries currently connected to this
%%      authority.
registries() ->
    gen_server:call(?MODULE, registries).

%%--------------------------------------------------------------------
%% Cluster authority

%% How many remote exchanges is considered to be a healthy amount.
-define (REMOTE_EXCHANGE_REDUNDANCY_FACTOR, 2).

-record(lambda_authority_state, {
    %% keeping address of "self" cached
    self :: lambda_epmd:address(),
    %% modules known to the authority, mapped to exchanges
    exchanges = #{} :: #{module() => {Local :: pid(), Remote :: [pid()]}},
    %% other authorities. When a new node comes up, it is expected to
    %%  eventually connect to all authorities.
    authorities = #{} :: #{pid() => lambda_epmd:address()},
    %% brokers connected
    brokers = #{} :: #{pid() => lambda_epmd:address()}
}).

-type state() :: #lambda_authority_state{}.

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: broker " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

-spec init([gen:emgr_name()]) -> {ok, state()}.
init([BootProc]) ->
    Authorities = lambda_bootstrap:bootstrap(BootProc),
    %% bootstrap discovery. Race condition possible, when all nodes
    %%  start at once, and never retry.
    Self = lambda_epmd:get_node(),
    maps:map(
        fun (Location, Addr) ->
            ok = lambda_epmd:set_node(Location, Addr),
            Location ! {authority, self(), Self}
        end, Authorities),
    ?dbg("AUTH discovering ~200p", [Authorities]),
    {ok, #lambda_authority_state{self = Self}}.

handle_call(authorities, _From, #lambda_authority_state{authorities = Auth} = State) ->
    {reply, maps:keys(Auth), State};

handle_call(registries, _From, #lambda_authority_state{brokers = Regs} = State) ->
    {reply, maps:keys(Regs), State}.

handle_cast(_Cast, _State) ->
    error(notsup).

%% Broker requesting exchanges for a Module
handle_info({exchange, Module, Broker}, #lambda_authority_state{} = State) ->
    %% are there enough exchanges for this module?
    {Exch, State1} = ensure_exchange(Module, State),
    %% simulate a reply
    Broker ! {exchange, Module, Exch},
    %% subscribe broker to all updates to exchanges for the module
    {noreply, State1#lambda_authority_state{}};

%% authority discovered by another Authority
handle_info({authority, Peer, _Addr}, State) when Peer =:= self() ->
    %% discovered self
    {noreply, State};
handle_info({authority, Peer, Addr}, #lambda_authority_state{self = Self, authorities = Auth, brokers = Regs} = State)
    when not is_map_key(Peer, Auth) ->
    io:format(standard_error, "AUTH ANOTHER ~p ~200p", [Peer, Addr]),
    _MRef = monitor(process, Peer),
    %% exchange known registries - including ourself!
    erlang:send(Peer, {quorum, Auth#{self() => Self}, Regs}, [noconnect]),
    {noreply, State#lambda_authority_state{authorities = Auth#{Peer => peer_addr(Addr)}}};

%% authority discovered by a Broker
handle_info({discover, Broker, Addr}, #lambda_authority_state{self = Self, authorities = Auth, brokers = Regs} = State) ->
    ?dbg("AUTH BEING DISCOVERED by ~s (~200p) ~200p", [node(Broker), Broker, Addr]),
    _MRef = monitor(process, Broker),
    %% send self, and a list of other authorities to discover
    erlang:send(Broker, {authority, self(), Self, Auth}, [noconnect]),
    {noreply, State#lambda_authority_state{brokers = Regs#{Broker => peer_addr(Addr)}}};

%% exchanging information from another Authority
handle_info({quorum, MoreAuth, MoreRegs}, #lambda_authority_state{self = Self, authorities = Others, brokers = Regs} = State) ->
    ?dbg("AUTH QUORUM ~200p ~200p", [MoreAuth, MoreRegs]),
    %% merge authorities.
    UpdatedAuth = maps:fold(
        fun (NewAuth, _Addr, Existing) when is_map_key(NewAuth, Existing) ->
                Existing;
            (NewAuth, Addr, Existing) ->
                _MRef = erlang:monitor(process, NewAuth),
                Existing#{NewAuth => Addr}
        end, Others, MoreAuth),
    %% merge registries, notifying newly discovered
    SelfAuthMsg = {authority, self(), Self, Others},
    UpdatedReg = maps:fold(
        fun (Reg, _Addr, ExReg) when is_map_key(Reg, ExReg) ->
                ExReg;
            (Reg, Addr, ExReg) ->
                lambda_epmd:set_node(node(Reg), Addr), %% TODO: may need some overload protection for mass exchange
                _MRef = erlang:monitor(process, Reg),
                %% don't suspend the authority to avoid lock-up when dist connection busy
                %% TODO: figure out how dist can be busy when we have never sent anything before
                ok = erlang:send(Reg, SelfAuthMsg, [nosuspend]),
                ExReg#{Reg => Addr}
        end, Regs, MoreRegs),
    {noreply, State#lambda_authority_state{authorities = UpdatedAuth, brokers = UpdatedReg}};

%% Handling disconnects from authorities and registries
handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_authority_state{authorities = Auth, brokers = Regs} = State) ->
    %% it is far more common to have Broker disconnect, and not Authority
    case maps:take(Pid, Regs) of
        {_, NewRegs} ->
            {noreply, State#lambda_authority_state{brokers = NewRegs}};
        error ->
            case maps:take(Pid, Auth) of
                {_, NewAuth} ->
                    {noreply, State#lambda_authority_state{authorities = NewAuth}};
                error ->
                    %% most likely some stale reply
                    {noreply, State}
            end
    end.

%%--------------------------------------------------------------------
%% Internal implementation

ensure_exchange(Module, #lambda_authority_state{exchanges = Exch} = State) ->
    case maps:find(Module, Exch) of
        {ok, {Local, Remote}} when is_pid(Local) ->
            {[Local | Remote], State};
        {ok, {undefined, Remote}} when length(Remote) >= ?REMOTE_EXCHANGE_REDUNDANCY_FACTOR ->
            {Remote, State};
        {ok, {undefined, Remote}} ->
            %% not enough remote exchanges, start one locally
            {ok, New} = lambda_exchange:start_link(Module), % %% TODO: avoid crashing authority here?
            {[New | Remote], State#lambda_authority_state{exchanges = Exch#{Module => {New, Remote}}}};
        error ->
            %% no exchanges at all
            {ok, New} = lambda_exchange:start_link(Module), %% TODO: avoid crashing authority here?
            {[New], State#lambda_authority_state{exchanges = Exch#{Module => {New, []}}}}
    end.

peer_addr({ip, Addr, Port}) ->
    {ip, Addr, Port};
peer_addr({epmd, Hostname}) ->
    {epmd, Hostname};
peer_addr({Node, Port}) when is_atom(Node), is_integer(Port), Port > 0, Port < 65536 ->
    Socket = ets:lookup_element(sys_dist, Node, 6),
    {ok, {Ip, _EphemeralPort}} = inet:peername(Socket), %% TODO: TLS support
    case Ip of
        V4 when tuple_size(V4) =:= 4 -> {inet, Ip, Port};
        V6 when tuple_size(V6) =:= 8 -> {inet6, Ip, Port}
    end.
