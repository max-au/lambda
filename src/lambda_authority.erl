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
-spec start(Neighbours :: [lambda_registry:point()]) -> gen:start_ret().
start(Neighbours) ->
    gen_server:start({local, ?MODULE}, ?MODULE, [Neighbours], []).

%% @doc
%% Starts the server and links it to calling process.
-spec start_link([lambda_registry:point()]) -> gen:start_ret().
start_link(Neighbours) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Neighbours], []).

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

-type name() :: term().

-record(lambda_authority_state, {
    %% keeping address of "self" cached
    self :: lambda_epmd:address(),
    %% name => processes mapping. For every name, a list of
    %%  processes registered under this name is maintained, and a
    %%  list of processes listening for updates to the name.
    registry = #{} :: #{name() => {Pub :: [pid()], Sub :: [pid()]}},
    %% other authorities. When a new node comes up, it is expected to
    %%  eventually connect to all authorities.
    authorities = #{} :: #{pid() => lambda_epmd:address()},
    %% non-authority peers and their addresses
    registries = #{} :: #{pid() => lambda_epmd:address()}
}).

-type state() :: #lambda_authority_state{}.

-spec init([Neighbours :: lambda_registry:bootstrap()]) -> {ok, state()}.
init([Neighbours]) ->
    %% bootstrap discovery. Race condition possible, when all nodes
    %%  start at once, and never retry.
    {ok, Self} = lambda_epmd:get_node(node()),
    maps:map(
        fun (Location, Addr) ->
            ok = lambda_epmd:set_node(Location, Addr),
            Location ! {authority, self(), Self}
        end, Neighbours),
    {ok, #lambda_authority_state{self = Self}}.

handle_call(authorities, _From, #lambda_authority_state{authorities = Auth} = State) ->
    {reply, maps:keys(Auth), State};
handle_call(registries, _From, #lambda_authority_state{registries = Regs} = State) ->
    {reply, maps:keys(Regs), State}.

handle_cast(_Cast, _State) ->
    error(badarg).

%% authority discovered by another Authority
handle_info({authority, Peer, Addr}, #lambda_authority_state{self = Self, authorities = Auth, registries = Regs} = State) ->
    _MRef = monitor(process, Peer),
    %% exchange known registries - including ourself!
    erlang:send(Peer, {exchange, Auth#{self() => Self}, Regs}, [noconnect]),
    {noreply, State#lambda_authority_state{authorities = Auth#{Peer => peer_addr(Addr)}}};

%% authority discovered by Registry
handle_info({discover, Peer, Addr}, #lambda_authority_state{self = Self, authorities = Auth, registries = Regs} = State) ->
    _MRef = monitor(process, Peer),
    %% send self, and a list of other authorities to discover
    erlang:send(Peer, {authority, self(), Self, Auth}, [noconnect]),
    {noreply, State#lambda_authority_state{registries = Regs#{Peer => peer_addr(Addr)}}};

%% exchanging information from another Authority
handle_info({exchange, MoreAuth, MoreRegs}, #lambda_authority_state{self = Self, authorities = Others, registries = Regs} = State) ->
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
                _MRef = erlang:monitor(process, Reg),
                %% don't suspend the authority to avoid lock-up when dist connection busy
                erlang:send(Reg, SelfAuthMsg, [nosuspend]),
                ExReg#{Reg => Addr}
        end, Regs, MoreRegs),
    {noreply, State#lambda_authority_state{authorities = UpdatedAuth, registries = UpdatedReg}};

%% Handling disconnects from authorities and registries
handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_authority_state{authorities = Auth, registries = Regs} = State) ->
    %% it is far more common to have Registry disconnect, and not Authority
    case maps:take(Pid, Regs) of
        {_, NewRegs} ->
            {noreply, State#lambda_authority_state{registries = NewRegs}};
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
