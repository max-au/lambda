%% @doc
%% Lambda authority: process keeping track of all connected brokers and
%%  other authorities. When a broker needs to find an exchange to trade
%%  a module, it sends 'exchange' query to all known authorities,
%%  and every authority responds with known list of exchanges.
%%
%% If authority does not have enough known exchanges, it will start one
%%  locally, provides that erlang:phash2({node(), Module}) of this
%%  authority process sorts below threshold from all known authorities.
%%
%% It is expected that all authorities are connected to each other,
%%  forming a full mesh between processes (and corresponding nodes).
%% Authority remembers connectivity information for all other authorities
%%  and attempts to re-discover lost authorities (but not brokers).
%%
%% When an authority discovers a peer, it also updates all known
%%  brokers with the new authority list (eventually making all brokers
%%  to be connected to all authorities, NB: this may be partitioned
%%  in the future).
%%
%% There is an initial bootstrapping process when authority does not
%%  know any other authority and needs to find some. This information
%%  can be fed to authority (or broker) via bootstrapping API.
%%
%% Authority message exchange protocol:
%% * authority -> authority:
%% * authority -> broker:
%% * broker -> authority:
%% * bootstrap -> authority: provide a mapping for bootstrapping
%%
%%
%% @end
-module(lambda_authority).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/0,
    authorities/1,
    brokers/1,
    peers/2
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
%% Starts the server and links it to calling process. Registers a local
%%  process name.
-spec start_link() -> {ok, pid()} | {error, {already_started, pid()}}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API

%% @doc returns a list of authorities currently connected.
-spec authorities(lambda:dst()) -> [pid()].
authorities(Authority) ->
    gen_server:call(Authority, authorities).

%% @doc returns a list of brokers currently connected to this
%%      authority.
-spec brokers(lambda:dst()) -> [pid()].
brokers(Authority) ->
    gen_server:call(Authority, brokers).

%% @doc adds a map of potential authority peers
-spec peers(lambda:dst(), #{lambda_discovery:location() => lambda_discovery:address()}) -> ok.
peers(Authority, Peers) ->
    Authority ! {peers, Peers},
    ok.

%%--------------------------------------------------------------------
%% Cluster authority

%% How many remote exchanges is considered to be a healthy amount.
-define (REMOTE_EXCHANGE_REDUNDANCY_FACTOR, 2).

-record(lambda_authority_state, {
    %% modules known to the authority, mapped to exchanges
    exchanges = #{} :: #{module() => {Local :: pid(), Remote :: [pid()]}},
    %% all known authorities, including self. When a new node comes up, it is expected to
    %%  eventually connect to all authorities.
    authorities :: #{pid() => lambda_discovery:address()},
    %% brokers connected
    brokers = #{} :: #{pid() => lambda_discovery:address()}
}).

-type state() :: #lambda_authority_state{}.

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #lambda_authority_state{authorities = #{self() => lambda_discovery:get_node()}}}.

handle_call(authorities, _From, #lambda_authority_state{authorities = Auth} = State) ->
    {reply, maps:keys(Auth), State};

handle_call(brokers, _From, #lambda_authority_state{brokers = Brokers} = State) ->
    {reply, maps:keys(Brokers), State}.

handle_cast(_Req, _State) ->
    erlang:error(notsup).

%% bootstrap: try to discover not yet known Peers
handle_info({peers, Peers}, #lambda_authority_state{authorities = Auth} = State) ->
    ?LOG_DEBUG("NEW PEERS ~200p", [maps:without(maps:keys(Auth), Peers)], #{domain => [lambda]}),
    discover(Auth, Peers),
    {noreply, State};

%% syn: initiator is discovering us, sending a list of Peers known to it
handle_info({syn, Initiator, Peers}, #lambda_authority_state{authorities = Auth} = State) ->
    ?LOG_DEBUG("SYN from ~p ~200p (brokers ~200p)", [Initiator, Peers, maps:keys(State#lambda_authority_state.brokers)], #{domain => [lambda]}),
    %% remember initiator
    _MRef = monitor(process, Initiator),
    Contact = maps:get(Initiator, Peers),
    %% acknowledge: send our own view to initiator
    Initiator ! {ack, self(), Auth},
    %% initiator may have sent us a few more peers we are not aware about
    NewAuth = Auth#{Initiator => Contact},
    discover(NewAuth, Peers),
    %% tell known brokers to look for a new authority
    [lambda_broker:authorities(Broker, #{Initiator => Contact})
        || Broker <- maps:keys(State#lambda_authority_state.brokers)],
    {noreply, State#lambda_authority_state{authorities = NewAuth}};

%% ack: we are the initiator
handle_info({ack, Origin, Peers}, #lambda_authority_state{authorities = Auth} = State) ->
    case is_map_key(Origin, Auth) orelse Origin =:= self() of
        true ->
            ?LOG_DEBUG("PEER KNOWN ~p", [if Origin =:= self() -> "self"; true -> Origin end], #{domain => [lambda]}),
            {noreply, State};
        false ->
            %% new peer authority
            ?LOG_DEBUG("ACK ~p (known ~200p)", [Origin, Peers], #{domain => [lambda]}),
            _MRef = monitor(process, Origin),
            Contact = maps:get(Origin, Peers),
            %% notify known brokers about new Authority
            [lambda_broker:authorities(Broker, #{Origin => Contact})
                || Broker <- maps:keys(State#lambda_authority_state.brokers)],
            {noreply, State#lambda_authority_state{authorities = Auth#{Origin => Contact}}}
    end;

%% authority discovered by a Broker
handle_info({discover, Broker, Addr}, #lambda_authority_state{authorities = Auth, brokers = Brokers} = State) ->
    ?LOG_DEBUG("BROKER from ~s (~200p) ~200p", [node(Broker), Broker, Addr], #{domain => [lambda]}),
    Self = maps:get(self(), Auth),
    _MRef = monitor(process, Broker),
    %% send self, and a list of other authorities to discover
    erlang:send(Broker, {authority, self(), Self, Auth}, [noconnect]),
    {noreply, State#lambda_authority_state{brokers = Brokers#{Broker => peer_addr(Addr)}}};

%% Broker requesting exchanges for a Module
handle_info({exchange, Module, Broker}, #lambda_authority_state{} = State) ->
    %% are there enough exchanges for this module?
    {Exch, State1} = ensure_exchange(Module, State),
    %% simulate a reply
    Broker ! {exchange, Module, Exch},
    %% subscribe broker to all updates to exchanges for the module
    {noreply, State1#lambda_authority_state{}};

%% broker cancels a subscription for module exchange
handle_info({cancel, _Module, _Broker}, #lambda_authority_state{exchanges = Exchanges} = State) ->
    ?LOG_DEBUG("cancel ~s subscription for ~s (~200p)", [_Module, node(_Broker), _Broker], #{domain => [lambda]}),
    %% send self, and a list of other authorities to discover
    {noreply, State#lambda_authority_state{exchanges = Exchanges}};

%% Handling disconnects from authorities and brokers
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

discover(Existing, New) ->
    maps:map(
        fun (Location, _Addr) when is_map_key(Location, Existing) ->
                ok;
            (Location, Addr) ->
                ok = lambda_discovery:set_node(Location, Addr),
                Location ! {syn, self(), Existing}
        end, New).

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

peer_addr({Addr, Port}) when is_tuple(Addr), is_integer(Port), Port > 0, Port < 65536 ->
    {Addr, Port};
peer_addr({Node, Port}) when is_atom(Node), is_integer(Port), Port > 0, Port < 65536 ->
    Socket = ets:lookup_element(sys_dist, Node, 6),
    {ok, {Ip, _EphemeralPort}} = inet:peername(Socket), %% TODO: TLS support
    case Ip of
        V4 when tuple_size(V4) =:= 4 -> {Ip, Port};
        V6 when tuple_size(V6) =:= 8 -> {Ip, Port}
    end;
peer_addr(not_distributed) ->
    not_distributed.
