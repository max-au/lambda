%% @doc
%% Lambda channel.
%% Drawing analogy, lambda channel is the TCP connection.
%% Lambda server accepts a new connection, spawning a new
%%  lambda channel (server side). From the client side, lambda_plb
%%  keeps track of multiple open connections.
%%
%% Channel can be in one of two states:
%% * open (tokens > 0). In this state channel can receive
%%   requests (jobs/started)
%% * blocked (tokens = 0). In this state channel does not
%%   expect to receive any requests, and will terminate
%%   if it happens.
%%
%% Channel message exchange protocol:
%% * plb -> channel: request (either job or started)
%% * worker -> channel: request complete (worker done)
%% * plb -> channel: client closed connection
%% * channel -> plb send demand to client
%% * channel -> plb/server: terminate
%%
%% Additional logic: channel sends demand to plb when tokens
%%  are down to low watermark.
%% @end
-module(lambda_channel).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/2,
    start_link/3
]).

-behaviour(gen_server).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal export (for remote spawn mode)
-export([handle/4, handle_erpc/4]).

-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% API implementation

start_link(Module, Capacity) ->
    %% by default, low watermark is 1/2 of requested capacity.
    %% This may create "wave" effect: when exactly half of the capacity
    %%  is currently being taken with very slow requests, all subsequent
    %%  request may be blocked until at least one slow request completes.
    start_link(Module, Capacity, Capacity div 2).

start_link(Module, Capacity, Watermark) ->
    gen_server:start_link(?MODULE, [Module, Capacity, Watermark], []).

%%-----------------------------------------------------------------
%% gen_server implementation

-record(lambda_channel_state, {
    %% client (plb) to send demand to
    to :: pid(),
    %% capacity (tokens): how many requests can be made without
    %%  additional demand
    capacity :: pos_integer(),
    %% how many tokens left in the current connection
    tokens :: non_neg_integer(),
    %% low watermark: send demand when less than X tokens left
    low_watermark = 0 :: non_neg_integer(),
    %% how many requests are currently being executed
    %% TODO: maybe remove?
    in_flight = 0 :: non_neg_integer(),
    %% how many requests were executed in total (by this connection)
    %% TODO: maybe replace with some counter
    total = 0 :: non_neg_integer()
}).

init([To, Capacity, Watermark]) ->
    erlang:monitor(process, To),
    demand(To, Capacity),
    {ok, #lambda_channel_state{to = To, capacity = Capacity, tokens = Capacity, low_watermark = Watermark}}.

%% old-fashion job invoked via message
%% Technically simulates just the very same thing erpc does
handle_call({job, M, F, A}, From, #lambda_channel_state{in_flight = InFlight} = State) ->
    erlang:spawn_monitor(?MODULE, handle_erpc, [From, M, F, A]),
    {noreply, State#lambda_channel_state{in_flight = InFlight + 1}};

handle_call(get_count, _From, #lambda_channel_state{in_flight = InFlight, total = Total} = State) ->
    {reply, {InFlight, Total}, State}.

handle_cast(_Request, _State) ->
    erlang:error(notsup).

handle_info({started, Worker}, #lambda_channel_state{in_flight = InFlight} = State) ->
    ?LOG_DEBUG("worker ~p started", [Worker], #{domain => [lambda]}),
    erlang:monitor(process, Worker),
    {noreply, State#lambda_channel_state{in_flight = InFlight + 1}};

handle_info({'DOWN', _MRef, process, Client, _Reason}, #lambda_channel_state{to = Client} = State) ->
    ?LOG_DEBUG("client ~p disconnected, ~200p", [Client, _Reason], #{domain => [lambda]}),
    {stop, normal, State};

handle_info({'DOWN', _MRef, process, _Worker, _Reason}, #lambda_channel_state{in_flight = InFlight, total = Total} = State) ->
    ?LOG_DEBUG("worker ~p terminated, ~200p", [_Worker, _Reason], #{domain => [lambda]}),
    {noreply, State#lambda_channel_state{in_flight = InFlight - 1, total = Total + 1, tokens = maybe_demand(State)}}.

%%--------------------------------------------------------------------
%% Internal implementation

maybe_demand(#lambda_channel_state{capacity = Capacity, to = Client, low_watermark = LWM, tokens = Tokens}) when Tokens =:= (LWM + 1) ->
    demand(Client, Capacity - LWM),
    Capacity;
maybe_demand(#lambda_channel_state{tokens = Tokens}) ->
    Tokens - 1.

demand(Client, Demand) ->
    ?LOG_DEBUG("demanding: ~b from ~p", [Demand, Client], #{domain => [lambda]}),
    Client ! {demand, Demand, self()}.

%%--------------------------------------------------------------------
%% proxy to start client requests

handle(Sap, M, F, A) ->
    Sap ! {started, self()},
    ?LOG_DEBUG("handling call: ~s:~s(~w)", [M, F, A], #{domain => [lambda]}),
    erlang:apply(M, F, A).

%%--------------------------------------------------------------------
%% proxy to start requests without OTP 23 remote_spawn feature
handle_erpc(ReplyTo, M, F, A) ->
    ?LOG_DEBUG("handling job: ~s:~s(~w)", [M, F, A], #{domain => [lambda]}),
    try
        Res = erlang:apply(M, F, A),
        gen:reply(ReplyTo, {response, Res})
    catch
        Class:Reason:Stack ->
            gen:reply(ReplyTo, {Class, Reason, Stack})
    end.
