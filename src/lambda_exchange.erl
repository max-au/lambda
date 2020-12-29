%% @doc
%% Lambda exchange, matching orders from brokers. Started by an authority,
%%  and linked to authority.
%%
%% An exchange serves a single module.
%%  Exchange orders:
%%   - sell (broker, amount)
%%   - buy (broker,  amount)
%%
%%  Brokers send "sell" orders on server behalf. Broker is monitored by exchange
%%   if it has any outstanding order. Orders from disconnected brokers are
%%   immediately cancelled.
%%
%%
%% Exchange match algorithm considers:
%%  * capacity
%%  * failure domains
%%  * previous allocations
%%
%% Exchange is started by an authority, if it does not have enough exchanges
%%  already known. Authority stops locally running exchange if it sorts
%%  first in the list of all known peer authorities, and has enough other
%%  exchanges running for this module.
%%
%% @end
-module(lambda_exchange).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/1,
    sell/4,
    buy/3
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
%% Starts the exchange and links it to calling process.
-spec start_link(module()) -> gen:start_ret().
start_link(Module) ->
    gen_server:start_link(?MODULE, [Module], []).

%%--------------------------------------------------------------------
%% API

sell(Exchange, SellerContact, Id, Capacity) ->
    erlang:send(Exchange, {sell, SellerContact, Id, Capacity, self()}, [noconnect, nosuspend]).

buy(Exchange, Id, Capacity) ->
    erlang:send(Exchange, {buy, Id, Capacity, self()}, [noconnect, nosuspend]).

%%--------------------------------------------------------------------
%% gen_server implementation

%% Sell order contains seller contact information that is sent to buyer when order matches.
-type sell_order() :: {
    Quantity :: non_neg_integer(),
    MRef :: reference(),
    Contact :: {lambda_broker:location(), lambda_epmd:address()}
}.

%% Buy order
-type buy_order() :: {Broker :: pid(), MRef :: reference(), Id :: non_neg_integer(), Quantity :: pos_integer()}.

-record(lambda_exchange_state, {
    %% module traded here
    module :: module(),
    %% outstanding sell orders. A broker can have only 1 sell order for this module.
    sell = #{} :: #{pid() => sell_order()},
    %% outstanding buy orders, should be normally empty - if not, then
    %%  system is under pressure, lacking resources. Completed buy orders are removed
    %%  from the list
    buy = [] :: [buy_order()]
}).

%% -define(DEBUG, true).
-ifdef (DEBUG).
-define (dbg(Fmt, Arg), io:format(standard_error, "~s ~p: exchange " ++ Fmt ++ "~n", [node(), self() | Arg])).
-else.
-define (dbg(Fmt, Arg), ok).
-endif.

-type state() :: #lambda_exchange_state{}.

-spec init([module()]) -> {ok, state()}.
init([Module]) ->
    {ok, #lambda_exchange_state{module = Module}}.

handle_call(_Req, _From, #lambda_exchange_state{}) ->
    error(notsup).

handle_cast(_Req, _State) ->
    error(notsup).

handle_info({buy, Id, Quantity, Broker}, #lambda_exchange_state{module = Module, buy = Buy, sell = Sell} = State) ->
    %% match outstanding sales
    %% shortcut if there is anything for sale - to avoid monitoring new buyer
    case match(Broker, Module, Id, Quantity, Sell) of
        {NewSell, 0} ->
            ?dbg("got buy order from ~p for ~b (id ~b, immediately completed)", [Broker, Quantity, Id]),
            %% completed at full, nothing to monitor or remember
            {noreply, match_buy(State#lambda_exchange_state{sell = NewSell})};
        {NewSell, Remaining} ->
            %% out of capacity, put buy order in the queue
            ?dbg("got buy order from ~p for ~b (id ~b)", [Broker, Quantity, Id]),
            MRef = erlang:monitor(process, Broker),
            {noreply, match_buy(State#lambda_exchange_state{buy = [{Broker, MRef, Id, Remaining} | Buy], sell = NewSell})}
    end;

handle_info({sell, SellerContact, Id, Quantity, Broker}, #lambda_exchange_state{sell = Sell} = State) ->
    Order =
        case maps:find(Broker, Sell) of
            {ok, {_OldQ, MRef, Seller, Contact}} ->
                ?dbg("sell capacity update from ~p for ~b (id ~b)", [Broker, Quantity, Id]),
                {Quantity, MRef, Seller, Contact};
            error ->
                ?dbg("got sell order from ~p for ~b (id ~b, contact ~200p)", [Broker, Quantity, Id, SellerContact]),
                MRef = erlang:monitor(process, Broker),
                {Quantity, MRef, SellerContact}
        end,
    {noreply, match_buy(State#lambda_exchange_state{sell = Sell#{Broker => Order}})};

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_exchange_state{sell = Sell, buy = Buy} = State) ->
    ?dbg("detected down ~p, filtering orders", [Pid]),
    %% filter all buy orders. Can be slow, but this is a resource constrained situation already
    NewBuy = [{Pid, MRef, Id, Quantity} || {Pid1, MRef, Id, Quantity} <- Buy, Pid =/= Pid1],
    {noreply, State#lambda_exchange_state{buy = NewBuy, sell = maps:remove(Pid, Sell)}}.

%%--------------------------------------------------------------------
%% Internal implementation

match_buy(#lambda_exchange_state{buy = []} = State) ->
    %% no buy orders
    State;
match_buy(#lambda_exchange_state{sell = Sell} = State) when Sell =:= #{} ->
    %% no sell orders
    State;
match_buy(#lambda_exchange_state{module = Module, buy = [{Broker, MRef, Id, Quantity} | More], sell = Sell} = State) ->
    %% should be a match somewhere
    case match(Broker, Module, Id, Quantity, Sell) of
        {NewSell, 0} ->
            %% stop monitoring this broker reference
            erlang:demonitor(MRef, [flush]),
            %% completed at full, continue
            match_buy(State#lambda_exchange_state{buy = More, sell = NewSell});
        {NewSell, Remaining} ->
            %% out of capacity, keep the order in the queue
            State#lambda_exchange_state{buy = [{Broker, MRef, Id, Remaining} | More], sell = NewSell}
    end.

%% Finds full or partial match and replies to buyer's Broker if any match is found
match(Broker, Module, Id, Quantity, Sell) ->
    %% iterate over a map of non-zero-capacity sellers
    case select(maps:next(maps:iterator(Sell)), Quantity, []) of
        {_, Quantity} ->
            {Sell, Quantity};
        {Selected, Remain} ->
            %% send to buyer's broker
            complete_order(Broker, Module, Id, Selected),
            %% update remaining sellers map
            NewSell = lists:foldl(
                fun ({S, _, Q}, Sellers) ->
                    maps:update_with(S, fun ({OQ, OM, Contact}) -> {OQ - Q, OM, Contact} end, Sellers)
                end, Sell, Selected),
            {NewSell, Remain}
    end.

%% Selects first N sellers with non-zero orders outstanding
select(none, Remain, Selected) ->
    {Selected, Remain};
select({Broker, {Q, _MRef, Contact}, _Next}, Remain, Selected) when Remain =< Q ->
    {[{Broker, Contact, Remain} | Selected], 0};
select({_Broker, {0, _MRef, _Contact}, Next}, Remain, Selected) ->
    %% skip zero
    select(maps:next(Next), Remain, Selected);
select({Broker, {Q, _MRef, Contact}, Next}, Remain, Selected) ->
    %% continue picking non-zero
    select(maps:next(Next), Remain - Q, [{Broker, Contact, Q} | Selected]).

complete_order(To, Module, Id, Sellers) ->
    NoBroker = [{Contact, Quantity} || {_, Contact, Quantity} <- Sellers],
    ?dbg("responding to buyer's broker ~p for ~s (id ~b, sellers ~200p)", [To, Module, Id, NoBroker]),
    To ! {order, Id, Module, NoBroker}.
