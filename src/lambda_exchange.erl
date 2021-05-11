%% @doc
%% Lambda exchange, matching orders from brokers. Started by an authority,
%%  and linked to authority.
%% Separated from authority to enable concurrency - an authority is connected
%%  to thousands of brokers, while exchange will be connected only to a small
%%  numbers of brokers.
%%
%% An exchange serves a single module.
%%  Exchange orders:
%%   - sell (broker, Amount, Meta)
%%   - buy (broker,  Amount, Meta)
%%
%%  Brokers send "sell" orders on server behalf. Broker is monitored by exchange
%%   if it has any outstanding order. Orders from disconnected brokers are
%%   immediately cancelled.
%%
%%
%% Exchange match algorithm considers:
%%  * version
%%  * capacity
%%  * failure domains [passed via meta, NOT IMPLEMENTED YET]
%%  * previous allocations [NOT IMPLEMENTED YET]
%%
%% Exchange is started by an authority, if it does not have enough exchanges
%%  already known. Authority stops locally running exchange if it sorts
%%  first in the list of all known peer authorities, and has enough other
%%  exchanges running for this module.
%%
%% Exchange message protocol:
%% * broker -> exchange: buy
%% * broker -> exchange: sell
%% * broker -> exchange: {'DOWN', ...}
%%
%% @end
-module(lambda_exchange).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/1,
    sell/5,
    buy/4,
    cancel/3
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
-spec start_link(module()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Module) ->
    gen_server:start_link(?MODULE, [Module], []).

%%--------------------------------------------------------------------
%% API

sell(Exchange, SellerContact, Id, Capacity, Meta) ->
    erlang:send(Exchange, {sell, SellerContact, Id, Capacity, self(), Meta}, [noconnect, nosuspend]).

buy(Exchange, Id, Capacity, Meta) ->
    erlang:send(Exchange, {buy, Id, Capacity, self(), Meta}, [noconnect, nosuspend]).

cancel(Exchange, Type, Id) ->
    erlang:send(Exchange, {cancel, Type, Id, self()}, [noconnect, nosuspend]).

%%--------------------------------------------------------------------
%% gen_server implementation

%% Sell order contains seller contact information that is sent to buyer when order matches.
-type sell_order() :: {
    Quantity :: non_neg_integer(),
    MRef :: reference(),
    Id :: non_neg_integer(),
    Contact :: {lambda:location(), lambda_discovery:address()},
    Meta :: lambda_broker:sell_meta()
}.

%% Buy order does not need to store contact (seller never contacts buyer)
-type buy_order() :: {
    Broker :: pid(),
    MRef :: reference(),
    Id :: non_neg_integer(),
    Quantity :: pos_integer(),
    Meta :: lambda_broker:buy_meta()
}.

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

-type state() :: #lambda_exchange_state{}.

-spec init([module()]) -> {ok, state()}.
init([Module]) ->
    {ok, #lambda_exchange_state{module = Module}}.

handle_call(_Req, _From, #lambda_exchange_state{}) ->
    error(notsup).

handle_cast(_Req, _State) ->
    error(notsup).

handle_info({buy, Id, Quantity, Broker, Meta}, #lambda_exchange_state{module = Module, buy = Buy, sell = Sell} = State) ->
    %% match outstanding sales
    %% shortcut if there is anything for sale - to avoid monitoring new buyer
    case match(Broker, Module, Id, Quantity, Sell, Meta) of
        {NewSell, 0} ->
            ?LOG_DEBUG("got buy order from ~p for ~b (id ~b, meta ~200p, satisfied)", [Broker, Quantity, Id, Meta], #{domain => [lambda]}),
            %% completed at full, nothing to monitor or remember
            {noreply, match_buy(State#lambda_exchange_state{sell = NewSell})};
        {NewSell, Remaining} ->
            %% out of capacity, put buy order in the queue
            ?LOG_DEBUG("got buy order from ~p for ~b (id ~b, meta ~200p)", [Broker, Quantity, Id, Meta], #{domain => [lambda]}),
            MRef = erlang:monitor(process, Broker),
            {noreply, match_buy(State#lambda_exchange_state{buy = [{Broker, MRef, Id, Remaining, Meta} | Buy], sell = NewSell})}
    end;

handle_info({sell, SellerContact, _Id, Quantity, Broker, Meta}, #lambda_exchange_state{sell = Sell} = State) ->
    Order =
        case maps:find(Broker, Sell) of
            {ok, {_OldQ, MRef, SellerContact, OldMeta}} ->
                ?LOG_DEBUG("sell capacity update from ~p for ~b (id ~b)", [Broker, Quantity, _Id], #{domain => [lambda]}),
                {Quantity, MRef, SellerContact, OldMeta};
            error ->
                ?LOG_DEBUG("got sell order from ~p for ~b (id ~b, contact ~200p)", [Broker, Quantity, _Id, SellerContact], #{domain => [lambda]}),
                MRef = erlang:monitor(process, Broker),
                {Quantity, MRef, SellerContact, Meta}
        end,
    {noreply, match_buy(State#lambda_exchange_state{sell = Sell#{Broker => Order}})};

handle_info({cancel, buy, Id, _Broker}, #lambda_exchange_state{buy = Buy} = State) ->
    ?LOG_DEBUG("canceling buy order ~b from ~p", [Id, _Broker], #{domain => [lambda]}),
    NewBuy = lists:keydelete(Id, 4, Buy),
    %% TODO: demonitor broker that has no orders left
    {noreply, State#lambda_exchange_state{buy = NewBuy}};

handle_info({cancel, sell, _Id, Broker}, #lambda_exchange_state{sell = Sell} = State) ->
    ?LOG_DEBUG("canceling sell order ~b from ~p", [_Id, Broker], #{domain => [lambda]}),
    %% update remaining sellers map
    NewSell = maps:remove(Broker, Sell),
    %% TODO: demonitor broker that has no orders left
    {noreply, State#lambda_exchange_state{sell = NewSell}};

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #lambda_exchange_state{sell = Sell, buy = Buy} = State) ->
    ?LOG_DEBUG("detected broker down ~p (reason ~200p)", [Pid, _Reason], #{domain => [lambda]}),
    %% filter all buy orders. Can be slow, but this is a resource constrained situation already
    NewBuy = [{Pid1, MRef, Id, Quantity, Meta} || {Pid1, MRef, Id, Quantity, Meta} <- Buy, Pid =/= Pid1],
    ?LOG_DEBUG("removed: ~200p ~200p", [Buy -- NewBuy, maps:get(Pid, Sell, [])], #{domain => [lambda]}),
    {noreply, State#lambda_exchange_state{buy = NewBuy, sell = maps:remove(Pid, Sell)}}.

%%--------------------------------------------------------------------
%% Internal implementation

match_buy(#lambda_exchange_state{buy = []} = State) ->
    %% no buy orders
    State;
match_buy(#lambda_exchange_state{sell = Sell} = State) when Sell =:= #{} ->
    %% no sell orders
    State;
match_buy(#lambda_exchange_state{module = Module, buy = [{Broker, MRef, Id, Quantity, BuyMeta} | More], sell = Sell} = State) ->
    %% should be a match somewhere
    case match(Broker, Module, Id, Quantity, Sell, BuyMeta) of
        {NewSell, 0} ->
            %% stop monitoring this broker reference
            %% NB: there are more monitors, separately, for other orders, could be improved!
            erlang:demonitor(MRef, [flush]),
            %% completed at full, continue
            match_buy(State#lambda_exchange_state{buy = More, sell = NewSell});
        {NewSell, Remaining} ->
            %% out of capacity, keep the order in the queue
            State#lambda_exchange_state{buy = [{Broker, MRef, Id, Remaining, BuyMeta} | More], sell = NewSell}
    end.

%% Finds full or partial match and replies to buyer's Broker if any match is found
match(Broker, Module, Id, Quantity, Sell, BuyMeta) ->
    %% iterate over a map of non-zero-capacity sellers
    case select(maps:next(maps:iterator(Sell)), Quantity, BuyMeta, []) of
        {_, Quantity} ->
            {Sell, Quantity};
        {Selected, Remain} ->
            %% send to buyer's broker
            complete_order(Broker, Module, Id, Selected),
            %% update remaining sellers map
            NewSell = lists:foldl(
                fun ({S, _, Q, _M}, Sellers) ->
                    maps:update_with(S, fun ({OQ, OM, Contact, Meta}) -> {OQ - Q, OM, Contact, Meta} end, Sellers)
                end, Sell, Selected),
            {NewSell, Remain}
    end.

%% Selects first N sellers with non-zero orders outstanding
select(none, Remain, _BuyMeta, Selected) ->
    {Selected, Remain};
select({_Broker, {0, _MRef, _Contact, _Meta}, Next}, Remain, BuyMeta, Selected) ->
    %% skip zero
    select(maps:next(Next), Remain, BuyMeta, Selected);
select({_Broker, {_Q, _MRef, _Contact, SellMeta}, _Next} = Iter, Remain, BuyMeta, Selected) ->
    select_pick(match_meta(SellMeta, BuyMeta), Iter, Remain, BuyMeta, Selected).

select_pick(false, {_Broker, _Sell, Next}, Remain, BuyMeta, Selected) ->
    %% meta did not match
    select(maps:next(Next), Remain, BuyMeta, Selected);
select_pick(true, {Broker, {Q, _MRef, Contact, Meta}, _Next}, Remain, _BuyMeta, Selected) when Remain =< Q ->
    %% fully satisfied
    {[{Broker, Contact, Remain, Meta} | Selected], 0};
select_pick(true, {Broker, {Q, _MRef, Contact, Meta}, Next}, Remain, BuyMeta, Selected) ->
    %% continue picking non-zero
    select(maps:next(Next), Remain - Q, BuyMeta, [{Broker, Contact, Q, Meta} | Selected]).

%% @private
%%  Returns 'true' only when seller/buyer meta match
match_meta(#{module := SellMod} = _SellMeta, #{module := #{vsn := BuyVsn}} = _BuyMeta) when is_integer(BuyVsn) ->
    %% buyer requests a specific version, so at least seller meta must have it
    ?LOG_DEBUG("Metas: sell ~p, buy ~p, check: ~p", [_SellMeta, _BuyMeta, {ok, BuyVsn} =:= maps:find(vsn, SellMod)], #{domain => [lambda]}),
    {ok, BuyVsn} =:= maps:find(vsn, SellMod);
match_meta(_SellMeta, _BuyMeta) ->
    ?LOG_DEBUG("Metas: sell ~p, buy ~p", [_SellMeta, _BuyMeta], #{domain => [lambda]}),
    true.

complete_order(To, Module, Id, Sellers) ->
    NoBroker = [{Contact, Quantity, Meta} || {_, Contact, Quantity, Meta} <- Sellers],
    ?LOG_DEBUG("responding to buyer's broker ~p for ~s (id ~b, sellers ~200P)", [To, Module, Id, NoBroker, 10], #{domain => [lambda]}),
    To ! {order, Id, Module, NoBroker}.
