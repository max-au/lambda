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

%% Introspection API
-export([
    orders/2
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

-type seller_contact() :: {lambda:location(), lambda_discovery:address()}.

-spec sell(lambda:dst(), Id :: non_neg_integer(), Capacity :: non_neg_integer(),
    Meta :: lambda_broker:sell_meta(), seller_contact()) -> ok | nosuspend | noconnect.
sell(Exchange, Id, Capacity, Meta, SellerContact) ->
    erlang:send(Exchange, {sell, Id, Capacity, self(), Meta, SellerContact}, [noconnect, nosuspend]).

-spec buy(lambda:dst(), Id :: non_neg_integer(), Capacity :: non_neg_integer(),
    Meta :: lambda_broker:buy_meta()) -> ok | nosuspend | noconnect.
buy(Exchange, Id, Capacity, Meta) ->
    erlang:send(Exchange, {buy, Id, Capacity, self(), Meta}, [noconnect, nosuspend]).

-spec cancel(lambda:dst(), Id :: non_neg_integer(), buy | sell) -> ok | nosuspend | noconnect.
cancel(Exchange, Id, Type) ->
    erlang:send(Exchange, {cancel, Id, Type, self()}, [noconnect, nosuspend]).

%%--------------------------------------------------------------------
%% Introspection API: designed for debugging use only. Significantly affects
%%  exchange performance when done on a regular basis.

%% Sell order contains seller contact information that is sent to buyer when order matches.
%% Buy order does not need to store contact (seller never contacts buyer)
-record(order, {
    id :: non_neg_integer(),
    quantity :: non_neg_integer(),
    broker :: pid(),
    meta :: lambda_broker:sell_meta() | lambda_broker:buy_meta(),
    contact :: undefined | seller_contact(),
    mref :: reference()
}).

-type order() :: #order{}.

-type filter() :: #{
    type => buy | sell,
    broker => pid(),
    id => non_neg_integer()
}.

-spec orders(lambda:dst(), filter()) -> [order()].
orders(Exchange, Filter) ->
    gen_server:call(Exchange, {orders, Filter}).

%%--------------------------------------------------------------------
%% gen_server implementation

-record(lambda_exchange_state, {
    %% module traded here
    module :: module(),
    %% outstanding sell orders. A broker can have only 1 sell order for this module.
    sell = #{} :: #{pid() => order()},
    %% outstanding buy orders, should be normally empty - if not, then
    %%  system is under pressure, lacking resources. Completed buy orders are removed
    %%  from the list
    buy = [] :: [order()]
}).

-type state() :: #lambda_exchange_state{}.

-spec init([module()]) -> {ok, state()}.
init([Module]) ->
    {ok, #lambda_exchange_state{module = Module}}.

handle_call({orders, Filters}, _From, #lambda_exchange_state{buy = Buy, sell = Sell} = State) ->
    {reply, filter(Buy, Sell, Filters), State}.

handle_cast(_Req, _State) ->
    error(notsup).

handle_info({buy, Id, Quantity, Broker, Meta}, #lambda_exchange_state{module = Module, buy = Buy, sell = Sell} = State) ->
    %% match outstanding sales
    %% shortcut if there is anything for sale - to avoid monitoring new buyer
    case match(Module, Sell, Quantity, Id, Broker, Meta) of
        {NewSell, 0} ->
            ?LOG_DEBUG("got buy order from ~p for ~b (id ~b, meta ~200p, satisfied)", [Broker, Quantity, Id, Meta], #{domain => [lambda]}),
            %% completed at full, nothing to monitor or remember
            {noreply, match_buy(State#lambda_exchange_state{sell = NewSell})};
        {NewSell, Remaining} ->
            %% out of capacity, put buy order in the queue
            ?LOG_DEBUG("got buy order from ~p for ~b (id ~b, meta ~200p)", [Broker, Quantity, Id, Meta], #{domain => [lambda]}),
            MRef = erlang:monitor(process, Broker),
            Order = #order{id = Id, broker = Broker, meta = Meta, quantity = Remaining, mref = MRef},
            {noreply, match_buy(State#lambda_exchange_state{buy = [Order | Buy], sell = NewSell})}
    end;

handle_info({sell, Id, Quantity, Broker, Meta, SellerContact}, #lambda_exchange_state{sell = Sell} = State) ->
    Order1 =
        case maps:find(Broker, Sell) of
            {ok, Order} ->
                ?LOG_DEBUG("sell capacity update from ~p for ~b (id ~b)", [Broker, Quantity, Id], #{domain => [lambda]}),
                Order#order{quantity = Quantity};
            error ->
                ?LOG_DEBUG("got sell order from ~p for ~b (id ~b, contact ~200p)", [Broker, Quantity, Id, SellerContact], #{domain => [lambda]}),
                MRef = erlang:monitor(process, Broker),
                #order{quantity = Quantity, id = Id, mref = MRef, broker = Broker, contact = SellerContact, meta = Meta}
        end,
    {noreply, match_buy(State#lambda_exchange_state{sell = Sell#{Broker => Order1}})};

handle_info({cancel, buy, Id, Broker}, #lambda_exchange_state{buy = Buy} = State) ->
    ?LOG_DEBUG("canceling buy order ~b from ~p", [Id, Broker], #{domain => [lambda]}),
    NewBuy = [Order || #order{broker = Br, id = Id1} = Order <- Buy, Br =/= Broker orelse Id1 =/= Id],
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
    NewBuy = [Order || #order{broker = Pid1} = Order <- Buy, Pid =/= Pid1],
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
match_buy(#lambda_exchange_state{module = Module, buy = [Order | More], sell = Sell} = State) ->
    %% should be a match somewhere
    case match(Module, Sell, Order#order.quantity, Order#order.id, Order#order.broker, Order#order.meta) of
        {NewSell, 0} ->
            %% stop monitoring this buyer's broker reference
            %% TODO: there are more monitors, separately, for other orders, could be improved!
            erlang:demonitor(Order#order.mref, [flush]),
            %% completed at full, continue
            match_buy(State#lambda_exchange_state{buy = More, sell = NewSell});
        {NewSell, Remaining} ->
            %% out of sell capacity, keep the buy order in the queue
            State#lambda_exchange_state{buy = [Order#order{quantity = Remaining} | More], sell = NewSell}
    end.

%% Finds full or partial match and replies to buyer's Broker if any match is found
match(Module, Sell, Quantity, Id, Broker, BuyMeta) ->
    %% iterate over a map of non-zero-capacity sellers
    case select(maps:next(maps:iterator(Sell)), Quantity, BuyMeta, []) of
        {_, Quantity} ->
            {Sell, Quantity};
        {Selected, Remain} ->
            %% send to buyer's broker
            complete_order(Broker, Module, Id, Selected),
            %% update remaining sellers map
            NewSell = lists:foldl(
                fun ({Br, _Contact, QM, _Meta}, Sellers) ->
                    maps:update_with(Br,
                        fun (#order{quantity = Q} = Order) ->
                            Order#order{quantity = Q - QM}
                        end, Sellers)
                end, Sell, Selected),
            {NewSell, Remain}
    end.

%% Selects first N sellers with non-zero orders outstanding
%% Returns list of "Selected": {Broker, Contact, Quantity, Meta}
select(none, Remain, _BuyMeta, Selected) ->
    {Selected, Remain};
select({_Broker, #order{quantity = 0}, Next}, Remain, BuyMeta, Selected) ->
    %% skip zero
    select(maps:next(Next), Remain, BuyMeta, Selected);
select({_Broker, #order{meta = SellMeta}, _Next} = Iter, Remain, BuyMeta, Selected) ->
    select_pick(match_meta(SellMeta, BuyMeta), Iter, Remain, BuyMeta, Selected).

select_pick(false, {_Broker, _Sell, Next}, Remain, BuyMeta, Selected) ->
    %% meta did not match
    select(maps:next(Next), Remain, BuyMeta, Selected);
select_pick(true, {Broker, #order{quantity = Q, contact = Contact, meta = Meta}, _Next},
    Remain, _BuyMeta, Selected) when Remain =< Q ->
    %% fully satisfied
    {[{Broker, Contact, Remain, Meta} | Selected], 0};
select_pick(true, {Broker, #order{quantity = Q, contact = Contact, meta = Meta}, Next},
    Remain, BuyMeta, Selected) ->
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
    lambda_broker:complete_order(To, Module, Id, NoBroker).

%%--------------------------------------------------------------------
%% Introspection internals
filter(Buy, _Sell, #{type := buy} = Filters) ->
    filter_buy(Buy, Filters);
filter(_Buy, Sell, #{type := sell} = Filters) ->
    filter_sell(Sell, Filters);
filter(Buy, Sell, Filters) ->
    filter_buy(Buy, Filters) ++ filter_sell(Sell, Filters).

filter_sell(Sell, #{broker := Pid} = Filters) ->
    case maps:find(Pid, Sell) of
        {ok, #order{id = Id} = Order} when map_get(id, Filters) =:= Id ->
            [Order];
        {ok, Order} when not is_map_key(id, Filters) ->
            [Order];
        error ->
            []
    end;
filter_sell(Sell, #{id := Id}) ->
    [Order || Order <- maps:values(Sell), element(3, Order) =:= Id];
filter_sell(Sell, #{}) ->
    maps:values(Sell).

filter_buy(Buy, #{broker := Pid, id := Id}) ->
    [Order || Order <- Buy, element(4, Order) == Pid, element(3, Order) =:= Id];
filter_buy(Buy, #{broker := Pid}) ->
    [Order || Order <- Buy, element(4, Order) == Pid];
filter_buy(Buy, #{id := Id}) ->
    [Order || Order <- Buy, element(3, Order) =:= Id];
filter_buy(Buy, #{}) ->
    Buy.
