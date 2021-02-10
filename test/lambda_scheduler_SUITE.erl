%% @doc
%%  Tests for 'lambda_scheduler' module.
%% @end
-module(lambda_scheduler_SUITE).
-author("maximfca@gmail.com").

%% Common Test headers
-include_lib("stdlib/include/assert.hrl").

%% Test server callbacks
-export([
    suite/0,
    all/0,
    end_per_testcase/2
]).

%% Test cases
-export([
    node/0, node/1,
    single_dep/0, single_dep/1,
    tree/0, tree/1,
    dag/0, dag/1,
    cycle/0, cycle/1,
    unknown_dependency/0, unknown_dependency/1,
    double_two/1,
    digraph_prop/0, digraph_prop/1
]).

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [node, single_dep, tree, dag, cycle, unknown_dependency, double_two, digraph_prop].

end_per_testcase(TestCase, Config) ->
    is_pid(whereis(TestCase)) andalso gen_server:stop(TestCase),
    Config.

%% build callback
build(Sched, Name, Jobs) ->
    {Value, Deps} = maps:get(Name, Jobs),
    case lambda_scheduler:require(Sched, Name, Deps) of
        {error, {cycle, Loop}} ->
            %% throw on cycles
            error({cycle, Loop});
        Done ->
            Vals = maps:values(Done),
            [error({cycle, [Name | C]}) || {error, {cycle, C}} <- Vals], %% recursively throw on loops
            [error(R) || {error, R} <- Vals], %% throw on deps errors
            lists:sum([V || {ok, V} <- Vals]) + Value %% sum of the path
    end.

%% -------------------------------------------------------------------
%% Test Cases

node() ->
    [{doc, "Tests one single node (no dependencies)"}].

node(Config) when is_list(Config) ->
    Resolver = fun (Sched, Name) -> build(Sched, Name, #{node => {1, []}}) end,
    ?assertEqual({ok, 1}, lambda_scheduler:build(Resolver, node)).

single_dep() ->
    [{doc, "Tests simple case with one dependency"}].

single_dep(Config) when is_list(Config) ->
    Resolver =
        fun (Sched, root) ->
                #{dep := {ok, DepVal}} = lambda_scheduler:require(Sched, root, [dep]),
                DepVal + 10;
            (_Sched, dep) ->
                42
        end,
    ?assertEqual({ok, 52}, lambda_scheduler:build(Resolver, root)).

tree() ->
    [{doc, "Tests tree (no joining paths)"}].

tree(Config) when is_list(Config) ->
    Tree = #{
        root => {1, [left, right]},
        left => {10, []},
        right => {20, [right_right]},
        right_right => {30, []}
    },
    DepTree = #{
        root => {{ok, 61}, #{left => {ok, 10}, right => {ok, 50}}},
        left => {{ok, 10}, #{}},
        right => {{ok, 50}, #{right_right => {ok, 30}}},
        right_right => {{ok, 30}, #{}}
    },
    TreeSum = lists:sum([Val || {Val, _Deps} <- maps:values(Tree)]),
    {ok, Pid} = lambda_scheduler:start_link({local, ?FUNCTION_NAME},
        fun (Sched, Name) -> build(Sched, Name, Tree) end),
    {started, _} = lambda_scheduler:request(Pid, root),
    ?assertEqual({ok, TreeSum}, lambda_scheduler:await(Pid, root)),
    %% verify provides/depends
    ?assertEqual([left, right], lambda_scheduler:depends(Pid, root)),
    ?assertEqual([root], lambda_scheduler:provides(Pid, right)),
    %% fetch the graph
    Targets = lambda_scheduler:targets(Pid),
    Graph = lists:foldl(
        fun (T, G) ->
            Val = lambda_scheduler:value(Pid, T),
            Deps = lambda_scheduler:depends(Pid, T),
            DepVals = maps:from_list([{D, lambda_scheduler:value(Pid, D)} || D <- Deps]),
            G#{T => {Val, DepVals}}
        end, #{}, Targets),
    ?assertEqual(DepTree, Graph).

dag() ->
    [{doc, "Tests simple directed acyclic graph"}].

dag(Config) when is_list(Config) ->
    Graph = #{
        root => {1, [left, right]},
        left => {10, [center]}, right => {20, [center]},
        center => {100, []}},
    ?assertEqual({ok, 231}, lambda_scheduler:build(
        fun (Sched, Name) -> build(Sched, Name, Graph) end, root)).

cycle() ->
    [{doc, "Tests that cycle does not cause issues"}].

cycle(Config) when is_list(Config) ->
    Cycle = #{root => {1, [root]}},
    {ok, Pid} = lambda_scheduler:start_link({local, ?FUNCTION_NAME},
        fun (Sched, Name) -> build(Sched, Name, Cycle) end),
    %% expected that build starts
    ?assertMatch({started, _}, lambda_scheduler:request(Pid, root)),
    %% but, when waiting for result, an error is reported
    ?assertEqual({error, {cycle, [root, root]}}, lambda_scheduler:await(Pid, root)),
    gen_server:stop(Pid),
    Cycle2 = #{root => {1, [second]}, second => {1, [third]}, third => {1, [second]}},
    {ok, Pid1} = lambda_scheduler:start_link({local, ?FUNCTION_NAME},
        fun (Sched, Name) -> build(Sched, Name, Cycle2) end),
    ?assertMatch({started, _}, lambda_scheduler:request(Pid1, root)),
    %% here: expect that root & second were built, then cycle was detected
    ?assertEqual({error, {cycle, [root, second, third, second, third]}},
        lambda_scheduler:await(Pid1, root)),
    gen_server:stop(Pid1),
    %% check that cycle it correctly returned
    Cycle3 = #{eight=>{1, []}, five=>{1, [eight,twelve]},twelve=>{1, [eight,five]}},
    {ok, Pid2} = lambda_scheduler:start_link({local, ?FUNCTION_NAME},
        fun (Sched, Name) -> build(Sched, Name, Cycle3) end),
    ?assertMatch({started, _}, lambda_scheduler:request(Pid2, five)),
    ?assertEqual({error, {cycle, [five, twelve, five, twelve]}},
        lambda_scheduler:await(Pid2, five)).

unknown_dependency() ->
    [{doc, "Tests that (yet) unknown dependency does not break circular dependnecies check"}].

unknown_dependency(Config) when is_list(Config) ->
    Cycle = #{
        three => {1, [nine, three]}, %% loops
        nine => {1, []}
    },
    {ok, Pid} = lambda_scheduler:start_link({local, ?FUNCTION_NAME},
        fun (Sched, Name) -> build(Sched, Name, Cycle) end),
    ?assertMatch({started, _}, lambda_scheduler:request(Pid, three)),
    %% but, when waiting for result, an error is reported
    ?assertEqual({error, {cycle, [three, three]}}, lambda_scheduler:await(Pid, three)),
    gen_server:stop(Pid).

double_two(Config) when is_list(Config) ->
    Graph = #{one=>[one],six=>[two,one],two=>[]},
    Collector = spawn_link(fun () -> collect_loop([]) end),
    Builder = fun (Sched, Name) -> topsort(Sched, Collector, Name, Graph) end,
    {ok, two} = lambda_scheduler:build(Builder, two),
    Sorted = gen_server:call(Collector, get),
    ?assertEqual([two], Sorted).

digraph_prop() ->
    [{doc, "Property-based test ensuring dynamic dependencies behavior is equal to static DAG"}].

digraph_prop(Config) when is_list(Config) ->
    case proper:quickcheck(prop_digraph_equality(),
        [long_result, {numtests, 50}, {max_size, 500}, {start_size, 10}]) of
        true ->
            ok;
        {error, Reason} ->
            {fail, {error, Reason}};
        CounterExample ->
            {fail, {example, CounterExample}}
    end.

%% -------------------------------------------------------------------
%% Properties

%% "Build" callback that simply does topological sorting
%%  via sending a message to serialise the order.
topsort(Sched, Collector, Name, Jobs) ->
    Deps = maps:get(Name, Jobs),
    case lambda_scheduler:require(Sched, Name, Deps) of
        {error, Err} ->
            error(Err);
        Done ->
            [error(R) || {error, R} <- maps:values(Done)], %% error on deps errors
            Collector ! {push, Name},
            Name
    end.

collect_loop(Collected) ->
    receive
        {push, Data} ->
            collect_loop([Data | Collected]);
        {'$gen_call', From, get} ->
            gen:reply(From, lists:reverse(Collected)),
            collect_loop([]);
        stop ->
            ok
    end.

%% form a dep-map: [{From, To}] => #{From => [To], To => []}
edges_to_dep_map(Edges) ->
    lists:foldl(
        fun ({From, To}, Acc) ->
            Acc1 = if is_map_key(To, Acc) -> Acc; true -> Acc#{To => []} end,
            maps:update_with(From,
                fun (Deps) ->
                    case lists:member(To, Deps) of
                        true -> Deps;
                        false -> [To | Deps]
                    end
                end, [To], Acc1)
        end, #{}, Edges).

%% creates a digraph from dep-map
digraph(Graph) ->
    G = digraph:new([private]),
    [digraph:add_vertex(G, V) || V <- maps:keys(Graph)],
    [[[_|_] = digraph:add_edge(G, Dep, V) || Dep <- Deps] || {V, Deps} <- maps:to_list(Graph)],
    G.

is_topo([], _Visited) ->
    true;
is_topo([Vert | _Tail], Visited) when is_map_key(Vert, Visited) ->
    false;
is_topo([Vert | Tail], Visited) ->
    is_topo(Tail, Visited#{Vert => true}).

prop_digraph_equality() ->
    proper:forall(graph(),
        fun (Edges) ->
            Graph = edges_to_dep_map(Edges),
            Digraph = digraph(Graph),
            Collector = spawn_link(fun () -> collect_loop([]) end),
            Builder = fun (Sched, Name) -> topsort(Sched, Collector, Name, Graph) end,
            %% test every single vertex of a graph
            Failed = lists:any(
                fun (Vertex) ->
                    Dyn = lambda_scheduler:build(Builder, Vertex),
                    Sorted = gen_server:call(Collector, get),
                    case Dyn of
                        {ok, Vertex} ->
                            Topo = digraph_utils:reaching([Vertex], Digraph),
                            case lists:sort(Topo) =/= lists:sort(Sorted) of
                                true ->
                                    true;
                                false ->
                                    %% check topo-sort: no vertex should depend on previous one
                                    not is_topo(Sorted, #{})
                            end;
                        {error, {cycle, Cycle}} ->
                            %% check that subgraph formed for a cycle is a strong component
                            Dup = lists:last(Cycle),
                            TopoCycle = digraph:get_short_cycle(Digraph, Dup),
                            TopoCycle =:= false
                    end
                end, maps:keys(Graph)
            ),
            Collector ! stop,
            digraph:delete(Digraph),
            Failed =:= false
        end
    ).


%% -------------------------------------------------------------------
%% Generators

%% Use limited number of vertices to facilitate connected graphs
vertex() ->
    proper_types:oneof([one, two, three, four, five, six, seven, eight,
        nine, ten, eleven, twelve, thirteen, fourteen, fifteen, sixteen, nineteen,
        twenty]).

graph() ->
    proper_types:list({vertex(), vertex()}).
