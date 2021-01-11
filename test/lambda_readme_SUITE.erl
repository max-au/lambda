%% @doc
%%     Tests for examples provided in README.md and documentation
%% @end
-module(lambda_readme_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    basic/0, basic/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 10}}].

all() ->
    [basic].

%%--------------------------------------------------------------------
%% Convenience & data
compile_code(Lines) ->
    Tokens = [begin {ok, T, _} = erl_scan:string(L), T end || L <- Lines],
    Forms = [begin {ok, F} = erl_parse:parse_form(T), F end || T <- Tokens],
    {ok, _Module, Binary} = compile:forms(Forms),
    Binary.

calc() ->
    File = ["-module(calc).",
        "-export([pi/1]).",
        "pi(Precision) when Precision >= 1, Precision =< 10 -> pi(4, -4, 3, Precision).",
        "pi(LastResult, Numerator, Denominator, Precision) ->  NextResult = LastResult + Numerator / Denominator,"
        "Pow = math:pow(10, Precision), case trunc(LastResult * Pow) =:= trunc(NextResult * Pow) of true ->"
        "trunc(NextResult * Pow) / Pow; false -> pi(NextResult, -1 * Numerator, Denominator + 2, Precision) end."],
    compile_code(File).

%%--------------------------------------------------------------------
%% Test Cases

basic() ->
    [{doc, "Basic test starting extra node, publishing/discovering lambda and running it remotely"}].

basic(Config) when is_list(Config) ->
    %% Server: publish calc
    {ok, Server} = peer:start_link(#{connection => standard_io}),
    %% need to add path to calc module
    {module,calc} = peer:apply(Server, code, load_binary, [calc, nofile, calc()]),
    ok = peer:apply(Server, application, start, [lambda]),
    {ok, _Srv} = peer:apply(Server, lambda, publish, [calc]),
    %% Client: discover calc
    {ok, Client} = peer:start_link(#{connection => standard_io}),
    ok = peer:apply(Client, application, start, [lambda]),
    {ok, _Plb} = peer:apply(Client, lambda, discover, [calc]),
    %% Execute calc remotely (on the client)
    ?assertEqual(3.14, peer:apply(Client, calc, pi, [2])),
    %% Shutdown
    peer:stop(Server).
