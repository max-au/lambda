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
%% Test Cases

basic() ->
    [{doc, "Basic test starting extra node and sending lambda there"}].

basic(Config) when is_list(Config) ->
    %%
    ok.
