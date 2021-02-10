%% @doc
%%     Tests Lambda authority
%% @end
-module(lambda_authority_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    suite/0,
    all/0
]).

%% Test cases exports
-export([
    mesh/0, mesh/1
]).

-include_lib("stdlib/include/assert.hrl").

suite() ->
    [{timetrap, {seconds, 5}}].

all() ->
    [mesh].

%%--------------------------------------------------------------------
%% Convenience

%%--------------------------------------------------------------------
%% Test Cases

mesh() ->
    [{doc, "Tests that authorities form a full mesh"}].

mesh(Config) when is_list(Config) ->
    ok.