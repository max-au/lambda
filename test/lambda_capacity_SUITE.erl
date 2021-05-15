%% @doc
%%     Tests capacity regulation for lambda_plb.
%% @end
-module(lambda_capacity_SUITE).
-author("maximfca@gmail.com").

%% Test server callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

%% Test cases exports
-export([
    capacity_wait/0, capacity_wait/1
]).


-include_lib("stdlib/include/assert.hrl").

all() ->
    [capacity_wait].

init_per_suite(Config) ->
    %% start an authority
    Boot = lambda_test:create_release(proplists:get_value(priv_dir, Config)),
    Auth = lambda_test:start_node(?MODULE, Boot, undefined, [], true),
    unlink(whereis(Auth)), %% otherwise Auth node exits immediately
    [{boot, Boot}, {auth, Auth} | Config].

end_per_suite(Config) ->
    peer:stop(proplists:get_value(auth, Config)),
    Config.

%%--------------------------------------------------------------------
%% Capacity regulator smoke tests

capacity_wait() ->
    [{doc, "Test node-based capacity regulation"}].

capacity_wait(Config) when is_list(Config) ->
    _Concurrency = 10.