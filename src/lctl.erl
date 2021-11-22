%% @doc
%% Lambda control module, designed as escript providing convenience access to
%%  lambda authority facilities.
%% @end
-module(lctl).
-author("maximfca@gmail.com").

-behaviour(cli).
-mode(compile).
-export([main/1, cli/0]).

main(Args) ->
    case cli:run(Args, #{progname => "lctl"}) of
        {ok, Text} when is_list(Text); is_binary(Text) ->
            io:format("~s", [Text]);
        ok ->
            ok;
        Error ->
            io:format("~tp~n", [Error])
    end.

cli() ->
    #{
        commands => #{
            "authority" => #{
                handler => fun authority_list/1,
                commands => #{
                    "list" => #{handler => fun authority_list/1}
                }
            },
            "module" => #{
                handler => fun module_list/1,
                commands => #{
                    "list" => #{handler => fun module_list/1},
                    "deploy" => #{
                        handler => fun module_deploy/1
                    }
                }
            }
        }
    }.

%% Common functions
join() ->
    net_kernel:start([lctl, shortnames]),
    {ok, _Apps} = application:ensure_all_started(lambda),
    timer:sleep(500), %% implement broker subscription instead
    ok.

leave() ->
    net_kernel:stop().

%% Handlers

authority_list(#{}) ->
    join(),
    KnownAuth = lambda_broker:authorities(lambda_broker),
    AuthInfo = [
        begin
            Node = node(Auth),
            #{addr := Ip, port := Port} = lambda_discovery:get_node(Node),
            io_lib:format("~s ~s:~b (~p)~n", [Node, inet:ntoa(Ip), Port, Auth])
        end || Auth <- KnownAuth],
    leave(),
    {ok, io_lib:format("Total: ~b~n", [length(KnownAuth)]) ++ lists:concat(AuthInfo)}.

module_list(#{}) ->
    join(),
    %% find authorities
    KnownAuth = lambda_broker:authorities(lambda_broker),
    AllModules = [lambda_authority:modules(Auth) || Auth <- KnownAuth],
    Modules = lists:usort(lists:concat(AllModules)),
    ModuleInfo = [
        begin
            io_lib:format("~s~n", [Mod])
        end || Mod <- Modules],
    leave(),
    {ok, io_lib:format("Total: ~b~n", [length(Modules)]) ++ lists:concat(ModuleInfo)}.

module_deploy(#{}) ->
    ok.