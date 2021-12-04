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
        handler => optional,
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
                        handler => fun module_deploy/1,
                        arguments => [
                            #{name => app, type => string, help => "Application the module belongs to"},
                            #{name => module, type => string, help => "Module name"},
                            #{name => capacity, short => $c, long => "-capacity",
                                type => int, default => 10, help => "Capacity, per node"}
                        ]
                    }
                }
            },
            "application" => #{
                handler => fun application_list/1,
                commands => #{
                    "list" => #{handler => fun application_list/1},
                    "create" => #{
                        handler => fun application_create/1,
                        arguments => [
                            #{name => name, type => string, help => "Application name"},
                            #{name => nodes, long => "-capacity", type => int, default => 1,
                                help => "Number of nodes to deploy to"}
                        ]
                    },
                    "delete" => #{
                        handler => fun application_delete/1,
                        arguments => [
                            #{name => name, type => string, help => "Application name"}
                        ]
                    }
                }
            },
            "request" => #{
                handler => fun request_call/1,
                arguments => [
                    #{name => module, type => string, help => "Module name"},
                    #{name => call, type => string, help => "Function name and arguments"}
                ]
            }
        }
    }.

%% Common functions

%% joins lambda cluster - discovers authorities and returns the list
%%  of authorities
join() ->
    net_kernel:start([lctl, shortnames]),
    {ok, _Apps} = application:ensure_all_started(lambda),
    %% step 1: ensure bootstrap complete
    %% step 2: ensure broker processes bootstrap messages
    %% step 3: ensure discovery processes all expected sets
    %% step 4: broker has to find at least 1 authority, which in
    %%  turn has to report a list of other authorities running
    Ref = lambda_broker:watch(lambda_broker, self()),
    Known = lambda_broker:authorities(lambda_broker),
    lambda_bootstrap:discover(),
    wait_for_authorities(Ref, Known, [], 5000).

wait_for_authorities(_Ref, [_|_] = Known, [], _Timeout) ->
    lambda_broker:unwatch(lambda_broker, self()),
    Known;
wait_for_authorities(Ref, Known, Contacted, Timeout) ->
    receive
        {Ref, authority, NewKnown, NewContacted} ->
            Remaining = lists:usort(Contacted ++ NewContacted) -- NewKnown,
            wait_for_authorities(Ref, NewKnown, Remaining, Timeout)
    after Timeout ->
        lambda_broker:unwatch(lambda_broker, self()),
        Known == [] andalso error(not_connected)
    end.

%% leaves lambda cluster
leave() ->
    logger:set_primary_config(level, warning),
    application:stop(lambda),
    net_kernel:stop().

%%--------------------------------------------------------------------
%% Handlers: authority

authority_list(#{}) ->
    KnownAuth = join(),
    AuthInfo = [
        begin
            Node = node(Auth),
            #{addr := Ip, port := Port} = lambda_discovery:get_node(Node),
            io_lib:format("~s ~s:~b (~p)~n", [Node, inet:ntoa(Ip), Port, Auth])
        end || Auth <- KnownAuth],
    leave(),
    {ok, io_lib:format("Total: ~b~n", [length(KnownAuth)]) ++ lists:concat(AuthInfo)}.

%%--------------------------------------------------------------------
%% Handlers: applications
application_list(#{}) ->
    KnownAuth = join(),
    %% Horrible hack for demo purposes only
    %% It connects to all machines in lambda cluster
    AllApps = lists:concat([rpc:call(node(Auth), lambda_broker, applications, [Broker])
        || Auth <- KnownAuth, Broker <- lambda_authority:brokers(Auth)]),
    leave(),
    AppSorted = lists:ukeysort(1, AllApps),
    AppInfo = [io_lib:format("~s: ~s~n", [App, Desc]) || {App, Desc, _Vsn} <- AppSorted],
    {ok, io_lib:format("Total: ~b~n", [length(AllApps)]) ++ lists:concat(AppInfo)}.

application_create(#{name := Name, nodes := Nodes}) ->
    App = list_to_atom(Name),
    Spec = [{description, Name ++ " lambda"}, {vsn, "1.0.0"}, {applications, [kernel, stdlib, lambda]}],
    %% another horrible hack for demo purposes only
    KnownAuth = join(),
    %% skip nodes with authority, and this node()
    ExcludeFromDeploy = [node() | [node(A) || A <- KnownAuth]],
    AllNodes = lists:usort([node(Broker) || Auth <- KnownAuth, Broker <- lambda_authority:brokers(Auth),
        lists:member(node(Broker), ExcludeFromDeploy) =:= false]),
    [_|_] = ToDeploy = lists:sublist(AllNodes, Nodes),
    %% connect directly, create apps on the node itself, don't care about
    %%  restart. In real production this should be done interacting with some
    %%  code deployment orchestrator.
    [begin
         true = net_kernel:connect_node(N),
         ok = rpc:call(N, application, load, [{application, App, Spec}])
     end || N <- ToDeploy],
    leave(),
    {ok, io_lib:format("Deployed ~s to ~p~n", [App, ToDeploy])}.

application_delete(#{name := Name}) ->
    App = list_to_atom(Name),
    KnownAuth = join(),
    AllNodes = lists:usort([node(Broker) || Auth <- KnownAuth, Broker <- lambda_authority:brokers(Auth)]),
    [rpc:call(N, application, unload, [App]) || N <- AllNodes],
    {ok, io_lib:format("Deleted ~s from ~p~n", [App, AllNodes])}.

%%--------------------------------------------------------------------
%% Handlers: modules

module_list(#{}) ->
    KnownAuth = join(),
    %% find authorities
    AllModules = [lambda_authority:modules(Auth) || Auth <- KnownAuth],
    Modules = lists:usort(lists:concat(AllModules)),
    ModuleInfo = [
        begin
            io_lib:format("~s~n", [Mod])
        end || Mod <- Modules],
    leave(),
    {ok, io_lib:format("Total: ~b~n", [length(Modules)]) ++ lists:concat(ModuleInfo)}.

module_deploy(#{app := AppName, module := Module, capacity := Cap}) ->
    App = list_to_atom(AppName),
    KnownAuth = join(),
    Mod = list_to_atom(Module),
    Filename = code:which(Mod),
    {ok, Bin} = file:read_file(Filename),
    %% find nodes where the app is deployed
    ToDeploy = [node(Broker)
        || Auth <- KnownAuth, Broker <- lambda_authority:brokers(Auth),
            lists:keymember(App, 1, rpc:call(node(Auth), lambda_broker, applications, [Broker]))],
    %% another hack for demo purposes
    [begin
         true = net_kernel:connect_node(N),
         {module, Mod} = rpc:call(N, code, load_binary, [Mod, "lambda", Bin]),
         {ok, _Pid} = rpc:call(N, lambda, publish, [Mod, #{capacity => Cap}])
     end || N <- ToDeploy],
    %% check if the module is already deployed, FIXME: do this
    leave(),
    {ok, io_lib:format("Deployed ~s to ~p~n", [Mod, ToDeploy])}.

%%--------------------------------------------------------------------
%% Handlers: requests

request_call(#{module := Module, call := Text}) ->
    Mod = list_to_atom(Module),
    join(),
    {ok, _Plb} = lambda:discover(Mod, #{capacity => 1}),
    TempModule = temp_module(Module ++ ":" ++ Text),
    Result = erlang:apply(TempModule, run, []),
    leave(),
    {ok, io_lib:format("~tp~n", [Result])}.

temp_module(Call) ->
    Mod = list_to_atom(lists:concat([?MODULE_STRING, erlang:system_time(millisecond)])),
    Code = [
        lists:flatten(io_lib:format("-module(~s).\n", [Mod])),
        "-export([run/0]).\n",
        lists:flatten(io_lib:format("run() -> ~s.\n", [Call]))
    ],
    Tokens = [begin {ok, Tokens, _} = erl_scan:string(C), Tokens end || C <- Code],
    Forms = [begin {ok, F} = erl_parse:parse_form(T), F end || T <- Tokens],
    {ok, Mod, Bin} = compile:forms(Forms),
    {module, Mod} = code:load_binary(Mod, atom_to_list(Mod) ++ ".erl", Bin),
    Mod.