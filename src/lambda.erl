%% @doc
%%  Lambda: API for publishing servers and turning modules into clients.
%%
%% @end
-module(lambda).
-author("maximfca@gmail.com").

%% API
-export([
    rewrite/3
]).

%% Erlang node address. Expected to be extended to add DNS and other discovery layers.
-type address() ::
    {ip, inet:ip_address(), inet:port_number()} |   %% IP address and port number
    {dns, string(), inet:port_number()} |           %% DNS name and port number
    {epmd, string()}.                               %% DNS name, epmd

%% Process location (similar to emgr_name()). Process ID (pid)
%%  designates both node name and ID. If process ID is not known,
%%  locally registered process name can be used.
-type location() :: pid() | {atom(), node()}.

%% Points: maps location to the address
-type points() :: #{location() => lambda_epmd:address()}.

-export_type([address/0, location/0, points/0]).

%% Lambda options
-type options() :: #{}.

%%--------------------------------------------------------------------
%% @doc Rewrites existing module (subset of functions) into remotely
%%      executed.
-spec rewrite(module(), Capacity :: pos_integer(), options()) -> ok.
rewrite(_Module, _Capacity, _Options) ->
    %% read module information, and replace all functions with proxies,
    %%  excluding system-defined function and exclusions by user.
    ok.
