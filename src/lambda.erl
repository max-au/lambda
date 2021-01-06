%% @doc
%%  Lambda: API for publishing servers and turning modules into clients.
%%
%% @end
-module(lambda).
-author("maximfca@gmail.com").

%% API
-export([
    start_link/3
]).

%% Erlang node address. Expected to be extended to add DNS and other discovery layers.
-type address() :: {inet:ip_address(), inet:port_number()}.

%% Process location (similar to emgr_name()). Process ID (pid)
%%  designates both node name and ID. If process ID is not known,
%%  locally registered process name can be used.
-type location() :: pid() | {atom(), node()}.

%% Points: maps location to the address and expiration time (absolute time)
-type points() :: #{location() => {lambda_discovery:address(), Expires :: infinity | non_neg_integer()}}.

-export_type([address/0, location/0, points/0]).

%% Lambda options
-type options() :: #{}.

%%--------------------------------------------------------------------
%% @doc Rewrites an existing module (subset of functions) into remotely
%%      executed, and starts a PLB for that module, linked to the
%%      current process.
-spec start_link(module(), Capacity :: pos_integer(), options()) -> ok.
start_link(_Module, _Capacity, _Options) ->
    %% read module information, and replace all functions with proxies,
    %%  excluding system-defined function and exclusions by user.
    ignore.
