%% @doc
%%  Lambda: API for publishing servers and turning modules into clients.
%%
%% @end
-module(lambda).
-author("maximfca@gmail.com").

%% API
-export([
    publish/1,
    discover/1
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

%%--------------------------------------------------------------------
%% @doc Discovers a module, and starts a PLB for that module under lambda supervision.
-spec discover(module()) -> ok.
discover(Module) ->
    lambda_plb_sup:start_plb(Module, #{low => 1, high => 10}).

%% @doc Publishes  a module, starting server under lambda supervision.
publish(Module) ->
    lambda_server_sup:start_server(Module, #{capacity => 10}).
