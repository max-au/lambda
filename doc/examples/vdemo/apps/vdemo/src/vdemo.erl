-module(vdemo).
-export([where/0]).

%-export([fqdn/0]).
%-lambda({vsn, 2}).

where() ->
    timer:sleep(10),
    {ok, Host} = inet:gethostname(),
    Host.

%fqdn() ->
%    timer:sleep(10),
%    {ok, Host} = inet:gethostname(),
%    Host ++ inet_db:res_option(domain).