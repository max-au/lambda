# Versioning demo
Script for lambda demo, explaining simple versioning approach.
This scenario is covered by `lambda_readme_SUITE`.

WARNING: actual versioning model is WORK IN PROGRESS. It is not robust enough,
and likely to support additional mechanism (e.g. source control hash, md5 of
the compiled module, hash of an exports table, and more)

## Authority
If you are using self-hosted authority, start it locally,
running following command in lambda application root:
```bash
  ERL_FLAGS="-lambda authority true" rebar3 shell --sname authority --setcookie lambda
```

## V1
Create a new release:
```
    rebar3 new release vdemo
    cd vdemo
```

Add lambda and erlperf as a dependency to rebar.config, and include it in the release.
Keep debug symbols in production release.
```
{erl_opts, [debug_info]}.
{deps, [lambda]}.

{relx, [
    {release, {vdemo, "0.1.0"}, [vdemo, lambda, sasl]},
    {debug_info, keep},
    {sys_config, "./config/sys.config"},
    {vm_args, "./config/vm.args"}
]}.
```

Create a new module named `apps/vdemo/src/vdemo.erl`:
```erlang
-module(vdemo).
-export([where/0]).
where() ->
    timer:sleep(10),
    {ok, Host} = inet:gethostname(),
    Host.
```

Change `config/vm.args` to enable alternative service discovery:
```
-sname vdemo
-start_epmd false
-connect_all false
-setcookie lambda
-epmd_module lambda_discovery
```

Change `config/sys.config` to automatically publish `vdemo`:
```
[
  {lambda, [
    {publish, [{vdemo, #{capacity => 8}}]}
  ]}
].
```

Create the release, and run it:
```bash
    rebar3 release
    _build/default/rel/vdemo/bin/vdemo console
```

Start your client node using rebar3 shell in lambda application directory:
```
    rebar3 shell --sname client --setcookie lambda
    (client@max-au-mbp)2> {ok, VDemo} = lambda:discover(vdemo, #{capacity => 4}).
    {ok,<0.196.0>}
    (client@max-au-mbp)3> vdemo:where().
    "max-au-mbp"
    (client@max-au-mbp)4> lambda_plb:capacity(VDemo).
    3
```

## V2
Change `vdemo.erl` adding `fqdn/0` and add version attribute:
```erlang
-export([fqdn/0]).

-lambda({vsn, 2}).
fqdn() ->
    timer:sleep(10),
    {ok, Host} = inet:gethostname(),
    Host ++"." ++ inet_db:res_option(domain).
```

Change node name in vm.args to run two releases concurrently:
```
-sname vdemo2
```

Create a new release and run it:
```
    rebar3 release
    _build/default/rel/vdemo/bin/vdemo console
```

Start one more client shell, and request a v2 of vdemo:
```
    rebar3 shell --sname client2 --setcookie lambda
    (client2@max-au-mbp)1> {ok, VDemo2} = lambda:discover(vdemo, #{vsn => 2, capacity => 4}).
    {ok,<0.196.0>}
    (client2@max-au-mbp)2> vdemo:fqdn().
    "max-au-mbp.localdomain"
```

## Bonus
What happens when version is not specified? Close second shell and restart, then
discover `vdemo`

```erlang
    (client2@max-au-mbp)7> {ok, VDemo} = lambda:discover(vdemo, #{capacity => 4}).
    {ok,<0.195.0>}
```

The behaviour is not reproducible. It may happen that `fqdn/0` is defined, but
it may also happen it is not, depending on which server is connected first.
Both cases will result in `error:undef` exception calling `vdemo:fqdn()`,
but in the former case, no remote call is made (and no token consumed).

# Simplified versioning example
Start authority in a shell:
```
    ERL_FLAGS="-lambda authority true" rebar3 shell --sname authority
```

Create the same "vdemo.erl" code (in `/tmp/vdemo.erl`).

Start a new shell and compile vdemo.erl there:
```
    rebar3 shell --sname server
    (server@max-au-mbp)1> c("/tmp/vdemo.erl").
    {ok,vdemo}
    (server@max-au-mbp)2> lambda:publish(vdemo, #{capacity => 4}).
    {ok,<0.207.0>}
```

Start a client shell, discover vdemo:

```
    rebar3 shell --sname client
    (client@max-au-mbp)1> lambda:discover(vdemo, #{capacity => 2}).
    {ok,<0.196.0>}
```

Edit `/tmp/vdemo.erl` to add `fqdn/0`, and start a second server:
```
    rebar3 shell --sname server2
    (server2@max-au-mbp)1> c("/tmp/vdemo.erl").
    {ok,vdemo}
    (server2@max-au-mbp)2> lambda:publish(vdemo, #{capacity => 4}).
    {ok,<0.207.0>}
```

Start a second client, and discover specific version of vdemo:
```
    rebar3 shell --sname client2
    (client2@max-au-mbp)1> lambda:discover(vdemo, #{capacity => 2, vsn => 2}).
    {ok,<0.204.0>}
    (client2@max-au-mbp)2> vdemo:fqdn().
    "max-au-mbp."
```
