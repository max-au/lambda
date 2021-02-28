# Versioning demo
Script for lambda demo, explaining simple versioning approach.
This scenario is partly covered in `lambda_readme_SUITE`.

WARNING: actual versioning model is WORK IN PROGRESS. It is not robust enough,
and likely to support additional mechanism (e.g. source control hash, md5 of
the compiled module, hash of an exports table, and more)

## Authority
If you are using self-hosted authority tier, start the authority locally.

## V1
Create a new release including lambda application:
```
    rebar3 new release vdemo
    cd vdemo
```

Add lambda as a dependency to rebar.config, and include it in the release:
```
{deps, [lambda]}
<...>
{release, {vdemo, "0.1.0"},
         [vdemo, lambda,
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

Create the production release, and run it:
```bash
    rebar3 as prod release
```

Start your client release:
```
    rebar3 shell
```


## V2
Change `vdemo.erl` adding `fqdn/0` and add version attribute:
```erlang
-export([fqdn/0]).

-lambda({vsn, 2}).
fqdn() ->
    timer:sleep(10),
    {ok, Host} = inet:gethostname(),
    Host ++ inet_db:res_option(domain).
```

Start another client and discover `v2`:
```
    rebar3 shell --sname second
    1>
```

## Bonus
Since v1 client does not require specific version, it will receive
an increase in throughput when v2 server joins the cluster.

```erlang
    erlperf:run()
```