# Deployment example

This example demonstrates how to quickly deploy a new micro-service in the lambda cloud.

## Prerequisites
Lambda authority should be running. Self-hosted local authority may be started with:
```bash
  ERL_FLAGS="-lambda authority true" rebar3 shell --sname authority --setcookie lambda
```

Lambda tier running `calc` application should be running and known to authority. Run:
```bash
  lctl service list
```

## Creating a new application
See VERSIONING.md demonstrating how to create a new release with
new `vdemo` application.

## Creating a new service
Create a new module, `trig.erl`, in the `vdemo` application.
```erlang
-module(trig).
-export([cos/1]).
cos(Arg) ->
    math:cos(Arg).
```

Deploy the module to existing `vdemo` application:
```erlang
  lambda:deploy(trig, #{capacity => 2}).
```
Lambda framework is aware that new service belongs to `vdemo`
application, hence deployment happens only to hosts already
running it.

 Ensure new service is published:
 ```bash
   lctl service list
 ```

Attempt to deploy existing service requires that new version
is published.
```erlang
  (lctl@max-au-mbp)4> lambda:deploy(trig, #{capacity => 2}).
  {error, already_exists}.
```

To bump the version, use `-vsn()` module attribute.