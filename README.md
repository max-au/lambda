# lambda: Computing Service for Erlang
Modern network applications implement 3-layer design:
 * frontend (e.g. web server)
 * compute tier
 * persistent storage (e.g. database)

Lambda provides an implementation of a scalable stateless compute tier in Erlang.

## Basic Example
This example runs Pi calculations in a remote node. At least 4-core CPU is required to
run it locally.

Code below implements `calc` exporting Pi calculation. Put it into a temporary directory.
```erlang
-module(calc).
-export([pi/1]).

pi(Precision) ->
    pi(4, -4, 3, Precision).

pi(LastResult, Numerator, Denominator, Precision) ->
    NextResult = LastResult + Numerator / Denominator,
    Pow = math:pow(10, Precision),
    case trunc(LastResult * Pow) =:= trunc(NextResult * Pow) of
        true ->
            trunc(NextResult * Pow) / Pow;
        false ->
            pi(NextResult, -1 * Numerator, Denominator+2, Precision)
    end.
```

Start an authority node: `ERL_FLAGS="-lambda authority true" rebar3 shell --name authority`, compile `calc` and 
publish it with small capacity:
```
    (authority@max-au)1> ===> Booted lambda
    c("/tmp/calc.erl").
    {ok,calc}
    (authority@max-au)2> lambda:publish(calc, #{capacity => 2}).
    ok
```
Start another shell, `rebar3 as test shell --name front`, discover `calc` and execute `pi(5)` remotely:
```
    (front@max-au)1> ===> Booted lambda
    lambda:discover(calc).
    ok
    (front@max-au)2> calc:pi(5).
    6765.
    ok
```
Use `erlperf` to verify maximum concurrency of 2: 
```
    (front@max-au)1> application:start(erlperf).
    ok
    (front@max-au)6> erlperf:run({calc, pi, [4]}, #{}, #{}).                      
    {166,2}
```
Output above means that `erlperf` detected total throughput of 166 `pi` calls per second, while running 2 concurrent
processes (matching capacity published from `authority` node).

Add more processing capacity by simply starting another node `rebar3 shell --name more`:
```
    (more@max-au)1> c("/tmp/calc.erl").
    {ok,calc}
    (more@max-au)2> lambda:publish(calc, #{capacity => 2}).
    ok
```
Ensure `front` received more capacity:
```
    (front@max-au)6> erlperf:run({calc, pi, [4]}, #{}, #{}).                      
    {316,4}
```

## Advanced Example
[See Math - scalable 3-tier application](doc/MATH.md).

## Use cases
Primary use-case is to enable remote code execution, and support remote tier lifecycle. It includes but
not limited to:
 * moving code to remote tier
 * updating remote tier code via hot code load
 * safe API update and deployment

## Design
[See DESIGN.md](doc/DESIGN.md)

## API
Public API is provided via `lambda` module. Lambda framework can supervise publishing and
discovery. Use release configuration (`sys.config`) to publish and/or discover modules:
```erlang
[
    {lambda, [
        {publish, calc},
        {discover, basic}
    ]}
].
```

It is also supported to publish and discover modules under your application supervision,
adding appropriate child specs to supervisor:
```erlang
    ChildSpes = [
        #{
            id => published_calc,
            start => {lambda, start_publish, [calc]}
        }
    ]
```


## Running tests
Running any tests will fetch additional libraries as dependencies.
This suite uses PropEr library to simulate all possible state changes.
```bash
    rebar3 ct --cover && rebar3 cover --verbose
```
## Changelog

Version 0.1.0:
 - initial release
