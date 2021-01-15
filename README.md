# lambda: Computing Service for Erlang
Modern network applications implement 3-layer design:
 * frontend (e.g. web server)
 * compute tier
 * persistent storage (e.g. database)

Lambda provides an implementation of a scalable stateless compute tier in Erlang.

## Basic Example
Put the code below (an Erlang module `calc` exporting Fibonacci function) into any temporary directory.
```erlang
-module(calc).
-export([fib/1]).

fib(0) -> 0;
fib(1) -> 1;
fib(N) when N > 0 ->
    fib(N - 1) + fib(N - 2).
```

Start an authority node: `ERL_FLAGS="-lambda authority true" rebar3 shell --name authority`, compile `calc` and publish it:
```
    Eshell V11.1.5  (abort with ^G)
    (back@max-au)1> ===> Booted lambda
    c("/tmp/calc.erl").
    {ok,calc}
    (back@max-au)2> lambda:publish(calc).
    ok
```
Start another shell, `rebar3 shell --name front`, discover `calc` and execute `fib(20)` remotely:
```
    Eshell V11.1.5  (abort with ^G)
    (front@max-au)1> ===> Booted lambda
    lambda:discover(calc).
    ok
    (front@max-au)2> calc:fib(20).
    6765.
    ok
```
To verify that `authority` node was processing the call, add side effect, and recompile:
```
    fib(0) -> io:format("Node: ~s~n", [node()]), 1;
...
    (back@max-au)1> c("/tmp/calc.erl").
    {ok,calc}
```

Add more processing capacity by simply starting another node `rebar3 shell --name more`.

## Advanced Example

### Backend
Create a new Erlang application using `rebar3 new app backend` and add `calc.erl` from the basic example.
Define `lambda` as a dependency of `backend` in `backend.app.src`:


Add `relx` release definition to `rebar.config`:

### Frontend
Create another release `frontend`, also depending on `lambda`, and implementing basic web server:

Frontend release does not contain any backend code at all.


Additional examples [available](doc/examples/BASIC.md).

## Design & Implementation

### Terminology
* *Node*. Instance of Erlang Run Time System (ERTS) running in a distributed
environment.
* *Process*. Erlang process running on a single node.
* *Server*.
* *Channel*.
* *PLB*.
* *Broker*.
* *Authority*.
* *Exchange*.

## API
Public API is provided via `lambda` module.

Programmatic APIs to use in releases.


## Running tests
Running any tests will fetch additional libraries as dependencies.
This suite uses PropEr library to simulate all possible state changes.
```bash
    rebar3 ct --cover && rebar3 cover --verbose
```
## Changelog

Version 1.0.0:
 - initial release
