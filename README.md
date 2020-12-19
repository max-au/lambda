# lambda: Erlang Remote Processing Framework

## Design & Implementation
Terminology
* *Node*. Instance of Erlang Run Time System (ERTS) running in a distributed
environment.
* *Process*. Erlang process running on a single node.

## API

## Implementation details
Below are implementation details, subject to change without further notice.

### Capacity discovery protocol

1. bootstrap:  
   monitor cluster membership  
   broadcast ```{discover, self()}``` to

### Backpressure implementation protocol

Request spawning protocol.

## Build
This project has no compile-time or run-time dependencies.

```bash
    $ rebar3 compile
```

### Running tests
Smoke tests are implemented in lambda_SUITE.

Running any tests will fetch additional libraries as dependencies. This is only
necessary to run the benchmarks (that are disabled by default). Running smoke
tests is as easy as:
    
    $ rebar3 ct --cover && rebar3 cover --verbose

Running property-based tests:

    $ rebar3 ct --suite lambda_proper_SUITE

This suite uses PropEr library to simulate all possible state changes.

### Formal model
Used by PropEr library to generate stateful test call sequence.

Generated events:
 * start peer node (up to some limit)
 
Properties:
 * group contains all processes that joined the group on all dist-visible nodes running the same scope

## Changelog

Version 1.0.0:
 - initial release
