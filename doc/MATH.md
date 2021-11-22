# Math: 3-tier system demo
This example demonstrates lambda usage in a classic 3-tier network application.

## Releases
Single umbrella application contains several releases:

* Frontend: contains server connection termination logic. Edge application implements a simple TCP acceptor, and running process per connection.
* Backend: stateless logic. Calc application implements math calculations.
* Storage: persistent database.

Client application (math) can be used from rebar3 shell, or via `telnet`.

## Edge implementation