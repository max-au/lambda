# Lambda: design and implementation
Lambda design is completely non-blocking. Lambda is implemented using message exchange protocol. No "calls"
are allowed in the implementation.

## Terminology
* *Authority*. Central component implementing a distributed service discovery mechanism. Every authority is
  connected to all other authorities, forming full mesh. Authority maintains the list of all exchanges in the
  system, and may start a new exchange when needed (e.g. network split, or new exchange needed).
* *Broker*. Connects to exchanges and forwards orders to/from listeners/plbs. Maintains connections to all
  known authorities. Broker is needed to fan out orders to multiple exchanges (for reliability purposes),
  and then deduplicate responses.
* *Exchange*. Receives orders from listeners and plbs and matches these orders. When there is a match, full
  or partial, exchange sends listener ("seller") contact information to plb ("buyer"). Exchange does not
  operate with listeners/plbs directly, but works through brokers. There are as many exchanges as there
  are different modules published or requested to discover.
* *Bootstrap*. Provides initial information for broker to find an authority (or another broker that knows
  an address of any authority). It is also used to let authorities find each other for the first time. A
  bootstrap can be as simple as a list of IP:PORT combinations, or as complex as custom service locator
  implementation. Lambda provides a number of built-in bootstrap implementations (static IP, DNS resolver,
  local `epmd`)
* *Discovery*. [Alternative implementation of Erlang node discovery](http://erlang.org/doc/apps/erts/alt_disco.html).
  Broker supplies node addresses via discovery component to be used by Erlang Distribution.
* *Listener*. Acts similar to TCP listening socket. Client makes a connection attempt,
  listener accepts it, spawning new channel.
* *Channel*. Server-side component supervising currently running requests, sending demand tokens to plb.
* *PLB*. Connected to multiple channels, a PLB is a Probabilistic Load Balancer.
* *Module*. Erlang module that is published or being discovered. Module has attributes (e.g. version).
  Similar to hot code load, old modules cannot be removed from the system unless there are no processes
  currently using the module.

Generic Erlang terms:
* *Node*. An instance of an Erlang Run Time System (ERTS) running in a distributed
  environment.
* *Process*. Erlang process running on a single node.

## Publishing
An Erlang node that wants to provide specific compute capacity (execute some module remotely)
goes through the following sequence:
 * listener sends "sell" message to a broker, creating an outstanding "sell" order
 * broker forwards "sell" order to known exchanges and requests all known authorities to
   supply more exchanges for the module
 * while order is active, for a new exchange is discovered, broker forward "sell" order
 * eventually buyers discover the listener and connect top it, reducing remaining listener 
   capacity. Listener updates outstanding "sell" order accordingly, and broker forwards
   updates to exchanges

## Discovery
Same sequence as publishing. PLB creates "buy" order which remains active until fully
satisfied.

## Order matching
Exchange receives "sell" and "buy" orders. Every "sell" order contains details explaining
how a PLB (buyer) can connect to listener (seller).

## Current limitations
Lambda implementation does not allow more than one outstanding "sell" or "buy" order per
listener and PLB accordingly.

# Update and hot code load
PLB can perform hot code reload of the client-side proxy, and listener can publish new
code of a module when it is loaded. Implementation follows OTP practices, when a new
PLB is started first, acquiring necessary capacity, then performing hot code load to
update proxy code.