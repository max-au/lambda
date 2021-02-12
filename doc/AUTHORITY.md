# Authority model
Authority implements mesh-connected ensemble.

## Ensemble discovery
 * 'DOWN' message from another broker or authority.

Authority process emits:
 * 'authority' message contains contact information for the authority, and a list
   of all other known authorities in the ensemble with their contacts.
   
## Exchange discovery

 * brokers can send 'exchange' message to authority, which creates a subscription
   for this broker/exchange combination