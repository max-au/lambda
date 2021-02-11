# Authority model
Authority implements mesh-connected ensemble.

## Ensemble discovery
Authority process accepts:
 * 'peers' message containing list of potential candidates for other authorities.
   When this message is matched, authority emits 'authority' message for every peer
   in the list, which serves as discovery process.
 * 'discover' message from brokers. Authority monitors all discovered brokers, and
   sends 'authority' message (as confirmation of discovery)
 * 'authority' message from other authorities in the ensemble.
   Upon matching, authority emits 'authority' message to all known brokers.
   The message also contains list of potentially new authorities to discover,
   triggering more discoveries
 * 'DOWN' message from another broker or authority.

Authority process emits:
 * 'authority' message contains contact information for the authority, and a list
   of all other known authorities in the ensemble with their contacts.
   
## Exchange discovery

 * brokers can send 'exchange' message to authority, which creates a subscription
   for this broker/exchange combination