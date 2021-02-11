# Authority model
Authority implements mesh-connected ensemble.

## Ensemble discovery
Authority process accepts:
 * 'peers' message containing list of potential candidates for other authorities.
   When this message is matched, authority emits 'authority' message for every peer
   in the list, containing origin (self) address and all other known authorities.
 * 'authority' message from other authorities in the ensemble, containing peer
   authority address and list of authorities known to peer.
   Upon matching, authority emits 'authority' message to all known brokers,
   and starts monitoring origin (sender of the message).
   Authority emits 'authority' messages to every other authority that is not
   yet monitored.
 * 'discover' message from brokers. Authority monitors all discovered brokers, and
   sends 'authority' message (as confirmation of discovery)
 * 'DOWN' message from another broker or authority.

Authority process emits:
 * 'authority' message contains contact information for the authority, and a list
   of all other known authorities in the ensemble with their contacts.
   
## Exchange discovery

 * brokers can send 'exchange' message to authority, which creates a subscription
   for this broker/exchange combination