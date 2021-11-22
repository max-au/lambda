# Debugging cluster
Tips and tricks

## Manual node name override
Use `lambda_discovery:set_node(Node, {{127, 0, 0, 1}, 4370}).` to override a `Node` IP address and port.

## Logging from peer
Use `lambda_test:logger_config([lambda_broker, lambda_bootstrap, lambda_authority])` to provide logger output to console.

## Bulletproof logging
Add this to lambda_broker/authority/plb/... to override ?LOG_DEBUG macro:
```erlang
%-undef (LOG_DEBUG).
%-define (LOG_DEBUG(X, Y, Z), io:format(standard_error, X ++ "~n", Y)).
```

## Running second authority with epmd
Use this, assuming first one is 'authority':
`ERL_FLAGS="-lambda authority true -lambda bootspec {epmd,[authority]}" rebar3 shell --sname auth2`