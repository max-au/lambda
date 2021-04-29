# Debugging cluster
Tips and tricks

## Manual node name override
Use `lambda_discovery:set_node(Node, {{127, 0, 0, 1}, 4370}).` to override a `Node` IP address and port.

## Logging from peer
Use `lambda_test:logger_config([lambda_broker, lambda_bootstrap, lambda_authority])` to provide logger output to console.