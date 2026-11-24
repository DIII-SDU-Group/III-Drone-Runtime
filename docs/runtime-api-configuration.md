# Runtime API Developer Configuration

`iii-runtime-api` is an unrestricted developer API on this research platform.
It accepts normal browser and CLI requests without a password, session token,
per-machine verifier file, receiver state, signed release identity, or firewall
policy. The default onboard environment is supplied by
`deployment/ansible/roles/runtime_control_plane/templates/runtime.env.j2`.

Useful settings are limited to normal runtime behavior:

- `III_RUNTIME_API_HOST` and `III_RUNTIME_API_PORT`
- `III_RUNTIME_API_PROFILE`
- `III_RUNTIME_API_MDNS_ENABLED` and `III_RUNTIME_API_MDNS_INSTANCE`
- `III_RUNTIME_API_PX4_MAVLINK_ENDPOINT` and `III_RUNTIME_API_PX4_ENABLED`
- `III_RUNTIME_API_LOG_DIR`

The absence of access control does not bypass vehicle safety. Flight command
handlers still use live vehicle state to reject unsafe actions such as arming
or mode changes when their physical preconditions are not met.
