# III-Drone-Runtime

Runtime-host control-plane package for III-Drone.

This repository owns:

- daemon transport/client utilities for the local III system daemon.
- `iii-runtime-api`, the FastAPI service that exposes runtime/operator control
  to GUI v2 and remote CLI workflows.
- runtime-host adapters for systemd, ROS/DDS, MAVLink/MAVSDK, logs, map
  aggregation, and operator command handlers.
- boot/session-aware runtime event emission compatible with receiver-owned
  retention, verified pull receipts, and explicit pre-clock uncertainty.
- small ROS-free geometry helpers used by runtime API map/projection shaping.

`III-Drone-Runtime` runs on the runtime host: the devcontainer/runtime
environment in simulation, and the onboard host in real deployments. Runtime
code must stay out of `III-Drone-GC`; the ground-control computer talks to this
package only over the runtime API.

Deployment and operator-network details are documented in
`docs/runtime-api-configuration.md` and
`../III-Drone-GC/docs/gui-v2-deployment.md`.

Useful smoke/acceptance docs:

- `../III-Drone-GC/docs/gui-v2-sim-e2e-smoke.md`
- `../III-Drone-GC/docs/gui-v2-real-profile-acceptance.md`
- `../III-Drone-GC/docs/gui-v2-security-checklist.md`

The real-aircraft workflow is authoritative in
`../../docs/field-inspection-operations.md`. Calibrated fixture staging in the
sim E2E runner is test setup only and is never an onboard mission input.

## Dependencies

Initial package boundaries:

- depends on `III-Drone-Contracts` for API schemas.
- depends on `III-Drone-Supervision` and `III-Drone-Interfaces` for runtime-side
  daemon/ROS integration.
- may use FastAPI, uvicorn, Pydantic, MAVSDK/pymavlink, and ROS runtime
  packages on the runtime host.
- does not depend on `III-Drone-Core` for GUI/API data shaping; runtime-facing
  math helpers live in `iii_drone_runtime.geometry` unless a future task
  explicitly narrows and documents a Core utility dependency.

## Development

```bash
python3 -m pytest test
```

From the workspace root, the full GUI v2/runtime suite is:

```bash
scripts/workspace/run_iii_test_suite.sh
```

## Handler Permission Metadata

Every command registered with `DispatchRegistry` carries explicit permission
metadata from `iii_drone_contracts.HandlerPermission`:

- `read_only`: diagnostics, status, lists, downloads, and validation calls that
  may remain available in Mission mode.
- `mutating`: operator mutations that must use the subsystem permission gate.
- `flight_critical`: PX4/control-owner/custom-operation starts that can move or
  interrupt the vehicle.
- `runtime_mutation`: daemon/system lifecycle mutations.

GUI and CLI policy reads this registry metadata through `/commands/handlers`;
it does not infer mutability from arbitrary command names. Mission mode rejects
custom-operation starts, gripper commands, perception mutations, and
configuration writes with explicit reasons while keeping declared read-only
diagnostics available.

## Operator Hold Contract

`px4.hold` is the single global flight intervention. Acceptance means the PX4
Hold request was dispatched; runtime control state then distinguishes PX4 Hold
confirmation, kinematically safe stopping of the deactivated action, and
clearing of mission/custom-operation ownership. A reconciliation timeout is a
degraded state and remains visible in control state and the event log.

Hold terminates the current autonomous run. It is not pause/resume: returning
to inspection requires a fresh explicit `mission.activate` request and fresh
eligibility validation. The inspection behavior tree's intentional recharge
cycle is separate and is the only mechanism that retains interrupted inspection
progress. There are no generic mission Resume, Abort, or Mission Land commands.
