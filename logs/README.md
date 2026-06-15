# Flight Logs

Log files are written here each time the app starts, named by launch timestamp.

## File naming

```
YYYYMMDD_HHMMSS.log
```

e.g. `20260615_074852.log`

## File format

Plain text, one record per line. Lines beginning with `#` are comments written at the top of each file.

### Record types

#### TELEM
Telemetry snapshot written once per second for each connected drone.

```
2026-06-15T07:48:52.123456 TELEM sysid=1 lat=64.5752570 lon=-149.4194180 alt=12.30 batt=87.5 hdg=043.2 armed=1 airborne=1
```

| Field | Description |
|---|---|
| `sysid` | Drone system ID |
| `lat` / `lon` | GPS position (7 decimal places) |
| `alt` | Altitude in metres |
| `batt` | Battery percentage |
| `hdg` | Heading in degrees (0–360) |
| `armed` | `1` = armed, `0` = disarmed |
| `airborne` | `1` = airborne (alt > 0.5 m), `0` = on ground |

#### EVENT
Logged when a drone connects or disconnects.

```
2026-06-15T07:48:52.123456 EVENT sysid=1 event=CONNECT
2026-06-15T07:48:52.123456 EVENT sysid=1 event=DISCONNECT
```

#### CMD
Logged when a command is sent from the dashboard.

```
2026-06-15T07:48:52.123456 CMD sysid=1 action=takeoff
2026-06-15T07:48:52.123456 CMD sysid=1 action=goto_target lat=64.575300 lon=-149.419500 alt=30.0 yaw=180.0
```

The optional trailing fields after `action=` are free-form detail and are only present for commands that carry parameters (e.g. `goto_target`).
