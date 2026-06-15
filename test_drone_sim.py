#!/usr/bin/env python3
"""
test_drone_sim.py — single-drone telemetry simulator

Listens on TCP 127.0.0.1:8081 and streams JSON telemetry lines that the
DJIInterface._telemetry_receiver loop can consume directly.

Usage:
    python test_drone_sim.py

Then make sure drone_ips.txt contains:
    127.0.0.1, Sim
"""

import json
import math
import socket
import threading
import time

# ── Mission parameters ───────────────────────────────────────────────────────
CENTER_LAT      = 64.5752898828691
CENTER_LON      = -149.51137196607007
CIRCLE_ALT_M    = 67.0        # metres AGL (constant)
CIRCLE_RADIUS_M = 80.0        # metres from centre
ORBIT_SPEED_MPS = 5.0         # ground speed — sets orbit period

BATT_START_PCT  = 100.0
BATT_END_PCT    = 10.0
BATT_DRAIN_S    = 600.0       # seconds to drop from start to end

HOST      = "127.0.0.1"
PORT      = 8081
TICK_HZ   = 5                 # telemetry frames per second

# ── Derived constants ────────────────────────────────────────────────────────
_M_PER_DEG_LAT = 111320.0
_M_PER_DEG_LON = 40_075_000.0 * math.cos(math.radians(CENTER_LAT)) / 360.0
ORBIT_PERIOD_S  = (2 * math.pi * CIRCLE_RADIUS_M) / ORBIT_SPEED_MPS

print(f"[SIM] Centre    : {CENTER_LAT:.7f}, {CENTER_LON:.7f}")
print(f"[SIM] Altitude  : {CIRCLE_ALT_M} m")
print(f"[SIM] Radius    : {CIRCLE_RADIUS_M} m")
print(f"[SIM] Orbit period : {ORBIT_PERIOD_S:.1f} s  ({ORBIT_PERIOD_S/60:.1f} min)")
print(f"[SIM] Battery drain: {BATT_DRAIN_S:.0f} s  ({BATT_DRAIN_S/60:.0f} min)")


# ── Telemetry generation ─────────────────────────────────────────────────────
_start = time.time()

def _telemetry_at(elapsed: float) -> dict:
    """Compute one telemetry frame for the given elapsed time."""
    # Clockwise orbit: angle advances with time
    angle = (2 * math.pi * elapsed / ORBIT_PERIOD_S) % (2 * math.pi)
    dlat  = CIRCLE_RADIUS_M * math.cos(angle) / _M_PER_DEG_LAT
    dlon  = CIRCLE_RADIUS_M * math.sin(angle) / _M_PER_DEG_LON

    # Tangent heading for clockwise orbit (degrees from north)
    heading = (math.degrees(angle) + 90.0) % 360.0

    # Battery: linear drain, floor at BATT_END_PCT
    frac = min(elapsed / BATT_DRAIN_S, 1.0)
    battery = round(BATT_START_PCT - frac * (BATT_START_PCT - BATT_END_PCT), 1)

    return {
        "location": {
            "latitude":  round(CENTER_LAT + dlat, 8),
            "longitude": round(CENTER_LON + dlon, 8),
            "altitude":  CIRCLE_ALT_M,
        },
        "heading":             round(heading, 1),
        "batteryLevel":        battery,
        "flightMode":          "AUTO",
        "isManualOverrideActive": False,
    }


# ── Client handler ───────────────────────────────────────────────────────────
def _handle(conn: socket.socket, addr):
    print(f"[SIM] Dashboard connected from {addr}")
    interval = 1.0 / TICK_HZ
    try:
        while True:
            frame = _telemetry_at(time.time() - _start)
            line  = json.dumps(frame) + "\n"
            conn.sendall(line.encode("utf-8"))
            time.sleep(interval)
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    finally:
        conn.close()
        print(f"[SIM] Dashboard disconnected from {addr}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(5)
    print(f"[SIM] Listening on {HOST}:{PORT}  (Ctrl-C to stop)\n")

    try:
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=_handle, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print("\n[SIM] Stopped.")
    finally:
        srv.close()


if __name__ == "__main__":
    main()
