#!/usr/bin/env python3
"""
test_drone_sim.py — multi-drone telemetry + command simulator

Reads assets/drone_ips_test.txt for the list of IPs and names.
Each drone binds its own servers to its assigned IP:
  Telemetry: <drone-ip>:8081  (TCP, JSON lines)
  Commands:  <drone-ip>:8080  (HTTP POST)

This means commands from the dashboard route to the correct drone
individually — 127.0.0.1, 127.0.0.2, 127.0.0.3, etc. are all valid
loopback addresses on Windows with no extra setup required.

Supported commands:
  POST /send/takeoff          — LANDED → ascend to cruise alt → ORBIT
  POST /send/land             — any → descend → LANDED
  POST /send/RTH              — any → fly to circle centre → descend → LANDED
  POST /send/abortMission     — treated as RTL
  POST /send/gotoWP           — body: "lat,lon,alt"
  POST /send/gotoWPwithPID    — body: "lat,lon,alt,yaw,speed"

Drones start ORBIT (airborne). Circle centres are spaced ~500 m apart east-west.
"""

import enum
import json
import math
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ── Mission parameters ───────────────────────────────────────────────────────
CENTER_LAT      = 64.5752898828691
CENTER_LON      = -149.51137196607007
CIRCLE_ALT_M    = 67.0
CIRCLE_RADIUS_M = 80.0
ORBIT_SPEED_MPS = 5.0

BATT_START_PCT  = 100.0
BATT_END_PCT    = 10.0
BATT_DRAIN_S    = 600.0

TELEM_PORT  = 8081
CMD_PORT    = 8080
TICK_HZ     = 5
SPACING_M   = 500.0
CLIMB_SPEED = 2.0    # m/s vertical
LAND_SPEED  = 1.5    # m/s vertical

IPS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "drone_ips_test.txt")

# ── Derived constants ────────────────────────────────────────────────────────
_M_PER_DEG_LAT = 111320.0
_M_PER_DEG_LON = 40_075_000.0 * math.cos(math.radians(CENTER_LAT)) / 360.0
ORBIT_PERIOD_S  = (2 * math.pi * CIRCLE_RADIUS_M) / ORBIT_SPEED_MPS

_start = time.time()


# ── State machine ────────────────────────────────────────────────────────────
class Mode(enum.Enum):
    ORBIT   = "AUTO"
    GOTO    = "GOTO"
    HOLD    = "HOLD"
    RTL     = "RTL"
    LANDING = "LANDING"
    LANDED  = "LANDED"
    TAKEOFF = "TAKEOFF"


class DroneState:
    def __init__(self, ip, centre_lat, centre_lon, phase, label, drain):
        self.ip         = ip
        self.centre_lat = centre_lat
        self.centre_lon = centre_lon
        self.phase      = phase
        self.label      = label
        self.drain      = drain
        self.lock       = threading.Lock()

        self.mode    = Mode.ORBIT
        self.lat, self.lon = self._orbit_pos(0.0)
        self.alt     = CIRCLE_ALT_M
        self.heading = 0.0

        self.target_lat = centre_lat
        self.target_lon = centre_lon
        self.target_alt = CIRCLE_ALT_M

    def _orbit_pos(self, elapsed):
        angle = (2 * math.pi * elapsed / ORBIT_PERIOD_S + self.phase) % (2 * math.pi)
        lat = self.centre_lat + CIRCLE_RADIUS_M * math.cos(angle) / _M_PER_DEG_LAT
        lon = self.centre_lon + CIRCLE_RADIUS_M * math.sin(angle) / _M_PER_DEG_LON
        return lat, lon

    def _orbit_heading(self, elapsed):
        angle = (2 * math.pi * elapsed / ORBIT_PERIOD_S + self.phase) % (2 * math.pi)
        return (math.degrees(angle) + 90.0) % 360.0

    def command_takeoff(self):
        with self.lock:
            if self.mode == Mode.LANDED:
                self.mode = Mode.TAKEOFF
                print(f"[SIM:{self.label}] TAKEOFF")

    def command_land(self):
        with self.lock:
            if self.mode not in (Mode.LANDED, Mode.LANDING):
                self.mode = Mode.LANDING
                print(f"[SIM:{self.label}] LAND")

    def command_rtl(self):
        with self.lock:
            self.target_lat = self.centre_lat
            self.target_lon = self.centre_lon
            self.target_alt = self.alt
            self.mode = Mode.RTL
            print(f"[SIM:{self.label}] RTL → ({self.centre_lat:.5f}, {self.centre_lon:.5f})")

    def command_goto(self, lat, lon, alt):
        with self.lock:
            self.target_lat = lat
            self.target_lon = lon
            self.target_alt = alt
            self.mode = Mode.GOTO
            print(f"[SIM:{self.label}] GOTO ({lat:.5f}, {lon:.5f}, {alt:.1f}m)")

    def tick(self, elapsed, dt):
        with self.lock:
            mode = self.mode

            if mode == Mode.ORBIT:
                self.lat, self.lon = self._orbit_pos(elapsed)
                self.alt     = CIRCLE_ALT_M
                self.heading = self._orbit_heading(elapsed)

            elif mode in (Mode.GOTO, Mode.RTL):
                dlat_m = (self.target_lat - self.lat) * _M_PER_DEG_LAT
                dlon_m = (self.target_lon - self.lon) * _M_PER_DEG_LON
                dist   = math.hypot(dlat_m, dlon_m)
                step   = ORBIT_SPEED_MPS * dt
                if dist <= step:
                    self.lat  = self.target_lat
                    self.lon  = self.target_lon
                    self.mode = Mode.LANDING if mode == Mode.RTL else Mode.HOLD
                else:
                    ratio = step / dist
                    self.lat += dlat_m * ratio / _M_PER_DEG_LAT
                    self.lon += dlon_m * ratio / _M_PER_DEG_LON
                    self.heading = math.degrees(math.atan2(dlon_m, dlat_m)) % 360.0

            elif mode == Mode.LANDING:
                self.alt = max(0.0, self.alt - LAND_SPEED * dt)
                if self.alt == 0.0:
                    self.mode = Mode.LANDED
                    print(f"[SIM:{self.label}] LANDED")

            elif mode == Mode.TAKEOFF:
                self.alt = min(CIRCLE_ALT_M, self.alt + CLIMB_SPEED * dt)
                if self.alt >= CIRCLE_ALT_M:
                    self.mode = Mode.ORBIT
                    print(f"[SIM:{self.label}] Reached altitude — ORBIT")

            # HOLD / LANDED: no movement

            frac    = min(elapsed / self.drain, 1.0)
            battery = round(BATT_START_PCT - frac * (BATT_START_PCT - BATT_END_PCT), 1)

            if self.mode in (Mode.ORBIT, Mode.GOTO, Mode.RTL):
                h_rad = math.radians(self.heading)
                spd_x = round(ORBIT_SPEED_MPS * math.sin(h_rad), 2)
                spd_y = round(ORBIT_SPEED_MPS * math.cos(h_rad), 2)
            else:
                spd_x, spd_y = 0.0, 0.0

            return {
                "location": {
                    "latitude":  round(self.lat, 8),
                    "longitude": round(self.lon, 8),
                    "altitude":  round(self.alt, 2),
                },
                "heading":                round(self.heading, 1),
                "batteryLevel":           battery,
                "flightMode":             self.mode.value,
                "isManualOverrideActive": False,
                "speed":                  {"x": spd_x, "y": spd_y, "z": 0.0},
            }


# ── Per-drone telemetry server ───────────────────────────────────────────────
def _telem_handle(conn, addr, state: DroneState):
    print(f"[SIM:{state.label}] Telemetry connected from {addr}")
    interval = 1.0 / TICK_HZ
    t_prev   = time.time()
    try:
        while True:
            now     = time.time()
            dt      = now - t_prev
            t_prev  = now
            frame   = state.tick(now - _start, dt)
            conn.sendall((json.dumps(frame) + "\n").encode("utf-8"))
            time.sleep(interval)
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    finally:
        conn.close()
        print(f"[SIM:{state.label}] Telemetry disconnected from {addr}")


def _telem_server(state: DroneState):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((state.ip, TELEM_PORT))
    srv.listen(5)
    srv.settimeout(1.0)
    print(f"[SIM:{state.label}] Telemetry  {state.ip}:{TELEM_PORT}")
    try:
        while True:
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            threading.Thread(target=_telem_handle, args=(conn, addr, state), daemon=True).start()
    finally:
        srv.close()


# ── Per-drone command server ─────────────────────────────────────────────────
def _make_handler(state: DroneState):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body   = self.rfile.read(length).decode("utf-8").strip()
            path   = self.path

            if path == "/send/takeoff":
                state.command_takeoff()
            elif path == "/send/land":
                state.command_land()
            elif path in ("/send/RTH", "/send/abortMission"):
                state.command_rtl()
            elif path in ("/send/gotoWP", "/send/gotoWPwithPID"):
                try:
                    parts = body.split(",")
                    state.command_goto(float(parts[0]), float(parts[1]), float(parts[2]))
                except (ValueError, IndexError):
                    print(f"[CMD:{state.label}] Bad gotoWP body: {body!r}")

            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, *_):
            pass

    return Handler


# ── Load IPs from file ───────────────────────────────────────────────────────
def _load_entries():
    try:
        entries = []
        with open(IPS_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                ip    = parts[0]
                name  = parts[1] if len(parts) > 1 else ip
                entries.append((ip, name))
        return entries or [("127.0.0.1", "Sim1")]
    except FileNotFoundError:
        print(f"[SIM] Warning: {IPS_FILE} not found — defaulting to 127.0.0.1")
        return [("127.0.0.1", "Sim1")]


def _centre_for(index, n):
    offset_m = (index - (n - 1) / 2.0) * SPACING_M
    return CENTER_LAT, CENTER_LON + offset_m / _M_PER_DEG_LON


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    entries = _load_entries()
    n       = len(entries)

    drones = []
    for i, (ip, name) in enumerate(entries):
        lat, lon = _centre_for(i, n)
        phase    = i * (2 * math.pi / n)
        drain    = BATT_DRAIN_S * (0.8 + 0.4 * i / max(n - 1, 1))
        drones.append(DroneState(ip, lat, lon, phase, name, drain))

    print(f"[SIM] {n} drone(s) | orbit {ORBIT_PERIOD_S:.1f}s | spacing {SPACING_M:.0f}m\n")

    for drone in drones:
        print(f"[SIM] {drone.label} ({drone.ip})  centre=({drone.centre_lat:.5f}, {drone.centre_lon:.5f})  drain={drone.drain:.0f}s")
        threading.Thread(target=_telem_server, args=(drone,), daemon=True).start()
        cmd_srv = ThreadingHTTPServer((drone.ip, CMD_PORT), _make_handler(drone))
        threading.Thread(target=cmd_srv.serve_forever, daemon=True).start()
        print(f"[SIM:{drone.label}] Commands   {drone.ip}:{CMD_PORT}")

    print(f"\n[SIM] All servers running. Ctrl-C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SIM] Stopped.")


if __name__ == "__main__":
    main()
