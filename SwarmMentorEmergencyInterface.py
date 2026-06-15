# -----------------------------
# FlightBoard.py
# -----------------------------
import time
import json
import math
import logging
import socket
import threading
from flask import Response
import tempfile
import os
import subprocess as sp
import threading
import requests
import dash_leaflet as dl
from datetime import datetime
from flask import send_from_directory

import dash
from dash import html, dcc, ctx
from dash.dependencies import Input, Output, State, ALL
from drone_logger import ULogWriter

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)  # Only show critical errors, hiding standard 200 POST logs

EP_STICK = "/send/stick"
EP_ZOOM = "/send/camera/zoom"
EP_GIMBAL_SET_PITCH = "/send/gimbal/pitch"
EP_GIMBAL_SET_YAW = "/send/gimbal/yaw"
EP_TAKEOFF = "/send/takeoff"
EP_LAND = "/send/land"
EP_RTH = "/send/RTH"
EP_ENABLE_VIRTUAL_STICK = "/send/enableVirtualStick"
EP_ABORT_MISSION = "/send/abortMission"
EP_GOTO_WP = "/send/gotoWP"
EP_GOTO_WP_PID = "/send/gotoWPwithPID"
EP_GOTO_ALTITUDE = "/send/gotoAltitude"
EP_GOTO_YAW = "/send/gotoYaw"


# -----------------------------
# Drone fleet controller
# -----------------------------
DRONE_LIST_FILE = os.getenv("SWARM_DRONE_LIST_FILE", os.path.join("assets", "drone_ips.txt"))
DRONE_COMMAND_PORT = int(os.getenv("SWARM_DRONE_COMMAND_PORT", "8080"))
DRONE_TELEMETRY_PORT = int(os.getenv("SWARM_DRONE_TELEMETRY_PORT", "8081"))
DRONE_CONNECT_TIMEOUT = float(os.getenv("SWARM_DRONE_CONNECT_TIMEOUT", "3.0"))
DRONE_RETRY_INTERVAL = float(os.getenv("SWARM_DRONE_RETRY_INTERVAL", "15.0"))
DRONE_SYNC_INTERVAL = float(os.getenv("SWARM_DRONE_SYNC_INTERVAL", "0.5"))



# -----------------------------
# DJI per-drone manager + fleet supervisor
# -----------------------------
def load_drone_ip_list(file_path):
    """Load drone IPs from a text or JSON file."""
    if not file_path or not os.path.exists(file_path):
        print(f"[FLEET] Drone list file not found: {file_path}")
        return []

    with open(file_path, "r", encoding="utf-8") as handle:
        raw_text = handle.read().strip()

    if not raw_text:
        return []

    if file_path.lower().endswith(".json") or raw_text[:1] in "[{":
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict):
            items = parsed.get("drones", parsed.get("ips", parsed.get("items", [])))
        else:
            items = parsed

        results = []
        for index, item in enumerate(items, start=1):
            if isinstance(item, dict):
                ip_address = item.get("ip") or item.get("address") or item.get("host")
                drone_id = int(item.get("id", index))
            else:
                ip_address = str(item).strip()
                drone_id = index

            if ip_address:
                name = item.get("name", "").strip() if isinstance(item, dict) else ""
                if not name:
                    name = ip_address.split(".")[-1].zfill(3)
                results.append({"id": drone_id, "ip": ip_address, "name": name})
        return results

    results = []
    for index, line in enumerate(raw_text.splitlines(), start=1):
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        if "," in stripped:
            ip_part, name = stripped.split(",", 1)
            ip_part = ip_part.strip()
            name = name.strip()
        else:
            ip_part = stripped
            name = ip_part.split(".")[-1].zfill(3)
        results.append({"id": index, "ip": ip_part, "name": name})
    return results


class DJIInterface:
    """Per-drone manager that keeps a TCP telemetry connection alive and sends HTTP commands."""

    def __init__(self, IP_RC="", drone_id=None, telemetry_port=8081, retry_interval=15.0, connect_timeout=5.0):
        self.IP_RC = IP_RC or ""
        self.drone_id = drone_id
        self.telemetryPort = telemetry_port
        self.retry_interval = retry_interval
        self.connect_timeout = connect_timeout
        self.baseCommandUrl = f"http://{self.IP_RC}:8080" if self.IP_RC else ""

        self._telemetry = {}
        self._telemetry_lock = threading.Lock()
        self._telemetry_thread = None
        self._running = False

        self.connected = False
        self.has_ever_connected = False
        self.last_error = ""
        self.last_http_error = ""  # <-- NEW: Keep track of HTTP issues separately
        self.last_seen = 0.0
        self.retrying = False

    def startTelemetryStream(self):
        """If not already running, create a thread to receive incomming telemetry async.
           This opens and maintains the socket connection.
        """
        if self._running:
            return
        #a flag to allow the thread to be broken from the main
        self._running = True
        self._telemetry_thread = threading.Thread(target=self._telemetry_receiver, daemon=True)
        self._telemetry_thread.start()

    def stopTelemetryStream(self):
        """Break the connection reciver loop by settin the thread break flag.
        """
        #TODO does this clean up gracefully and instantly?
        self._running = False
        if self._telemetry_thread:
            self._telemetry_thread.join(timeout=2)

    def _set_disconnected(self, error_message):
        """Forces a clean reset of connection states in the main thread."""
        self.connected = False
        self.retrying = True
        self.last_error = error_message
        # Do NOT reset self.last_seen here, let the UI know when we last had data

    def _telemetry_receiver(self):
        while self._running:
            sock = None
            buffer = ""
            try:
                # Setup for a fresh connection attempt
                self.retrying = True
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connect_timeout)

                # Try opening a fresh socket connection
                sock.connect((self.IP_RC, self.telemetryPort))

                # Connection successful! Update states
                # The connection is successful because no thrown exceptions from sock.connect
                sock.settimeout(4.0)  # Give up to 2 seconds between data frames
                self.connected = True
                self.has_ever_connected = True
                self.retrying = False
                self.last_error = ""
                print(f"[FLEET] Connected IP={self.IP_RC} telemetry={self.telemetryPort}")

                # Inner data ingestion loop
                while self._running:
                    try:
                        data = sock.recv(4096)
                        if not data:
                            raise ConnectionError("Drone closed the telemetry socket stream.")

                        buffer += data.decode("utf-8", errors="ignore")
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                telemetry = json.loads(line)
                            except json.JSONDecodeError:
                                continue

                            with self._telemetry_lock:
                                self._telemetry = telemetry
                                self._telemetry["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")
                                self.last_seen = time.time()
                                self.connected = True
                                self.retrying = False

                    except socket.timeout:
                        # If we haven't seen a message in 2 seconds, the link is dead
                        raise TimeoutError("No telemetry data received within timeout threshold.")

            except Exception as exc:
                # There was an issue with either the initial connection, or anything else with the socket.
                # Active failure state: immediately clean up everything
                self._set_disconnected(str(exc))
                if self.has_ever_connected:
                    print(f"[FLEET] Telemetry lost IP={self.IP_RC}: {exc}. Retrying in {self.retry_interval}s...")

                #If the socket was opened, but the error was elsewhere, then clean up.
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                        sock.close()
                    except Exception:
                        pass

                # Back off before attempting another fresh connection sequence
                time.sleep(self.retry_interval)

            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

    def getTelemetry(self):
        with self._telemetry_lock:
            return self._telemetry.copy()

    def requestSend(self, endPoint, data, verbose=False):
        if not self.IP_RC:
            return ""
        try:
            response = requests.post(self.baseCommandUrl + endPoint, str(data), timeout=3)
            self.last_http_error = ""  # Clear HTTP error on success
            if verbose:
                print("EP : " + endPoint + "\t" + str(response.content, encoding="utf-8"))
            return response.content.decode("utf-8")
        except requests.exceptions.RequestException as exc:
            # FIX: Record the error, but DO NOT drop the entire telemetry stream connection status!
            self.last_http_error = str(exc)
            print(f"[HTTP ERROR] IP={self.IP_RC} command to {endPoint} failed: {exc}")
            return ""


    def requestSendTakeOff(self):
        return self.requestSend(EP_TAKEOFF, "")

    def requestSendLand(self):
        return self.requestSend(EP_LAND, "")

    def requestSendRTH(self):
        self.requestAbortMission()
        return self.requestSend(EP_RTH, "")

    def requestSendEnableVirtualStick(self):
        return self.requestSend(EP_ENABLE_VIRTUAL_STICK, "")

    def requestSendGoToWP(self, latitude, longitude, altitude):
        return self.requestSend(EP_GOTO_WP, f"{latitude},{longitude},{altitude}")

    def requestSendGoToWPwithPID(self, latitude, longitude, altitude, yaw, speed: float = 5.0):
        return self.requestSend(EP_GOTO_WP_PID, f"{latitude},{longitude},{altitude},{yaw},{speed}")

    def requestSendGotoAltitude(self, altitude):
        return self.requestSend(EP_GOTO_ALTITUDE, f"{altitude}")

    def requestAbortMission(self):
        return self.requestSend(EP_ABORT_MISSION, "")

    def requestSendHold(self):
        return self.requestAbortMission()

    def requestSendGotoYaw(self, yaw):
        self.requestSendEnableVirtualStick()
        return self.requestSend(EP_GOTO_YAW, f"{yaw}")

    def getBatteryLevel(self):
        return self.getTelemetry().get("batteryLevel", -1)

    def getLocation(self):
        return self.getTelemetry().get("location", {})

    def getHeading(self):
        return self.getTelemetry().get("heading", 0.0)

    def getFlightMode(self):
        return self.getTelemetry().get("flightMode", "UNKNOWN")

    def isManualOverrideActive(self):
        return self.getTelemetry().get("isManualOverrideActive", False)

    def get_snapshot(self):
        telemetry = self.getTelemetry()
        location = telemetry.get("location", {})
        return {
            "id": self.drone_id,
            "ip": self.IP_RC,
            "connected": self.connected,
            "retrying": self.retrying,
            "last_error": self.last_error,
            "last_seen": self.last_seen,
            "battery": self.getBatteryLevel(),
            "heading": self.getHeading(),
            "location": location,
            "flight_mode": self.getFlightMode(),
            "manual_override": self.isManualOverrideActive(),
            "telemetry": telemetry,
        }

# Must match SAFETY_TOKEN hardcoded in the Android app (WildBridgeDefaultLayoutActivity).
SAFETY_TOKEN = "98"
SAFETY_TOKEN_HEADER = "X-Safety-Token"
EP_RELEASE_SAFETY_CONTROL = "/releaseSafetyControl"


class DJIInterfaceSafety(DJIInterface):
    """Safety Computer interface: a DJIInterface that always sends the Safety token."""

    def __init__(self, IP_RC="", safety_token=SAFETY_TOKEN):
        super().__init__(IP_RC)
        self.safety_token = safety_token

    def setSafetyToken(self, token):
        self.safety_token = token

    def _authHeaders(self):
        if self.safety_token:
            return {SAFETY_TOKEN_HEADER: str(self.safety_token)}
        return {}

    def requestSend(self, endPoint, data, verbose=False):
        if self.IP_RC == "":
            print(f"No IP_RC provided, returning empty string for request at {endPoint}")
            return ""
        try:
            response = requests.post(
                self.baseCommandUrl + endPoint, str(data),
                headers=self._authHeaders(), timeout=5)
            if verbose:
                print("EP : " + endPoint + "\t" + str(response.content, encoding="utf-8"))
            return response.content.decode('utf-8')
        except requests.exceptions.RequestException as e:
            print(f"Request error at {endPoint}: {e}")
            return ""

    def requestReleaseSafetyControl(self):
        return self.requestSend(EP_RELEASE_SAFETY_CONTROL, "")


class DroneFleetManager:
    """Loads drone IPs from a file, spawns one manager per drone, and aggregates state for the dashboard."""

    def __init__(self, drone_list_path=None, telemetry_port=8081, retry_interval=15.0, connect_timeout=5.0):
        self.drone_list_path = drone_list_path or os.getenv("WILDBRIDGE_DRONE_LIST", os.path.join("assets", "drone_ips.txt"))
        self.telemetry_port = telemetry_port
        self.retry_interval = retry_interval
        self.connect_timeout = connect_timeout
        self.lock = threading.Lock()

        self.managers = {}
        self.open_drones = []
        self.retry_drones = []
        self.agents = {}
        self.gps_data = {}
        self.batt_pct = {}
        self.heading = {}
        self.armed_state = {}
        self.airborne_state = {}
        self.rtl_active = {}
        self.flight_mode_str = {}
        self.connection_state = {}
        self.ip_by_id = {}
        self.name_by_id = {}
        self._prev_connection_state = {}
        self.last_error_by_id = {}
        self.last_seen_by_id = {}
        self.last_error = {}
        self.last_seen = {}
        self.retrying = {}
        self.targets = {}
        self.rtltargets = {}

        self._running = True
        self._load_and_spawn()
        self._sync_snapshot()
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()

    def _load_and_spawn(self):
        entries = load_drone_ip_list(self.drone_list_path)
        if not entries:
            print(f"[FLEET] No drones loaded from {self.drone_list_path}")

        for entry in entries:
            drone_id = int(entry["id"])
            ip_address = entry["ip"]

            if drone_id in self.managers:
                print(f"[FLEET] Drone ID={drone_id} is already loaded. Skipping re-initialization.")
                continue

            manager = DJIInterfaceSafety(
                IP_RC=ip_address,
                safety_token=SAFETY_TOKEN
            )
            manager.drone_id = drone_id
            manager.telemetryPort = self.telemetry_port
            manager.retry_interval = self.retry_interval
            manager.connect_timeout = self.connect_timeout
            manager.baseCommandUrl = f"http://{ip_address}:{DRONE_COMMAND_PORT}"

            self.managers[drone_id] = manager
            self.ip_by_id[drone_id] = ip_address
            self.name_by_id[drone_id] = entry.get("name", ip_address.split(".")[-1].zfill(3))

            manager.startTelemetryStream()

            print(f"[FLEET] Spawned manager for Drone ID={drone_id} IP={ip_address}")

    def stop(self):
        self._running = False
        for drone_id in list(self.managers.keys()):
            try:
                self.managers[drone_id].stopTelemetryStream()
            except Exception as e:
                print(f"[FLEET] Error stopping manager thread for Drone {drone_id}: {e}")


    def _sync_snapshot(self):
        with self.lock:
            self.open_drones = []
            self.retry_drones = []
            for drone_id, manager in self.managers.items():
                snapshot = manager.get_snapshot()

                self.connection_state[drone_id] = bool(snapshot["connected"])
                self.last_error_by_id[drone_id] = snapshot["last_error"]
                self.last_seen_by_id[drone_id] = snapshot["last_seen"]
                self.last_error[drone_id] = snapshot["last_error"]
                self.last_seen[drone_id] = snapshot["last_seen"]
                self.retrying[drone_id] = bool(snapshot["retrying"])
                self.agents[drone_id] = snapshot["last_seen"]

                telemetry = snapshot.get("telemetry", {})
                location = telemetry.get("location", {})

                lat = float(location.get("latitude", 0.0))
                lon = float(location.get("longitude", 0.0))
                alt = float(location.get("altitude", 0.0))

                heading = float(telemetry.get("heading", 0.0))
                battery = telemetry.get("batteryLevel", snapshot.get("battery", -1))
                flight_mode = telemetry.get("flightMode", snapshot.get("flight_mode", "UNKNOWN"))
                manual_override = telemetry.get("isManualOverrideActive", False)

                self.gps_data[drone_id] = (lat, lon, alt)
                self.batt_pct[drone_id] = battery
                self.heading[drone_id] = heading
                self.armed_state[drone_id] = not manual_override
                self.airborne_state[drone_id] = alt > 0.5
                self.rtl_active[drone_id] = False
                self.flight_mode_str[drone_id] = flight_mode

                connected = bool(snapshot["connected"])
                prev = self._prev_connection_state.get(drone_id)
                if prev is None:
                    # first snapshot — log initial state if already online
                    if connected:
                        ulog.log_event(drone_id, 'CONNECT')
                elif connected and not prev:
                    ulog.log_event(drone_id, 'CONNECT')
                elif not connected and prev:
                    ulog.log_event(drone_id, 'DISCONNECT')
                self._prev_connection_state[drone_id] = connected

                if connected:
                    ulog.log_telemetry(
                        drone_id=drone_id, lat=lat, lon=lon, alt=alt,
                        heading=heading,
                        batt_pct=float(battery) if isinstance(battery, (int, float)) else 0.0,
                        armed=not manual_override,
                        airborne=alt > 0.5,
                    )

                if snapshot["connected"]:
                    self.open_drones.append(drone_id)
                else:
                    self.retry_drones.append(drone_id)

    def _sync_loop(self):
        summary_tick = 0
        while self._running:
            self._sync_snapshot()
            summary_tick += 1
            if summary_tick >=30:
                summary_tick = 0
                with self.lock:
                    waiting_ips = [
                        self.ip_by_id[did] for did in self.retry_drones
                        if not self.managers[did].has_ever_connected
                    ]
                if waiting_ips:
                    print(f"[FLEET] {len(waiting_ips)} drone(s) waiting to connect: {', '.join(waiting_ips)}")
            time.sleep(1.0)

    def calculate_bearing(self, lat1, lon1, lat2, lon2):
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lon = math.radians(lon2 - lon1)

        x = math.sin(delta_lon) * math.cos(lat2_rad)
        y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)

        bearing_rad = math.atan2(x, y)
        bearing_deg = math.degrees(bearing_rad)
        return (bearing_deg + 360) % 360

    def _manager(self, drone_id):
        return self.managers.get(drone_id)

    def _safetymanager(self, drone_id):            #reroute to prevent needing to change lots of safetymanager->manager later
        return self.managers.get(drone_id)


# -----------------------------
# Shared styles & Fleet Init
# -----------------------------
ulog = ULogWriter('logs/' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.log')
receiver = DroneFleetManager(drone_list_path=DRONE_LIST_FILE)

with open("assets/defaults.json", "r") as f:
    defaults_data = json.load(f)

default_values = [
    defaults_data.get("lat", 0),
    defaults_data.get("lon", 0),
    defaults_data.get("alt", 0),
    defaults_data.get("radius", 0),
    defaults_data.get("offset", 0),
    defaults_data.get("tgtalt", 0),
    defaults_data.get("rtlalt", 0)
]


# -----------------------------
# Utility layout builders (Declared BEFORE layout to fix NameError)
# -----------------------------
def ip_label(ip):
    if not ip:
        return "---"
    return str(ip).strip().split('.')[-1].zfill(3)

def calculate_centroid(latitudes, longitudes):
    return (sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes))

def calculate_zoom_from_bounds(latitudes, longitudes):
    lat_span = max(latitudes) - min(latitudes)
    lon_span = max(longitudes) - min(longitudes)
    max_span = max(lat_span, lon_span)
    if max_span <= 0: return 20
    zoom_lookup = [(0.0002, 21), (0.0005, 20), (0.0010, 19), (0.0020, 18), (0.0050, 17), (0.0100, 16), (0.0200, 15), (0.0500, 14)]
    for span_limit, zoom in zoom_lookup:
        if max_span <= span_limit: return zoom
    return 8

def vertical_divider(extra_class=''):
    return html.Div(className=('agent-div ' + extra_class).strip())

def status_square(is_on, label):
    return html.Div(title=label, className='status-dot ' + ('dot-on' if is_on else 'dot-off'))

def status_indicator(is_on, label):
    return html.Div([status_square(is_on, label), html.Span(label, className='status-label')], className='status-indicator')

def create_agent_bar(agent_id, ip, name, online, retrying, last_seen, gps, batt, mode, last_error):
    status_text = 'ONLINE' if online else 'OFFLINE'
    age_text = 'fresh' if not last_seen else f"{time.time() - last_seen:.1f}s ago"

    return html.Div(
        id={'type': 'agent-row-style', 'index': agent_id},
        className='drone-row offline',
        children=[
            html.Button(
                "⏻",
                id={'type': 'drone-activate-btn', 'index': agent_id},
                n_clicks=0,
                className='drone-activate-btn drone-inactive',
                type="button"
            ),
            html.Div([
                html.Div(f"Drone {name}", className='drone-label'),
                html.Div(f"IP: {ip}", className='drone-meta'),
                html.Div(f"State: {status_text}", id={'type': 'agent-state-txt', 'index': agent_id}, className='drone-meta'),
                html.Div(f"Last seen: {age_text}", id={'type': 'agent-age-txt', 'index': agent_id}, className='drone-meta'),
                html.Div(f"Mode: {mode}", id={'type': 'agent-mode-txt', 'index': agent_id}, className='drone-meta')
            ], className='agent-col agent-col-identity'),
            vertical_divider('agent-div-before-status'),
            html.Div([
                html.Div([
                    html.Div(id={'type': 'agent-airborne-dot', 'index': agent_id}, title="Airborne",
                             className='status-dot grounded'),
                    html.Span("Airborne", className='status-label')
                ], className='status-indicator'),
                status_indicator(not retrying, "Retry OK"),
                status_indicator(last_error == "", "Healthy"),
                html.Div(last_error if last_error else "", className='drone-error')
            ], className='agent-col agent-col-status'),
            vertical_divider('agent-div-before-telemetry'),
            html.Div([
                html.Div(f"Batt: {batt:.1f}%" if isinstance(batt, (int, float)) else f"Batt: {batt}", id={'type': 'agent-batt-txt', 'index': agent_id}, className='drone-telem'),
                html.Div(f"Lat: {gps[0]:.6f}", id={'type': 'agent-lat-txt', 'index': agent_id}, className='drone-telem'),
                html.Div(f"Lon: {gps[1]:.6f}", id={'type': 'agent-lon-txt', 'index': agent_id}, className='drone-telem'),
                html.Div(f"Alt: {gps[2]:.1f}", id={'type': 'agent-alt-txt', 'index': agent_id}, className='drone-telem')
            ], className='agent-col agent-col-telemetry'),
            vertical_divider('agent-div-before-buttons'),
            html.Div(
                html.Div([
                    html.Button("Takeoff",    id={'type':'agent-btn','index':f'{agent_id}-takeoff'},  type="button", n_clicks=0, className='drone-btn'),
                    html.Button("Hold",       id={'type':'agent-btn','index':f'{agent_id}-hold'},     type="button", n_clicks=0, className='drone-btn'),
                    html.Button("Goto",       id={'type':'agent-btn','index':f'{agent_id}-goto'},     type="button", n_clicks=0, className='drone-btn drone-btn-goto'),
                    html.Button("Land",       id={'type':'agent-btn','index':f'{agent_id}-land'},     type="button", n_clicks=0, className='drone-btn'),
                    html.Button("RTL",        id={'type':'agent-btn','index':f'{agent_id}-rtl'},      type="button", n_clicks=0, className='drone-btn'),
                    html.Button("Return CTL", id={'type':'agent-btn','index':f'{agent_id}-release'},  type="button", n_clicks=0, className='drone-btn drone-btn-release'),
                ], className='agent-btn-grid'),
                className='agent-col agent-col-buttons',
            ),
            vertical_divider('agent-div-trailing')
        ],
    )


# -----------------------------
# Dash App Framework Initialisation
# -----------------------------
app = dash.Dash(__name__, update_title=None)
app.index_string = '''<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>'''

server = app.server
@server.route("/tiles/<path:path>")
def serve_tiles(path):
    return send_from_directory("assets/tiles", path)


app.layout = html.Div([
    html.Div([
        html.H1("FlightBoard Dashboard", className='dashboard-title'),
        html.Span("0.00 MB", id='log-size-counter', className='log-size-counter'),
    ], className='title-bar'),
    html.Div(id="button-output"),
    html.Div([
        html.Button("ACTIVATE ALL", id="activate-all-btn", n_clicks=0, className='btn-global btn-activate-all'),
        html.Button("DEACTIVATE ALL", id="deactivate-all-btn", n_clicks=0, className='btn-global btn-deactivate-all'),
        html.Div(className='global-actions-divider'),
        html.Button("TAKEOFF ALL", id="takeoff-all", n_clicks=0, className='btn-global'),
        html.Button("HOLD ALL", id="hold-all", n_clicks=0, className='btn-global'),
        html.Button("LAND ALL", id="land-all", n_clicks=0, className='btn-global'),
        html.Button("RTL ALL", id="rtl-all", n_clicks=0, className='btn-global'),
        dcc.ConfirmDialog(id='confirm-land-all', message='Are you sure you want to LAND ALL drones?'),
        dcc.ConfirmDialog(id='confirm-rtl-all', message='Are you sure you want to RTL ALL drones?'),
    ], className='global-actions'),

    html.Div([
        html.Div(children=[
            dl.Map(
                id='agent-map',
                center=[64.575257, -149.419418],
                zoom=17,
                minZoom=1,
                maxZoom=22,
                doubleClickZoom=False,
                children=[
                    dl.TileLayer(url="/tiles/FairbanksTestSite/{z}/{x}/{y}.jpg", tms=False, noWrap=True, minZoom=1, maxZoom=22),
                    dl.LayerGroup(id='layer-rtl-targets'),
                    dl.LayerGroup(id='layer-targets'),
                    dl.LayerGroup(id='map-markers'),
                    dl.LayerGroup(id='layer-custom'),
                ],
            ),
            html.Div(children=[
                html.Button("Center Map", id="map-center", n_clicks=0, className='btn-global'),
                html.Div([
                    html.Div("Goto Height (m)", className='goto-height-label'),
                    dcc.Input(id='goto-height', type='number', placeholder='50', value=defaults_data.get("goto_height", 50))
                ], className='goto-height-wrap')
            ], className='map-overlay')
        ], id='map-container'),

        html.Div([
            html.Button("⯈", id="toggle-agents", className="toggle-btn"),
            html.Div(
                id='agent-container',
                children=[
                    html.Div(
                        create_agent_bar(
                            agent_id=entry["id"],
                            ip=entry["ip"],
                            name=entry["name"],
                            online=False,
                            retrying=False,
                            last_seen=0.0,
                            gps=(0,0,0),
                            batt=0,
                            mode="OFFLINE",
                            last_error=""
                        ),
                        id=f"agent-row-wrapper-{entry['id']}"
                    ) for entry in load_drone_ip_list(DRONE_LIST_FILE)
                ],
            )
        ], id='agents-bar', className='collapsed')
    ], className='content-row'),

    dcc.Interval(id='interval-agents', interval=500, n_intervals=0),
    dcc.Interval(id='interval-map', interval=250, n_intervals=0),
    dcc.Store(id='viewport-width', storage_type='session'),
    dcc.Store(id="resize-trigger"),
    dcc.Store(id='activated-drones', data=[]),

    html.Div(id='debug-output', style={'display':'none'}),
    html.Div(id='map-click-left-output', style={'display': 'none'}),
    html.Div(id='map-click-right-output', style={'display': 'none'}),
    html.Div(id='map-click-dleft-output', style={'display': 'none'}),
], className='main-layout')


# -----------------------------
# Callbacks
# -----------------------------
@app.callback(
    Output('log-size-counter', 'children'),
    Input('interval-agents', 'n_intervals'),
)
def update_log_size(_n):
    return f"{ulog.size_mb():.2f} MB"


@app.callback(
    Output('activated-drones', 'data'),
    Input({'type': 'drone-activate-btn', 'index': ALL}, 'n_clicks'),
    Input('activate-all-btn', 'n_clicks'),
    Input('deactivate-all-btn', 'n_clicks'),
    State({'type': 'drone-activate-btn', 'index': ALL}, 'id'),
    State('activated-drones', 'data'),
    prevent_initial_call=True
)
def update_activated_drones(_toggle_clicks, _n_activate_all, _n_deactivate_all, btn_ids, current_activated):
    triggered_id = ctx.triggered_id
    all_ids = [b['index'] for b in btn_ids]

    if triggered_id == 'activate-all-btn':
        return all_ids
    if triggered_id == 'deactivate-all-btn':
        return []

    # Individual toggle
    drone_id = triggered_id['index']
    activated = set(current_activated or [])
    if drone_id in activated:
        activated.discard(drone_id)
    else:
        activated.add(drone_id)
    return list(activated)


@app.callback(
    Output({'type': 'drone-activate-btn', 'index': ALL}, 'className'),
    Output({'type': 'drone-activate-btn', 'index': ALL}, 'children'),
    Input('activated-drones', 'data'),
    State({'type': 'drone-activate-btn', 'index': ALL}, 'id'),
)
def update_activate_button_appearance(activated, btn_ids):
    activated_set = set(activated or [])
    classnames = []
    for b in btn_ids:
        if b['index'] in activated_set:
            classnames.append('drone-activate-btn drone-active')
        else:
            classnames.append('drone-activate-btn drone-inactive')
    return classnames, ['⏻'] * len(btn_ids)


@app.callback(
    Output('debug-output', 'children'),
    Input({'type': 'agent-btn', 'index': ALL}, 'n_clicks_timestamp'),
    State({'type': 'agent-btn', 'index': ALL}, 'id'),
    State('goto-height', 'value'),
    State('activated-drones', 'data'),
    prevent_initial_call=True
)
def debug_buttons(timestamps, ids, dynamic_height, activated_drones):
    valid = [(ts, i) for i, ts in enumerate(timestamps) if ts is not None]
    if not valid:
        return dash.no_update

    max_ts, max_idx = max(valid, key=lambda x: x[0])
    btn_id = ids[max_idx]
    drone_id, action = btn_id['index'].split('-')

    if drone_id.isdigit():
        drone_id_int = int(drone_id)
        if drone_id_int not in (activated_drones or []):
            return dash.no_update
        action = action.lower()

        if action == 'goto' and dynamic_height is not None:
            with receiver.lock:
                if drone_id_int in receiver.targets:
                    receiver.targets[drone_id_int]['alt'] = float(dynamic_height)

        send_command(drone_id_int, action)

    return dash.no_update


@app.callback(
    Output('agent-map', 'viewport'),
    Input('map-center', 'n_clicks'),
    prevent_initial_call=True
)
def center_map_on_agents(n_clicks):
    if not n_clicks:
        return dash.no_update

    with receiver.lock:
        active_drones = list(receiver.open_drones)
        if not active_drones:
            return dash.no_update

        latitudes, longitudes = [], []
        for drone_id in active_drones:
            gps = receiver.gps_data.get(drone_id)
            if not gps:
                continue
            lat, lon, _ = gps
            if lat == 0 or lon == 0:
                continue
            latitudes.append(float(lat))
            longitudes.append(float(lon))

        if not latitudes:
            return dash.no_update

        center_lat, center_lon = calculate_centroid(latitudes, longitudes)
        zoom = calculate_zoom_from_bounds(latitudes, longitudes)

    print(f"[MAP] Instantly centering viewport on {len(latitudes)} drones -> Center: ({center_lat:.6f}, {center_lon:.6f}) Zoom: {zoom}")

    return {
        "center": [center_lat, center_lon],
        "zoom": zoom-1,
        "transition": "flyTo"
    }

@app.callback(
    Output({'type': 'agent-row-style', 'index': ALL}, 'className'),
    Output({'type': 'agent-state-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-age-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-mode-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-batt-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-lat-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-lon-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-alt-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-airborne-dot', 'index': ALL}, 'className'),
    Input('interval-agents', 'n_intervals'),
    State({'type': 'agent-state-txt', 'index': ALL}, 'id'),
    State('activated-drones', 'data'),
)
def update_dashboard(n, dynamic_ids, activated_drones):
    activated_set = set(activated_drones or [])
    row_classes = []
    states, ages, modes, batts, lats, lons, alts, airborne_dots = [], [], [], [], [], [], [], []

    with receiver.lock:
        for idx_dict in dynamic_ids:
            drone_id = idx_dict['index']

            online = receiver.connection_state.get(drone_id, False)
            last_seen = receiver.last_seen.get(drone_id, 0.0)
            gps = receiver.gps_data.get(drone_id, (0.0, 0.0, 0.0))
            batt = receiver.batt_pct.get(drone_id, 0)
            mode = receiver.flight_mode_str.get(drone_id, "UNKNOWN")

            status_text = 'ONLINE' if online else 'OFFLINE'
            age_text = 'fresh' if not last_seen else f"{time.time() - last_seen:.1f}s ago"

            stale = online and (last_seen == 0 or time.time() - last_seen > 20)
            if not online:
                state_class = 'offline'
            elif stale:
                state_class = 'online stale'
            elif batt < 30:
                state_class = 'online battery-critical'
            elif batt < 50:
                state_class = 'online battery-low'
            else:
                state_class = 'online battery-ok'

            active_class = ' activated' if drone_id in activated_set else ' deactivated'
            row_classes.append(f'drone-row {state_class}{active_class}')

            states.append(f"State: {status_text}")
            ages.append(f"Last seen: {age_text}")
            modes.append(f"Mode: {mode}")
            batts.append(f"Batt: {batt:.1f}%" if isinstance(batt, (int, float)) else f"Batt: {batt}")
            lats.append(f"Lat: {gps[0]:.6f}")
            lons.append(f"Lon: {gps[1]:.6f}")
            alts.append(f"Alt: {gps[2]:.1f}")

            airborne = gps[2] > 2.0
            airborne_dots.append('status-dot airborne' if airborne else 'status-dot grounded')

    return row_classes, states, ages, modes, batts, lats, lons, alts, airborne_dots


@app.callback(
    Output("agents-bar", "className"),
    Output("toggle-agents", "children"),
    Input("toggle-agents", "n_clicks")
)
def toggle_agents_bar(n):
    if n and n % 2 == 1: return "expanded", "⯇"
    return "collapsed", "⯈"


@app.callback(
    Output('map-markers', 'children'),
    Input('interval-map', 'n_intervals')
)
def update_map_markers(n):
    markers = []
    with receiver.lock:
        for drone_id in receiver.agents.keys():
            lat, lon, alt = receiver.gps_data.get(drone_id, (0, 0, 0))
            if lat == 0 and lon == 0:
                continue
            name = receiver.name_by_id.get(drone_id, str(drone_id))
            heading = receiver.heading.get(drone_id, 0.0)
            markers.extend(_drone_map_markers(name, lat, lon, alt, heading))
    return markers


@app.callback(Output('layer-targets', 'children'), Input('interval-map', 'n_intervals'))
def update_target_markers(n): return []

@app.callback(
    Output('layer-rtl-targets', 'children'),
    Input('interval-map', 'n_intervals')
)
def update_rtl_target_markers(n):
    markers = []
    with receiver.lock:
        for drone_id, target in receiver.rtltargets.items():
            if not target: continue
            lat, lon, alt = target.get('lat', 0), target.get('lon', 0), target.get('alt', 0)
            if lat == 0 and lon == 0: continue
            markers.append(dl.Marker(position=[lat, lon], icon={"iconUrl": "/assets/cross.png", "iconSize": [40, 40], "iconAnchor": [20, 20]}, children=dl.Tooltip(f"RTL Drone {drone_id}: {lat:.6f}, {lon:.6f}, alt {alt:.1f}")))
    return markers


def _drone_map_markers(name, lat, lon, alt, heading):
    """Return [arrow_marker, label_marker] for a single drone using DivIcon."""
    arrow_html = (
        f'<div style="width:48px;height:48px;transform:rotate({heading:.0f}deg)">'
        f'<svg viewBox="0 0 48 48" width="48" height="48" xmlns="http://www.w3.org/2000/svg">'
        f'<polygon points="24,3 41,45 24,35 7,45" fill="#1E88E5" stroke="#0D47A1" stroke-width="2"/>'
        f'</svg></div>'
    )
    label_html = f'<div class="drone-map-label">{name}</div>'
    tooltip = dl.Tooltip(f"Drone {name}: Alt: {alt:.1f}m, Hdg: {heading:.1f}°")
    return [
        dl.Marker(
            position=[lat, lon],
            icon=dl.DivIcon(html=arrow_html, iconSize=[48, 48], iconAnchor=[24, 24], className=''),
            zIndexOffset=999,
        ),
        dl.Marker(
            position=[lat, lon],
            icon=dl.DivIcon(html=label_html, iconSize=[32, 32], iconAnchor=[16, 16], className=''),
            children=tooltip,
            zIndexOffset=1000,
        ),
    ]

def generate_circular_targets(center_lat, center_lon, alt, radius_m, offset_deg, tgt_alt, num_targets):
    targets = []
    meters_per_deg_lat = 111320
    meters_per_deg_lon = 40075000 * math.cos(math.radians(center_lat)) / 360
    for i in range(num_targets):
        angle = 2 * math.pi * i / num_targets + math.radians(offset_deg)
        dlat = (radius_m * math.cos(angle)) / meters_per_deg_lat
        dlon = (radius_m * math.sin(angle)) / meters_per_deg_lon
        target_lat, target_lon = center_lat + dlat, center_lon + dlon
        delta_north = (center_lat - target_lat) * meters_per_deg_lat
        delta_east  = (center_lon - target_lon) * meters_per_deg_lon
        heading_deg = (math.degrees(math.atan2(delta_east, delta_north)) + 360) % 360
        gpitch = math.degrees(math.atan2(tgt_alt - alt, math.sqrt(delta_north**2 + delta_east**2)))
        targets.append({"lat": round(target_lat, 6), "lon": round(target_lon, 6), "alt": alt, "head": round(heading_deg, 1), "gpitch": round(gpitch, 1)})
    return targets


@app.callback(
    Output('map-click-dleft-output', 'children'),
    Output('layer-custom', 'children'),
    Input('agent-map', 'n_dblclicks'),
    State('agent-map', 'dblclickData'),
    State('goto-height', 'value'),
    prevent_initial_call=True
)
def add_custom_marker(n_dblclicks, dblclickData, target_alt):
    if n_dblclicks is None or dblclickData is None or 'latlng' not in dblclickData:
        return dash.no_update, dash.no_update
    lat, lon = round(dblclickData['latlng']['lat'], 6), round(dblclickData['latlng']['lng'], 6)
    target_alt = float(target_alt) if target_alt is not None else 10.0
    if lat == 0.0 or lon == 0.0: return "Invalid map boundary selection.", dash.no_update

    print(f"[STAGED CENTER] Staging shared target hub at Lat: {lat:.6f}, Lon: {lon:.6f}")
    with receiver.lock:
        drone_ids = sorted(receiver.agents.keys())
        targets = generate_circular_targets(center_lat=lat, center_lon=lon, alt=target_alt, radius_m=8.0, offset_deg=0, tgt_alt=target_alt, num_targets=max(len(drone_ids), 1))
        receiver.targets = {drone_id: target for drone_id, target in zip(drone_ids, targets)}

    single_center_marker = dl.Marker(position=[lat, lon], icon={"iconUrl": "/assets/cross.png", "iconSize": [48, 48], "iconAnchor": [24, 24]}, children=dl.Tooltip(f"Staged Swarm Center: {lat:.6f}, {lon:.6f} ({target_alt}m)"))
    return f"Target center set at lat: {lat:.6f}, lon: {lon:.6f}", [single_center_marker]


@app.callback(Output('confirm-land-all', 'displayed'), Input('land-all', 'n_clicks'), prevent_initial_call=True)
def show_land_all_confirm(n): return True

@app.callback(Output('confirm-rtl-all', 'displayed'), Input('rtl-all', 'n_clicks'), prevent_initial_call=True)
def show_rtl_all_confirm(n): return True


@app.callback(
    Output('button-output', 'children'),
    Input('takeoff-all', 'n_clicks'),
    Input('hold-all', 'n_clicks'),
    Input('confirm-land-all', 'submit_n_clicks'),
    Input('confirm-rtl-all', 'submit_n_clicks'),
    State('activated-drones', 'data'),
    prevent_initial_call=True
)
def handle_all_buttons(_n_takeoff, _n_hold, n_land_confirm, n_rtl_confirm, activated_drones):
    triggered_id = ctx.triggered_id
    if triggered_id is None: return dash.no_update

    if triggered_id == "takeoff-all": action = "TAKEOFF"
    elif triggered_id == "hold-all": action = "HOLD"
    elif triggered_id == "confirm-land-all" and n_land_confirm: action = "LAND"
    elif triggered_id == "confirm-rtl-all" and n_rtl_confirm: action = "RTL"
    else: return dash.no_update

    targets = activated_drones or []
    if not targets:
        return "No drones activated — nothing sent."

    print(f"[COMMAND] {action} ALL -> {len(targets)} activated drone(s): {targets}")
    send_command_activated(action, targets)
    return f"{action} sent to {len(targets)} activated drone(s)."


def send_command(drone_id: int, action: str):
    """Send a swarm command to a single drone authenticated via Safety Computer Interface."""
    action = action.lower()
    ulog.log_command(drone_id, action)

    manager = receiver._safetymanager(drone_id)
    if not manager:
        print(f"[ERROR] No active authenticated safety manager found for Drone {drone_id}")
        return

    if action == 'takeoff':
        print(f"[SAFETY COMMAND] Sending Authenticated TAKEOFF to IP {manager.IP_RC}")
        manager.requestSendTakeOff()

    elif action == 'hold':
        print(f"[SAFETY COMMAND] Sending Authenticated HOLD/ABORT to IP {manager.IP_RC}")
        manager.requestAbortMission()

    elif action == 'goto':
        target = receiver.targets.get(drone_id)
        if not target:
            print(f"[WARNING] No staged goto target coordinates found for Drone {drone_id}")
            return

        lat = target.get('lat', 0)
        lon = target.get('lon', 0)
        alt = target.get('alt', 0)
        yaw = target.get('head', target.get('yaw', 0.0))

        print(f"[SAFETY COMMAND] Sending Authenticated GOTO to IP {manager.IP_RC} -> Lat: {lat}, Lon: {lon}")
        ulog.log_command(drone_id, 'goto_target', f"lat={lat} lon={lon} alt={alt} yaw={yaw}")
        manager.requestSendGoToWPwithPID(lat, lon, alt, yaw)

    elif action == 'land':
        print(f"[SAFETY COMMAND] Sending Authenticated LAND to IP {manager.IP_RC}")
        manager.requestSendLand()

    elif action == 'rtl':
        print(f"[SAFETY COMMAND] Sending Authenticated RTL to IP {manager.IP_RC}")
        manager.requestSendRTH()

    elif action == 'release':
        print(f"[SAFETY COMMAND] Releasing Safety Computer Control back to Pilot for IP {manager.IP_RC}")
        manager.requestReleaseSafetyControl()

    else:
        print(f"[WARNING] Unknown action token invocation for Drone {drone_id}: {action}")


def send_command_activated(action: str, activated_ids: list):
    for drone_id in activated_ids:
        time.sleep(0.1)
        send_command(int(drone_id), action)

# -----------------------------
# Run server
# -----------------------------
if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=8050)
