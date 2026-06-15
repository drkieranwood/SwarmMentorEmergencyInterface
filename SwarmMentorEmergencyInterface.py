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
# ASCII flight log recorder
# -----------------------------
class ULogWriter:
    """Write drone telemetry and commands to a plain-text log file.

    Each line: ISO-timestamp TYPE key=value ...
    Types: TELEM, CMD
    """

    def __init__(self, filepath):
        self._f = open(filepath, 'w', encoding='utf-8', buffering=1)  # line-buffered
        self._lock = threading.Lock()
        self._f.write(f"# FlightBoard log started {datetime.now().isoformat()}\n")
        self._f.write("# TELEM: timestamp sysid lat lon alt batt heading armed airborne\n")
        self._f.write("# EVENT: timestamp sysid event=CONNECT|DISCONNECT\n")
        self._f.write("# CMD:   timestamp sysid action [detail...]\n")
        print(f"[LOG] Logging to {filepath}")

    def _ts(self):
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

    def log_telemetry(self, sysid, lat, lon, alt, batt_pct, heading, armed, airborne):
        line = (
            f"{self._ts()} TELEM"
            f" sysid={sysid}"
            f" lat={lat:.7f} lon={lon:.7f} alt={alt:.2f}"
            f" batt={batt_pct:.1f}"
            f" hdg={heading:.1f}"
            f" armed={int(bool(armed))}"
            f" airborne={int(bool(airborne))}\n"
        )
        with self._lock:
            self._f.write(line)

    def log_event(self, sysid, event):
        with self._lock:
            self._f.write(f"{self._ts()} EVENT sysid={sysid} event={event}\n")

    def log_command(self, sysid, action, detail=''):
        line = f"{self._ts()} CMD sysid={sysid} action={action}"
        if detail:
            line += f" {detail}"
        line += '\n'
        with self._lock:
            self._f.write(line)

    def close(self):
        with self._lock:
            self._f.close()


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
                sysid = int(item.get("sysid", index))
            else:
                ip_address = str(item).strip()
                sysid = index

            if ip_address:
                name = item.get("name", "").strip() if isinstance(item, dict) else ""
                if not name:
                    name = ip_address.split(".")[-1].zfill(3)
                results.append({"sysid": sysid, "ip": ip_address, "name": name})
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
        results.append({"sysid": index, "ip": ip_part, "name": name})
    return results


class DJIInterface:
    """Per-drone manager that keeps a TCP telemetry connection alive and sends HTTP commands."""

    def __init__(self, IP_RC="", sysid=None, telemetry_port=8081, retry_interval=15.0, connect_timeout=5.0):
        self.IP_RC = IP_RC or ""
        self.sysid = sysid
        self.telemetryPort = telemetry_port
        self.retry_interval = retry_interval
        self.connect_timeout = connect_timeout
        self.baseCommandUrl = f"http://{self.IP_RC}:8080" if self.IP_RC else ""

        self._telemetry = {}
        self._telemetry_lock = threading.Lock()
        self._telemetry_thread = None
        self._running = False

        self.connected = False
        self.last_error = ""
        self.last_http_error = ""  # <-- NEW: Keep track of HTTP issues separately
        self.last_seen = 0.0
        self.retrying = False

    def startTelemetryStream(self):
        if self._running:
            return
        self._running = True
        self._telemetry_thread = threading.Thread(target=self._telemetry_receiver, daemon=True)
        self._telemetry_thread.start()

    def stopTelemetryStream(self):
        self._running = False
        if self._telemetry_thread:
            self._telemetry_thread.join(timeout=2)

    def _set_disconnected(self, error_message):
        """Forces a clean reset of connection states."""
        self.connected = False
        self.retrying = True
        self.last_error = error_message
        # Do NOT reset self.last_seen here, let the UI know when we last had data

    def _telemetry_receiver(self):
        while self._running:
            sock = None
            buffer = ""
            try:
                self.retrying = True
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connect_timeout)
                
                # Try opening a fresh socket connection
                sock.connect((self.IP_RC, self.telemetryPort))
                
                # Connection successful! Update states
                sock.settimeout(2.0)  # Give up to 2 seconds between data frames
                self.connected = True
                self.retrying = False
                self.last_error = ""
                print(f"[FLEET] Connected SYSID={self.sysid} IP={self.IP_RC} telemetry={self.telemetryPort}")

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
                # Active failure state: immediately clean up everything
                self._set_disconnected(str(exc))
                print(f"[FLEET] Telemetry disconnected SYSID={self.sysid} IP={self.IP_RC}: {exc}. Retrying in {self.retry_interval}s...")
                
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
            print(f"[HTTP ERROR] SYSID={self.sysid} command to {endPoint} failed: {exc}")
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
            "sysid": self.sysid,
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
        self.ip_by_sysid = {}
        self.name_by_sysid = {}
        self._prev_connection_state = {}
        self.last_error_by_sysid = {}
        self.last_seen_by_sysid = {}
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
            sysid = int(entry["sysid"])
            ip_address = entry["ip"]
            
            if sysid in self.managers:
                print(f"[FLEET] SYSID={sysid} is already loaded. Skipping re-initialization.")
                continue
            
            manager = DJIInterfaceSafety(
                IP_RC=ip_address,
                safety_token=SAFETY_TOKEN
            )
            manager.sysid = sysid
            manager.telemetryPort = self.telemetry_port
            manager.retry_interval = self.retry_interval
            manager.connect_timeout = self.connect_timeout
            manager.baseCommandUrl = f"http://{ip_address}:{DRONE_COMMAND_PORT}"

            self.managers[sysid] = manager
            self.ip_by_sysid[sysid] = ip_address
            self.name_by_sysid[sysid] = entry.get("name", ip_address.split(".")[-1].zfill(3))
            
            manager.startTelemetryStream()
            
            print(f"[FLEET] Spawned Single Managers for SYSID={sysid} IP={ip_address}")

    def stop(self):
        self._running = False
        for sysid in list(self.managers.keys()):
            try:
                self.managers[sysid].stopTelemetryStream()
            except Exception as e:
                print(f"[FLEET] Error stopping manager thread for SYSID {sysid}: {e}")
                

    def _sync_snapshot(self):
        with self.lock:
            self.open_drones = []
            self.retry_drones = []
            for sysid, manager in self.managers.items():
                snapshot = manager.get_snapshot()

                self.connection_state[sysid] = bool(snapshot["connected"])
                self.last_error_by_sysid[sysid] = snapshot["last_error"]
                self.last_seen_by_sysid[sysid] = snapshot["last_seen"]
                self.last_error[sysid] = snapshot["last_error"]
                self.last_seen[sysid] = snapshot["last_seen"]
                self.retrying[sysid] = bool(snapshot["retrying"])
                self.agents[sysid] = snapshot["last_seen"]

                telemetry = snapshot.get("telemetry", {})
                location = telemetry.get("location", {})

                lat = float(location.get("latitude", 0.0))
                lon = float(location.get("longitude", 0.0))
                alt = float(location.get("altitude", 0.0))

                heading = float(telemetry.get("heading", 0.0))
                battery = telemetry.get("batteryLevel", snapshot.get("battery", -1))
                flight_mode = telemetry.get("flightMode", snapshot.get("flight_mode", "UNKNOWN"))
                manual_override = telemetry.get("isManualOverrideActive", False)

                self.gps_data[sysid] = (lat, lon, alt)
                self.batt_pct[sysid] = battery
                self.heading[sysid] = heading
                self.armed_state[sysid] = not manual_override
                self.airborne_state[sysid] = alt > 0.5
                self.rtl_active[sysid] = False
                self.flight_mode_str[sysid] = flight_mode

                connected = bool(snapshot["connected"])
                prev = self._prev_connection_state.get(sysid)
                if prev is None:
                    # first snapshot — log initial state if already online
                    if connected:
                        ulog.log_event(sysid, 'CONNECT')
                elif connected and not prev:
                    ulog.log_event(sysid, 'CONNECT')
                elif not connected and prev:
                    ulog.log_event(sysid, 'DISCONNECT')
                self._prev_connection_state[sysid] = connected

                if connected:
                    ulog.log_telemetry(
                        sysid=sysid, lat=lat, lon=lon, alt=alt,
                        batt_pct=float(battery) if isinstance(battery, (int, float)) else 0.0,
                        heading=heading,
                        armed=not manual_override,
                        airborne=alt > 0.5,
                    )

                if snapshot["connected"]:
                    self.open_drones.append(sysid)
                else:
                    self.retry_drones.append(sysid)

    def _sync_loop(self):
        while self._running:
            self._sync_snapshot()
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

    def _manager(self, sysid):
        return self.managers.get(sysid)

    def _safetymanager(self, sysid):            #reroute to prevent needing to change lots of safetymanager->manager later
        return self.managers.get(sysid)


# -----------------------------
# Shared styles & Fleet Init
# -----------------------------
BTN_STYLE = {
    'height': '40px',
    'padding': '0 16px',
    'fontWeight': 'bold',
    'borderRadius': '8px',
    'border': '1px solid #ccc',
    'backgroundColor': '#f7f7f7',
    'fontFamily': "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    'cursor': 'pointer',
    'transition': 'all 0.2s ease',
}

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

def vertical_divider(height='6vh', extra_class=''):
    return html.Div(className=('agent-div ' + extra_class).strip(), style={'width': '1px', 'height': height, 'backgroundColor': '#999', 'margin': '0 5px', 'flexShrink': 0})

def status_square(is_on, label):
    return html.Div(title=label, style={'width': 'clamp(8px, 1vw, 14px)', 'height': 'clamp(8px, 1vw, 14px)', 'backgroundColor': '#4CAF50' if is_on else '#F44336', 'border': '1px solid #333', 'borderRadius': '50%', 'boxShadow': '0 1px 2px rgba(0,0,0,0.2)'})

def status_indicator(is_on, label):
    return html.Div([status_square(is_on, label), html.Span(label, style={'fontSize': 'clamp(10px, 1vw, 12px)', 'marginLeft': '5px', 'whiteSpace': 'nowrap'})], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '0.3vh'})

BUTTON_MIN_WIDTH = 50
NUM_COLS = 3
COLUMN_MIN_WIDTH = BUTTON_MIN_WIDTH * NUM_COLS
button_style = {'width': '100%', 'height': '100%', 'fontSize': 'clamp(10px,1vw,14px)', 'borderRadius': '4px'}

def create_agent_bar(agent_id, ip, name, online, retrying, last_seen, gps, batt, mode, last_error):
    divider_height = '6vh'
    status_text = 'ONLINE' if online else 'OFFLINE'
    age_text = 'fresh' if not last_seen else f"{time.time() - last_seen:.1f}s ago"
    drone_label = name

    return html.Div(
        # FIX: The pattern ID is now on the card itself so it styles the correct DOM element
        id={'type': 'agent-row-style', 'index': agent_id},
        children=[
            html.Div([
                html.Div(f"Drone {drone_label}", style={'fontWeight': 'bold','fontSize': 'clamp(12px, 1.5vw, 18px)','lineHeight':'1.1','fontFamily':'monospace'}),
                html.Div(f"IP: {ip}", style={'fontSize': 'clamp(10px, 1vw, 14px)'}),
                html.Div(f"State: {status_text}", id={'type': 'agent-state-txt', 'index': agent_id}, style={'fontSize': 'clamp(10px, 1vw, 14px)'}),
                html.Div(f"Last seen: {age_text}", id={'type': 'agent-age-txt', 'index': agent_id}, style={'fontSize': 'clamp(10px, 1vw, 14px)'}),
                html.Div(f"Mode: {mode}", id={'type': 'agent-mode-txt', 'index': agent_id}, style={'fontSize': 'clamp(10px, 1vw, 14px)'})
            ], className='agent-col agent-col-identity', style={'display': 'flex','flexDirection':'column','justifyContent':'center','flex':'0 0 auto'}),
            vertical_divider(divider_height, 'agent-div-before-status'),
            html.Div([
                html.Div([
                    html.Div(id={'type': 'agent-airborne-dot', 'index': agent_id}, title="Airborne",
                             style={'width': 'clamp(8px, 1vw, 14px)', 'height': 'clamp(8px, 1vw, 14px)',
                                    'backgroundColor': '#F44336', 'border': '1px solid #333',
                                    'borderRadius': '50%', 'boxShadow': '0 1px 2px rgba(0,0,0,0.2)'}),
                    html.Span("Airborne", style={'fontSize': 'clamp(10px, 1vw, 12px)', 'marginLeft': '5px', 'whiteSpace': 'nowrap'})
                ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '0.3vh'}),
                status_indicator(not retrying, "Retry OK"),
                status_indicator(last_error == "", "Healthy"),
                html.Div(last_error if last_error else "", style={'fontSize': 'clamp(9px, 0.9vw, 11px)', 'color': '#333', 'maxWidth': '110px', 'overflow': 'hidden', 'textOverflow': 'ellipsis', 'whiteSpace': 'nowrap'})
            ], className='agent-col agent-col-status', style={'display':'flex','flexDirection':'column','justifyContent':'center','alignItems':'flex-start','flex':'0 0 auto'}),
            vertical_divider(divider_height, 'agent-div-before-telemetry'),
            html.Div([
                html.Div(f"Batt: {batt:.1f}%" if isinstance(batt, (int, float)) else f"Batt: {batt}", id={'type': 'agent-batt-txt', 'index': agent_id}, style={'fontSize':'clamp(10px,1vw,14px)'}),
                html.Div(f"Lat: {gps[0]:.6f}", id={'type': 'agent-lat-txt', 'index': agent_id}, style={'fontSize':'clamp(10px,1vw,14px)'}),
                html.Div(f"Lon: {gps[1]:.6f}", id={'type': 'agent-lon-txt', 'index': agent_id}, style={'fontSize':'clamp(10px,1vw,14px)'}),
                html.Div(f"Alt: {gps[2]:.1f}", id={'type': 'agent-alt-txt', 'index': agent_id}, style={'fontSize':'clamp(10px,1vw,14px)'})
            ], className='agent-col agent-col-telemetry', style={'display':'flex','flexDirection':'column','justifyContent':'center','flex':'0 0 auto'}),
            vertical_divider(divider_height, 'agent-div-before-buttons'),
            html.Div(
                html.Div([
                    html.Button("Takeoff", id={'type':'agent-btn','index':f'{agent_id}-takeoff'}, type="button", n_clicks=0, style=button_style),
                    html.Button("Hold", id={'type':'agent-btn','index':f'{agent_id}-hold'}, type="button", n_clicks=0, style=button_style),
                    html.Button("Goto", id={'type':'agent-btn','index':f'{agent_id}-goto'}, type="button", n_clicks=0, style={**button_style, "backgroundColor": "#d9e8ff"}),
                    html.Button("Land", id={'type':'agent-btn','index':f'{agent_id}-land'}, type="button", n_clicks=0, style=button_style),
                    html.Button("RTL", id={'type':'agent-btn','index':f'{agent_id}-rtl'}, type="button", n_clicks=0, style=button_style),
                    html.Button("Return CTL", id={'type':'agent-btn','index':f'{agent_id}-release'}, type="button", n_clicks=0, style={**button_style, "backgroundColor": "#fff0f0", "color": "#a00000"}),
                ], className='agent-btn-grid', style={'display': 'grid', 'gridTemplateColumns': f'repeat({NUM_COLS}, 1fr)', 'gridTemplateRows': 'repeat(2, 1fr)', 'gap': '4px', 'flex': '1'}),
                className='agent-col agent-col-buttons',
                style={'display': 'flex', 'flexDirection': 'column', 'flex': '0 0 auto', 'height': '100%'}
            ),
            vertical_divider(divider_height, 'agent-div-trailing')
        ],
        style={
            'display': 'flex',
            'alignItems': 'stretch',
            'height': 'calc(5.5 * clamp(12px, 1vw, 14px) + 2 * 0.5vh + 16px)',
            'border': '1px solid #ddd',
            'borderRadius': '12px',
            'padding': '8px',
            'margin': '0.5vh 0',
            'backgroundColor': '#f0f0f0',
            'color': '#aaaaaa',
            'boxShadow': '0 2px 5px rgba(0,0,0,0.1)',
            'transition': 'all 0.2s ease'
        }
    )


# -----------------------------
# Dash App Framework Initialisation
# -----------------------------
app = dash.Dash(__name__, update_title=None)
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            button:hover {
                background-color: #e0e0e0;
                transform: scale(1.03);
                transition: all 0.2s ease;
            }
            .agent-mini:hover {
                box-shadow: 0 3px 8px rgba(0,0,0,0.15);
                background-color: rgba(230,230,255,0.3);
            }
            ::-webkit-scrollbar {
                width: 8px;
                height: 8px;
            }
            ::-webkit-scrollbar-thumb {
                background: rgba(0,0,0,0.2);
                border-radius: 4px;
            }
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

server = app.server
@server.route("/tiles/<path:path>")
def serve_tiles(path):
    return send_from_directory("assets/tiles", path)


app.layout = html.Div([
    # COMPACTED TITLE: Reduced text size, stripped heavy margins
    html.H1("FlightBoard Dashboard", style={
        'fontFamily': "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif", 
        'fontWeight': '600', 
        'fontSize': 'clamp(16px, 1.8vw, 22px)', 
        'margin': '0 0 4px 0',
        'lineHeight': '1.1'
    }),
    # COMPACTED STATUS WINDOW: Trimmed padding and bottom margin down
    html.Div(id="button-output", style={
        'width': '100%', 
        'padding': '6px 12px', 
        'backgroundColor': '#f9f9f9', 
        'border': '1px solid #ddd', 
        'marginBottom': '6px', 
        'fontSize': '13px', 
        'borderRadius': '6px',
        'boxSizing': 'border-box'
    }),
    html.Div([
        html.Button("TAKEOFF ALL", id="takeoff-all", n_clicks=0, style={**BTN_STYLE, 'marginRight': '6px'}),
        html.Button("HOLD ALL", id="hold-all", n_clicks=0, style={**BTN_STYLE, 'marginRight': '6px'}),
        html.Button("LAND ALL", id="land-all", n_clicks=0, style={**BTN_STYLE, 'marginRight': '6px'}),
        html.Button("RTL ALL", id="rtl-all", n_clicks=0, style={**BTN_STYLE, 'marginRight': '6px'}),
        dcc.ConfirmDialog(id='confirm-land-all', message='Are you sure you want to LAND ALL drones?'),
        dcc.ConfirmDialog(id='confirm-rtl-all', message='Are you sure you want to RTL ALL drones?'),
    ], style={'marginBottom': '20px', 'display': 'flex', 'alignItems': 'stretch'}),

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
                    dl.TileLayer(url="/tiles/nenana/{z}/{x}/{y}.jpg", tms=False, noWrap=True, minZoom=1, maxZoom=22),
                    dl.LayerGroup(id='layer-rtl-targets'),
                    dl.LayerGroup(id='layer-targets'),
                    dl.LayerGroup(id='map-markers'),
                    dl.LayerGroup(id='layer-custom'),
                ],
                style={'width': '100%', 'height': '100%'}
            ),
            html.Div(children=[
                html.Button("Center Map", id="map-center", n_clicks=0, style={**BTN_STYLE, 'width': '100%', 'marginBottom': '8px'}),
                html.Div([
                    html.Div("Goto Height (m)", style={'fontSize': '12px', 'marginBottom': '2px', 'textAlign': 'left'}),
                    dcc.Input(id='goto-height', type='number', placeholder='50', value=defaults_data.get("goto_height", 50), style={'width': '100%', 'height': '36px', 'fontSize': '14px', 'boxSizing': 'border-box'})
                ], style={'display': 'flex', 'flexDirection': 'column', 'alignItems': 'stretch', 'marginBottom': '6px'})
            ], style={'position': 'absolute', 'top': '10px', 'right': '10px', 'width': '160px', 'backgroundColor': 'rgba(255, 255, 255, 0.95)', 'padding': '8px', 'borderRadius': '8px', 'boxShadow': '0 2px 6px rgba(0,0,0,0.2)', 'zIndex': 1000, 'display': 'flex', 'flexDirection': 'column', 'alignItems': 'stretch'})
        ], id='map-container', style={'position': 'relative', 'width': '100%', 'height': '100%', 'flex': '1 1 auto', 'minHeight': '0'}),

        html.Div([
            html.Button("⯈", id="toggle-agents", className="toggle-btn"),
            html.Div(
                id='agent-container', 
                children=[
                    # Clean outer holder div with flat IDs to prevent border mirroring shadows
                    html.Div(
                        create_agent_bar(
                            agent_id=entry["sysid"],
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
                        id=f"agent-row-wrapper-{entry['sysid']}"
                    ) for entry in load_drone_ip_list(DRONE_LIST_FILE)
                ], 
                style={'height': '100%', 'overflowY': 'auto', 'overflowX': 'hidden', 'display': 'flex', 'flexDirection': 'column'}
            )
        ], id='agents-bar', className='collapsed')
    ], style={'position': 'relative', 'flex': '1', 'minHeight': '0', 'width': '100%', 'display': 'flex', 'flexDirection': 'row'}),

    dcc.Interval(id='interval-agents', interval=500, n_intervals=0),
    dcc.Interval(id='interval-map', interval=250, n_intervals=0),
    dcc.Store(id='viewport-width', storage_type='session'),
    dcc.Store(id="resize-trigger"),
    
    html.Div(id='debug-output', style={'display':'none'}),
    html.Div(id='map-click-left-output', style={'display': 'none'}),
    html.Div(id='map-click-right-output', style={'display': 'none'}),
    html.Div(id='map-click-dleft-output', style={'display': 'none'}),
], style={'height': '100vh', 'width': '100vw', 'display': 'flex', 'flexDirection': 'column', 'overflow': 'hidden', 'boxSizing': 'border-box', 'padding': '12px', 'gap': '12px'})


# -----------------------------
# Callbacks
# -----------------------------
@app.callback(
    Output('debug-output', 'children'),
    Input({'type': 'agent-btn', 'index': ALL}, 'n_clicks_timestamp'),
    State({'type': 'agent-btn', 'index': ALL}, 'id'),
    State('goto-height', 'value'),
    prevent_initial_call=True
)
def debug_buttons(timestamps, ids, dynamic_height):
    valid = [(ts, i) for i, ts in enumerate(timestamps) if ts is not None]
    if not valid: 
        return dash.no_update

    max_ts, max_idx = max(valid, key=lambda x: x[0])
    btn_id = ids[max_idx]
    sysid, action = btn_id['index'].split('-')

    if sysid.isdigit():
        sysid_int = int(sysid)
        action = action.lower()
        
        if action == 'goto' and dynamic_height is not None:
            with receiver.lock:
                if sysid_int in receiver.targets:
                    receiver.targets[sysid_int]['alt'] = float(dynamic_height)

        send_command(sysid_int, action)

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
        active_sysids = list(receiver.open_drones)
        if not active_sysids: 
            return dash.no_update
            
        latitudes, longitudes = [], []
        for sysid in active_sysids:
            gps = receiver.gps_data.get(sysid)
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
    Output({'type': 'agent-row-style', 'index': ALL}, 'style'),
    Output({'type': 'agent-state-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-age-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-mode-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-batt-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-lat-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-lon-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-alt-txt', 'index': ALL}, 'children'),
    Output({'type': 'agent-airborne-dot', 'index': ALL}, 'style'),
    Input('interval-agents', 'n_intervals'),
    State({'type': 'agent-state-txt', 'index': ALL}, 'id')
)
def update_dashboard(n, dynamic_ids):
    row_styles = []
    states, ages, modes, batts, lats, lons, alts, airborne_dots = [], [], [], [], [], [], [], []

    with receiver.lock:
        for idx_dict in dynamic_ids:
            sysid = idx_dict['index']

            online = receiver.connection_state.get(sysid, False)
            last_seen = receiver.last_seen.get(sysid, 0.0)
            gps = receiver.gps_data.get(sysid, (0.0, 0.0, 0.0))
            batt = receiver.batt_pct.get(sysid, 0)
            mode = receiver.flight_mode_str.get(sysid, "UNKNOWN")

            status_text = 'ONLINE' if online else 'OFFLINE'
            age_text = 'fresh' if not last_seen else f"{time.time() - last_seen:.1f}s ago"

            stale = online and (last_seen == 0 or time.time() - last_seen > 20)
            if not online:
                bg_color, text_color = '#f0f0f0', '#aaaaaa'
            elif stale:
                bg_color, text_color = '#d4eaf4', '#000000'
            elif batt < 30:
                bg_color, text_color = '#f4d4d4', '#000000'
            elif batt < 50:
                bg_color, text_color = '#f4f0d4', '#000000'
            else:
                bg_color, text_color = '#d4f4dd', '#000000'

            row_styles.append({
                'display': 'flex', 'alignItems': 'stretch',
                'height': 'calc(5.5 * clamp(12px, 1vw, 14px) + 2 * 0.5vh + 16px)',
                'border': '1px solid #ddd', 'borderRadius': '12px', 'padding': '8px',
                'margin': '0.5vh 0', 'backgroundColor': bg_color, 'color': text_color,
                'boxShadow': '0 2px 5px rgba(0,0,0,0.1)', 'transition': 'all 0.2s ease'
            })

            states.append(f"State: {status_text}")
            ages.append(f"Last seen: {age_text}")
            modes.append(f"Mode: {mode}")
            batts.append(f"Batt: {batt:.1f}%" if isinstance(batt, (int, float)) else f"Batt: {batt}")
            lats.append(f"Lat: {gps[0]:.6f}")
            lons.append(f"Lon: {gps[1]:.6f}")
            alts.append(f"Alt: {gps[2]:.1f}")

            airborne = gps[2] > 2.0
            airborne_dots.append({
                'width': 'clamp(8px, 1vw, 14px)', 'height': 'clamp(8px, 1vw, 14px)',
                'backgroundColor': '#4CAF50' if airborne else '#F44336',
                'border': '1px solid #333', 'borderRadius': '50%',
                'boxShadow': '0 1px 2px rgba(0,0,0,0.2)'
            })

    return row_styles, states, ages, modes, batts, lats, lons, alts, airborne_dots


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
        for sysid in receiver.agents.keys():
            gps = receiver.gps_data.get(sysid, (0, 0, 0))
            lat, lon, _alt = gps
            if lat != 0 and lon != 0:
                icon_url = f"/assets/blueNumberMarkers/number_{sysid}.png"
                markers.append(
                    dl.Marker(
                        position=[lat, lon],
                        children=dl.Tooltip(f"SYSID: {sysid}, Alt: {_alt}, Yaw: {receiver.heading.get(sysid, 0.0):.1f}°"),
                        icon={"iconUrl": icon_url, "iconSize": [48,48], "iconAnchor": [24, 48]},
                        zIndexOffset=1000
                    )
                )
                size = 120
                blue_arrow_icon_url = get_blue_arrow_icon_url(receiver.heading[sysid])
                anchor_x, anchor_y = compute_arrow_anchor((size, size), receiver.heading[sysid])
                markers.append(
                    dl.Marker(position=[lat,lon], icon={"iconUrl": blue_arrow_icon_url, "iconSize": [size, size], "iconAnchor": [anchor_x, anchor_y]}, zIndexOffset=999)
                )
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
        for sysid, target in receiver.rtltargets.items():
            if not target: continue
            lat, lon, alt = target.get('lat', 0), target.get('lon', 0), target.get('alt', 0)
            if lat == 0 and lon == 0: continue
            markers.append(dl.Marker(position=[lat, lon], icon={"iconUrl": "/assets/cross.png", "iconSize": [40, 40], "iconAnchor": [20, 20]}, children=dl.Tooltip(f"RTL SYSID {sysid}: {lat:.6f}, {lon:.6f}, alt {alt:.1f}")))
    return markers


def compute_arrow_anchor(image_size_px, rotation_deg):
    w, h = image_size_px
    cx, cy = w / 2, h / 2
    base_x, base_y = cx, h
    rel_x, rel_y = base_x - cx, base_y - cy
    theta = math.radians(-rotation_deg)
    new_y = rel_y * math.cos(theta)
    new_x = rel_y * math.sin(theta)
    return [round(cx + new_x, 1), round(cy + new_y, 1)]

def get_blue_arrow_icon_url(heading_deg: float) -> str:
    return f"/assets/bluearrow_rotated/bluearrow_{int(round(heading_deg)) % 360:03d}.png"

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
        sysids = sorted(receiver.agents.keys())
        targets = generate_circular_targets(center_lat=lat, center_lon=lon, alt=target_alt, radius_m=8.0, offset_deg=0, tgt_alt=target_alt, num_targets=max(len(sysids), 1))
        receiver.targets = {sysid: target for sysid, target in zip(sysids, targets)}

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
    prevent_initial_call=True
)
def handle_all_buttons(n_takeoff, n_hold, n_land_confirm, n_rtl_confirm):
    triggered_id = ctx.triggered_id
    if triggered_id is None: return dash.no_update

    if triggered_id == "takeoff-all": action = "TAKEOFF"
    elif triggered_id == "hold-all": action = "HOLD"
    elif triggered_id == "confirm-land-all" and n_land_confirm: action = "LAND"
    elif triggered_id == "confirm-rtl-all" and n_rtl_confirm: action = "RTL"
    else: return dash.no_update

    print(f"[DEBUG] ALL button action confirmed via Safety Interface: {action}")
    send_command_all(action)
    return f"ALL action executed: {action}"


def send_command(sysid: int, action: str):
    """Send a swarm command to a single drone authenticated via Safety Computer Interface."""
    action = action.lower()
    ulog.log_command(sysid, action)

    manager = receiver._safetymanager(sysid)
    if not manager:
        print(f"[ERROR] No active authenticated safety manager found for SYSID {sysid}")
        return

    if action == 'takeoff':
        print(f"[SAFETY COMMAND] Sending Authenticated TAKEOFF to Drone SYSID {sysid} at IP {manager.IP_RC}")
        manager.requestSendTakeOff()
        
    elif action == 'hold':
        print(f"[SAFETY COMMAND] Sending Authenticated HOLD/ABORT to Drone SYSID {sysid} at IP {manager.IP_RC}")
        manager.requestAbortMission()
        
    elif action == 'goto':
        target = receiver.targets.get(sysid)
        if not target:
            print(f"[WARNING] No staged goto target coordinates found for SYSID {sysid}")
            return
        
        lat = target.get('lat', 0)
        lon = target.get('lon', 0)
        alt = target.get('alt', 0)
        yaw = target.get('head', target.get('yaw', 0.0))
        
        print(f"[SAFETY COMMAND] Sending Authenticated GOTO to Drone SYSID {sysid} -> Lat: {lat}, Lon: {lon}")
        ulog.log_command(sysid, 'goto_target', f"lat={lat} lon={lon} alt={alt} yaw={yaw}")
        manager.requestSendGoToWPwithPID(lat, lon, alt, yaw)
        
    elif action == 'land':
        print(f"[SAFETY COMMAND] Sending Authenticated LAND to Drone SYSID {sysid} at IP {manager.IP_RC}")
        manager.requestSendLand()
        
    elif action == 'rtl':
        print(f"[SAFETY COMMAND] Sending Authenticated RTL to Drone SYSID {sysid} at IP {manager.IP_RC}")
        manager.requestSendRTH()

    elif action == 'release':
        print(f"[SAFETY COMMAND] Releasing Safety Computer Control back to Pilot for SYSID {sysid} at IP {manager.IP_RC}")
        manager.requestReleaseSafetyControl()
        
    else:
        print(f"[WARNING] Unknown action token invocation for SYSID {sysid}: {action}")


def send_command_all(action: str):
    for sysid in receiver.agents.keys():
        time.sleep(0.1)
        send_command(sysid, action)

# -----------------------------
# Run server
# -----------------------------
if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=8050)