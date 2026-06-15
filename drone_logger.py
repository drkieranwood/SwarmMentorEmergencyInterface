import os
import threading
from abc import ABC, abstractmethod
from datetime import datetime


class DroneLogger(ABC):
    """Abstract interface for drone flight logging.

    Implementations must handle telemetry, events, and commands.

    armed and airborne are platform-specific (ArduCopter / DJI respectively)
    and may be None when not applicable.
    """

    @abstractmethod
    def log_telemetry(self, drone_id, lat, lon, alt, heading,
                      batt_pct=None, armed=None, airborne=None):
        ...

    @abstractmethod
    def log_event(self, drone_id, event):
        ...

    @abstractmethod
    def log_command(self, drone_id, action, detail=''):
        ...

    @abstractmethod
    def close(self):
        ...


class ULogWriter(DroneLogger):
    """Write drone telemetry and commands to a plain-text log file.

    Each line: ISO-timestamp TYPE key=value ...
    Types: TELEM, EVENT, CMD
    """

    def __init__(self, filepath):
        self._filepath = filepath
        self._f = open(filepath, 'w', encoding='utf-8', buffering=1)  # line-buffered
        self._lock = threading.Lock()
        self._f.write(f"# FlightBoard log started {datetime.now().isoformat()}\n")
        self._f.write("# TELEM: timestamp id lat lon alt heading [batt] [armed] [airborne]\n")
        self._f.write("# EVENT: timestamp id event=CONNECT|DISCONNECT\n")
        self._f.write("# CMD:   timestamp id action [detail...]\n")
        print(f"[LOG] Logging to {filepath}")

    def _ts(self):
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

    def log_telemetry(self, drone_id, lat, lon, alt, heading,
                      batt_pct=None, armed=None, airborne=None):
        line = (
            f"{self._ts()} TELEM"
            f" id={drone_id}"
            f" lat={lat:.7f} lon={lon:.7f} alt={alt:.2f}"
            f" hdg={heading:.1f}"
        )
        if batt_pct is not None:
            line += f" batt={batt_pct:.1f}"
        if armed is not None:
            line += f" armed={int(bool(armed))}"
        if airborne is not None:
            line += f" airborne={int(bool(airborne))}"
        line += '\n'
        with self._lock:
            self._f.write(line)

    def log_event(self, drone_id, event):
        with self._lock:
            self._f.write(f"{self._ts()} EVENT id={drone_id} event={event}\n")

    def log_command(self, drone_id, action, detail=''):
        line = f"{self._ts()} CMD id={drone_id} action={action}"
        if detail:
            line += f" {detail}"
        line += '\n'
        with self._lock:
            self._f.write(line)

    def size_mb(self):
        try:
            return os.path.getsize(self._filepath) / (1024 * 1024)
        except OSError:
            return 0.0

    def close(self):
        with self._lock:
            self._f.close()
