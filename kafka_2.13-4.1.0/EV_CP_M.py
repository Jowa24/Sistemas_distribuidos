import sys
import socket
import time

from EV_Utile import (
    StatusMessage, InformationTypeMonitor, ChargingPointStatus,
    frame_message, SyncSocketFrameProtocol
)

# --- Global Config ---
FORMAT = 'utf-8'
POLL_INTERVAL = 1
SOCKET_TIMEOUT = 3.0
RECONNECT_DELAY = 5

def main():
    if len(sys.argv) < 5:
        print("Usage: python3 EV_CP_M.py <engine_ip> <engine_port> <central_ip> <central_port> <cp_id>")
        sys.exit(1)

    SERVER_ENGINE = sys.argv[1]
    PORT_ENGINE = int(sys.argv[2])
    ADDR_ENGINE = (SERVER_ENGINE, PORT_ENGINE)

    SERVER_CENTRAL = sys.argv[3]
    PORT_CENTRAL = int(sys.argv[4])
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)

    CP_ID = sys.argv[5]

    monitor = Monitor(
        ADDR_CENTRAL,
        ADDR_ENGINE,
        CP_ID
    )
    monitor.start()


class Monitor:

    def __init__(self, addr_central, addr_engine, cp_id):
        self._addr_central = addr_central
        self._addr_engine = addr_engine
        self._cp_id = cp_id
        
        self._central_client = None
        self._engine_client = None
        
        # Protokoll-Handler
        self._central_protocol = None
        self._engine_protocol = None

    # --- Properties ---
    @property
    def ip_central(self): return self._addr_central
    @property
    def ip_engine(self): return self._addr_engine
    @property
    def cp_id(self): return self._cp_id

    # --- Methods ---

    def start(self):
        
        while True:
            try:
                self.connect_sockets()
                self.run()
            except (socket.timeout, ConnectionRefusedError, ConnectionResetError, BrokenPipeError, ConnectionError) as e:
                print(f"Monitor {self.cp_id}: Connection lost ({e}). Reconnecting in {RECONNECT_DELAY}s...")
                self.close_sockets()
                time.sleep(RECONNECT_DELAY)
            except Exception as e:
                print(f"Monitor {self.cp_id}: Unexpected error: {e}. Restarting...")
                self.close_sockets()
                time.sleep(RECONNECT_DELAY)

    def connect_sockets(self):
        
        print(f"Monitor {self.cp_id}: Connecting to Engine at {self.ip_engine}...")
        self._engine_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._engine_client.settimeout(SOCKET_TIMEOUT)
        self._engine_client.connect(self.ip_engine)
        self._engine_protocol = SyncSocketFrameProtocol(self._engine_client) # Handler

        print(f"Monitor {self.cp_id}: Connecting to Central at {self.ip_central}...")
        self._central_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._central_client.settimeout(SOCKET_TIMEOUT)
        self._central_client.connect(self.ip_central)
        self._central_protocol = SyncSocketFrameProtocol(self._central_client) # Handler
        
        print(f"Monitor {self.cp_id}: Sockets connected.")
        
    def _parse_engine_response(self, response_str: str) -> (ChargingPointStatus, float):
        try:
            parts = response_str.split('&')
            status_val = int(parts[0].split('=')[1])
            price_val = float(parts[1].split('=')[1])
            return ChargingPointStatus(status_val), price_val
        except Exception as e:
            print(f"Monitor {self.cp_id}: Error parsing engine response '{response_str}': {e}")
            return ChargingPointStatus.DEFECT, 0.0

    def run(self):
        last_sent_status = None
        is_first_poll = True 
        print(f"Monitor {self.cp_id}: Starting 1-second poll loop...")

        while True:
            current_status = None
            current_price_kw = 0.0
            
            try:
                self._engine_client.sendall(frame_message("GET_STATUS"))
                response_str = self._engine_protocol.read_message()

                if not response_str:
                    raise ConnectionResetError("Engine closed connection")
                
                current_status, current_price_kw = self._parse_engine_response(response_str)

            except socket.timeout:
                print(f"Monitor {self.cp_id}: Engine poll TIMEOUT. Assuming DEFECT.")
                current_status = ChargingPointStatus.DEFECT
                current_price_kw = 0.0

            if current_status is not None and (current_status != last_sent_status or is_first_poll):
                
                try:
                    self.send_status_to_central(
                        current_status, 
                        last_sent_status, 
                        current_price_kw, 
                        is_first_poll
                    )
                except (socket.error, BrokenPipeError, ConnectionResetError) as e:
                    print(f"Monitor {self.cp_id}: Central seems down. ({e})")
                    raise ConnectionError("Central connection failed")
                
                last_sent_status = current_status
                is_first_poll = False

            time.sleep(POLL_INTERVAL)

    def send_status_to_central(self, current_status: ChargingPointStatus, 
                                 last_status: ChargingPointStatus, 
                                 current_price_kw: float, 
                                 is_auth: bool = False):
        
        msg_type = None
        
        if is_auth:
            msg_type = InformationTypeMonitor.AUTHENTICATION
            print(f"Monitor {self.cp_id}: Sending AUTHENTICATION -> {current_status.name}")
        elif current_status == ChargingPointStatus.DEFECT:
            msg_type = InformationTypeMonitor.ERROR
            print(f"Monitor {self.cp_id}: Sending ERROR -> {current_status.name}")
        elif last_status == ChargingPointStatus.DEFECT and current_status == ChargingPointStatus.ACTIVE:
            msg_type = InformationTypeMonitor.ERROR_RESOLVED
            print(f"Monitor {self.cp_id}: Sending ERROR_RESOLVED -> {current_status.name}")
        else:
            msg_type = InformationTypeMonitor.STATUS_UPDATE
            # print(f"Monitor {self.cp_id}: Sending STATUS_UPDATE -> {current_status.name}")

        msg_obj = StatusMessage(
            type=msg_type,
            cp_id=self.cp_id,
            status=current_status,
            price_kw=current_price_kw
        )

        self._central_client.sendall(frame_message(msg_obj.encode_for_socket()))

    def close_sockets(self):
        if self._central_client:
            self._central_client.close()
            self._central_client = None
        if self._engine_client:
            self._engine_client.close()
            self._engine_client = None

if __name__ == "__main__":
    main()