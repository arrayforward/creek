import argparse
import json
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path


class ManagedProcess:
    def __init__(self, label, command, log_directory):
        self.label = label
        self.command = [str(part) for part in command]
        self.log_path = Path(log_directory) / f"{label}.log"
        self.log = self.log_path.open("wb")
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            creationflags=flags,
        )

    def ensure_running(self):
        code = self.process.poll()
        if code is not None:
            raise RuntimeError(f"{self.label} exited with code {code}")

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        self.log.close()

    def output(self):
        if not self.log.closed:
            self.log.flush()
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError as error:
            return f"unable to read log: {error}"


def available_port(sock_type):
    family = socket.AF_INET
    with socket.socket(family, sock_type) as sock:
        if sock_type == socket.SOCK_STREAM:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def allocate_ports():
    names_tcp = [
        "node1_metrics", "node2_metrics",
        "entry_grpc", "entry_json", "entry_metrics",
        "service1_grpc", "service1_metrics",
        "service2_grpc", "service2_metrics",
        "backend1", "backend2",
    ]
    names_udp = ["node1_udp", "node2_udp", "entry_udp", "service1_udp", "service2_udp"]
    ports = {}
    used = set()
    for name in names_tcp:
        port = available_port(socket.SOCK_STREAM)
        while port in used:
            port = available_port(socket.SOCK_STREAM)
        used.add(port)
        ports[name] = port
    for name in names_udp:
        port = available_port(socket.SOCK_DGRAM)
        while port in used:
            port = available_port(socket.SOCK_DGRAM)
        used.add(port)
        ports[name] = port
    return ports


def address(port):
    return f"127.0.0.1:{port}"


def wait_tcp(port, processes, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            process.ensure_running()
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"TCP port {port} did not become ready")


def json_rpc_call(host, port, sid, sticky, name="e2e", timeout_seconds=3):
    body = json.dumps({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "SayHello",
        "params": {"name": name},
    })
    headers = (
        f"POST /rpc HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
    )
    if sticky:
        headers += f"x-creek-sticky: true\r\n"
    else:
        headers += f"x-creek-sticky: false\r\n"
    if sid:
        headers += f"x-creek-sid: {sid}\r\n"
    headers += "Connection: close\r\n\r\n"
    payload = headers.encode("ascii") + body.encode("utf-8")

    with socket.create_connection((host, port), timeout=timeout_seconds) as conn:
        conn.settimeout(timeout_seconds)
        conn.sendall(payload)
        conn.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)

    response = b"".join(chunks)
    header, separator, body_bytes = response.partition(b"\r\n\r\n")
    if not separator or not header.startswith(b"HTTP/1.1 200"):
        raise RuntimeError(f"json rpc http failure: {response.decode('utf-8', errors='replace')}")
    data = json.loads(body_bytes.decode("utf-8"))
    result = data.get("result") or {}
    backend_id = result.get("backend_id", "")
    message = result.get("message", "")
    return backend_id, message


def run_client_json(json_address, sid, sticky, count):
    host, port_str = json_address.rsplit(":", 1)
    port = int(port_str)
    rows = []
    for i in range(count):
        backend_id, message = json_rpc_call(host, port, sid, sticky)
        if not backend_id:
            raise RuntimeError(f"json rpc call {i+1}/{count} returned no backend_id")
        rows.append((backend_id, message))
    return rows


def wait_for_discovery(json_address, processes, timeout=20):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        for process in processes:
            process.ensure_running()
        try:
            rows = run_client_json(json_address, "99", False, 1)
            return rows[0]
        except (RuntimeError, OSError) as error:
            last_error = error
            time.sleep(0.25)
    raise RuntimeError(f"service discovery timed out: {last_error}")


def wait_for_switch(json_address, previous_backend, expected_backend, processes):
    deadline = time.monotonic() + 15
    last_error = None
    while time.monotonic() < deadline:
        for process in processes:
            process.ensure_running()
        try:
            rows = run_client_json(json_address, "1", True, 1)
            if rows[0][0] == expected_backend and rows[0][0] != previous_backend:
                return rows[0]
            last_error = RuntimeError(f"request still selected {rows[0][0]}")
        except (RuntimeError, OSError) as error:
            last_error = error
        time.sleep(0.25)
    raise RuntimeError(f"backend did not switch within 15 seconds: {last_error}")


def get_json(metrics_address, path):
    host, port_text = metrics_address.rsplit(":", 1)
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {metrics_address}\r\n"
        "Accept: application/json\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")
    with socket.create_connection((host, int(port_text)), timeout=3) as connection:
        connection.settimeout(3)
        connection.sendall(request)
        connection.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    response = b"".join(chunks)
    header, separator, body = response.partition(b"\r\n\r\n")
    if not separator or not header.startswith(b"HTTP/1.1 200"):
        raise RuntimeError(f"invalid stats response: {response.decode('utf-8', errors='replace')}")
    return json.loads(body.decode("utf-8"))


def call_total(value):
    if isinstance(value, dict):
        total = 0
        for key, child in value.items():
            if key == "calls" and isinstance(child, (int, float)):
                total += child
            else:
                total += call_total(child)
        return total
    if isinstance(value, list):
        return sum(call_total(child) for child in value)
    return 0


def verify_stats(metrics_address, json_address):
    for _ in range(3):
        run_client_json(json_address, "1", True, 1)
    deadline = time.monotonic() + 6
    previous_total = 0
    previous_payload = None
    while time.monotonic() < deadline:
        time.sleep(1.1)
        previous_payload = get_json(metrics_address, "/stats?previous=1")
        previous_total = call_total(previous_payload)
        if previous_total > 0:
            break
    if previous_total <= 0:
        raise RuntimeError(f"previous stats contain no calls: {previous_payload}")

    for _ in range(3):
        run_client_json(json_address, "1", True, 1)
    first_take = get_json(metrics_address, "/stats?take=1")
    if call_total(first_take) <= 0:
        raise RuntimeError(f"first take contains no calls: {first_take}")

    saw_hello_in_take = any(
        point.get("rpc_name") == "SayHello" and point.get("direction") == "client_to_leaf"
        for point in first_take.get("points", [])
    )
    if not saw_hello_in_take:
        raise RuntimeError(f"first take missing SayHello client_to_leaf metric: {first_take}")

    time.sleep(0.2)
    second_take = get_json(metrics_address, "/stats?take=1")
    for point in second_take.get("points", []):
        if point.get("rpc_name") == "SayHello" and point.get("direction") == "client_to_leaf":
            raise RuntimeError(f"second take still contains SayHello metrics: {point}")
    hello_seen = False
    for _ in range(8):
        run_client_json(json_address, "1", True, 1)
        latest_take = get_json(metrics_address, "/stats?take=1")
        if any(
            point.get("rpc_name") == "SayHello" and point.get("direction") == "client_to_leaf"
            for point in latest_take.get("points", [])
        ):
            hello_seen = True
            break
        time.sleep(0.4)
    if not hello_seen:
        raise RuntimeError("subsequent take never observed a fresh SayHello call")


def execute(arguments, log_directory, processes):
    ports = allocate_ports()
    token = "creek-e2e-token"
    sidecar = arguments.sidecar

    def start(label, command):
        process = ManagedProcess(label, command, log_directory)
        processes.append(process)
        return process

    common = ["--token", token, "--sync-ms", "100", "--metric-period-ms", "1000"]
    node1 = start(
        "node1",
        [
            sidecar, "node",
            "--id", "node-1",
            "--udp", address(ports["node1_udp"]),
            "--metrics", address(ports["node1_metrics"]),
            *common,
        ],
    )
    time.sleep(0.2)
    node2 = start(
        "node2",
        [
            sidecar, "node",
            "--id", "node-2",
            "--udp", address(ports["node2_udp"]),
            "--peer", f"node-1@{address(ports['node1_udp'])}",
            "--metrics", address(ports["node2_metrics"]),
            *common,
        ],
    )
    time.sleep(0.5)

    entry = start(
        "entry_leaf",
        [
            sidecar, "leaf",
            "--id", "entry-leaf",
            "--udp", address(ports["entry_udp"]),
            "--parent", f"node-1@{address(ports['node1_udp'])}",
            "--grpc", address(ports["entry_grpc"]),
            "--json", address(ports["entry_json"]),
            "--metrics", address(ports["entry_metrics"]),
            *common,
        ],
    )
    service1 = start(
        "service_leaf1",
        [
            sidecar, "leaf",
            "--id", "service-leaf-1",
            "--udp", address(ports["service1_udp"]),
            "--parent", f"node-2@{address(ports['node2_udp'])}",
            "--grpc", address(ports["service1_grpc"]),
            "--metrics", address(ports["service1_metrics"]),
            *common,
        ],
    )
    service2 = start(
        "service_leaf2",
        [
            sidecar, "leaf",
            "--id", "service-leaf-2",
            "--udp", address(ports["service2_udp"]),
            "--parent", f"node-1@{address(ports['node1_udp'])}",
            "--grpc", address(ports["service2_grpc"]),
            "--metrics", address(ports["service2_metrics"]),
            *common,
        ],
    )

    wait_tcp(ports["entry_json"], processes)
    wait_tcp(ports["service1_grpc"], processes)
    wait_tcp(ports["service2_grpc"], processes)

    backend1 = start(
        "backend1",
        [
            arguments.hello_server,
            "--id", "backend-1",
            "--listen", address(ports["backend1"]),
            "--leaf", address(ports["service1_grpc"]),
        ],
    )
    backend2 = start(
        "backend2",
        [
            arguments.hello_server,
            "--id", "backend-2",
            "--listen", address(ports["backend2"]),
            "--leaf", address(ports["service2_grpc"]),
        ],
    )
    wait_tcp(ports["backend1"], processes)
    wait_tcp(ports["backend2"], processes)

    json_target = address(ports["entry_json"])
    wait_for_discovery(json_target, processes)

    rows = run_client_json(json_target, "1", True, 10)
    selected = rows[0][0]
    if any(row[0] != selected for row in rows):
        raise RuntimeError(f"sticky sid selected multiple backends: {[row[0] for row in rows]}")

    backends = {"backend-1": backend1, "backend-2": backend2}
    if selected not in backends:
        raise RuntimeError(f"unexpected backend id: {selected}")
    expected = "backend-2" if selected == "backend-1" else "backend-1"
    backends[selected].kill()
    live_processes = [p for p in processes if p.process.poll() is None]
    wait_for_switch(json_target, selected, expected, live_processes)
    verify_stats(address(ports["entry_metrics"]), json_target)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sidecar", required=True)
    parser.add_argument("--hello-server", required=True)
    parser.add_argument("--hello-client", required=False)
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    with tempfile.TemporaryDirectory(prefix="creek-e2e-") as directory:
        processes = []
        try:
            execute(arguments, directory, processes)
        except BaseException:
            for p in processes:
                try:
                    if p.process.poll() is None:
                        sys.stderr.write(f"\n===== {p.label} =====\n")
                        sys.stderr.write(p.output())
                        sys.stderr.write("\n")
                except Exception:
                    pass
            traceback.print_exc()
            return 1
        finally:
            for p in reversed(processes):
                p.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
