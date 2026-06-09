"""Process lifecycle, TCP clients and a bounded, seeded UDP fault relay (stdlib only)."""
from __future__ import annotations
import heapq
import json
import os
from pathlib import Path
import random
import socket
import subprocess
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]

def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")

class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")
        self.bytes = 0

    def command(self, text):
        wire = (text + "\n").encode()
        self.sock.sendall(wire)
        line = self.file.readline(65536)
        if not line.endswith(b"\n"):
            raise RuntimeError("server closed/truncated response")
        self.bytes += len(wire) + len(line)
        return line.decode().strip()

    def request(self, home, seq):
        return self.command(f"REQ {home}:{seq}")

    def close(self):
        self.file.close()
        self.sock.close()

class Relay:
    def __init__(self, base, seed=1, delay_ms=0, jitter_ms=0, duplicate=0):
        self.base = base
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", base+20))
        self.sock.settimeout(.002)
        self.rng = random.Random(seed)
        self.delay = delay_ms/1000
        self.jitter = jitter_ms/1000
        self.duplicate = duplicate
        self.blocked = set()
        self.lock = threading.Lock()
        self.queue = []
        self.serial = 0
        self.live = True
        self.counters = dict(received=0, delivered=0, delivered_bytes=0, dropped=0,
                             duplicates=0, reordered=0, queue_peak=0)
        self.last_delivered = {}
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def isolate(self, node):
        with self.lock:
            self.blocked = {(node, j) for j in range(3) if j != node} | {(j, node) for j in range(3) if j != node}

    def reconnect(self):
        with self.lock:
            self.blocked.clear()

    def stats(self):
        with self.lock:
            return dict(self.counters, queued=len(self.queue))

    def run(self):
        while self.live:
            try:
                data, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                data = None
            except OSError:
                break
            with self.lock:
                if data:
                    try:
                        fields = data.decode().split()
                        source, dest, mid = map(int, fields[1:4])
                        if source not in range(3) or dest not in range(3) or addr != ("127.0.0.1", self.base+10+source):
                            continue
                        self.counters["received"] += 1
                        copies = 1 + int(self.rng.random() < self.duplicate)
                        self.counters["duplicates"] += copies-1
                        for _ in range(copies):
                            if (source, dest) in self.blocked or len(self.queue) >= 10000:
                                self.counters["dropped"] += 1
                            else:
                                self.serial += 1
                                due = time.monotonic() + self.delay + self.rng.uniform(0, self.jitter)
                                heapq.heappush(self.queue, (due, self.serial, source, dest, mid, data))
                                self.counters["queue_peak"] = max(self.counters["queue_peak"], len(self.queue))
                    except (ValueError, IndexError, UnicodeError):
                        self.counters["dropped"] += 1
                now = time.monotonic()
                while self.queue and self.queue[0][0] <= now:
                    _, _, source, dest, mid, data = heapq.heappop(self.queue)
                    if (source, dest) in self.blocked:
                        self.counters["dropped"] += 1
                        continue
                    self.sock.sendto(data, ("127.0.0.1", self.base+10+dest))
                    self.counters["delivered"] += 1
                    self.counters["delivered_bytes"] += len(data)
                    pair = (source, dest)
                    if mid < self.last_delivered.get(pair, 0):
                        self.counters["reordered"] += 1
                    self.last_delivered[pair] = max(mid, self.last_delivered.get(pair, 0))

    def close(self):
        self.live = False
        self.thread.join(timeout=2)
        self.sock.close()

def free_base():
    # Reserve/probe a contiguous range, then release just before process startup.
    # A concurrent unrelated binder can still win the race; fail cleanly in that case.
    for _ in range(100):
        base = random.SystemRandom().randrange(20000, 55000)
        sockets = []
        try:
            for offset, kind in [(0,1),(1,1),(2,1),(10,2),(11,2),(12,2),(20,2)]:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM if kind==1 else socket.SOCK_DGRAM)
                sockets.append(sock)
                sock.bind(("127.0.0.1", base+offset))
            return base
        except OSError:
            pass
        finally:
            for sock in sockets:
                sock.close()
    raise RuntimeError("could not allocate loopback ports")

class Cluster:
    def __init__(self, out, mode="threshold", allocation=(34,33,33), seed=1,
                 faults=None, isolate=None, threshold=8, batch=16, audit=True):
        self.out = Path(out).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        if (self.out/"config.json").exists():
            raise FileExistsError(f"Refusing to overwrite evidence: {self.out}")
        self.base = free_base()
        self.epoch = uuid.uuid4().hex
        self.mode = mode
        self.allocation = tuple(allocation)
        self.processes, self.clients, self.handles = [], [], []
        self.relay = None
        self.finished = False
        self.config = dict(mode=mode, allocation=allocation, seed=seed, faults=faults,
                           initial_isolation=isolate, threshold=threshold, batch=batch,
                           base=self.base, epoch=self.epoch, audit=audit)
        save(self.out/"config.json", self.config)
        try:
            if faults is not None:
                self.relay = Relay(self.base, seed, **faults)
                if isolate is not None:
                    self.relay.isolate(isolate)
            elif isolate is not None:
                raise ValueError("isolation needs relay")
            binary = Path(os.environ.get("QUOTA_SERVER", str(ROOT/"build/quota_server"))).resolve()
            for i in range(3):
                handle = (self.out/f"server{i}.stdout").open("w")
                self.handles.append(handle)
                cmd = [str(binary), "--id", str(i), "--base", str(self.base), "--epoch", self.epoch,
                       "--mode", mode, "--allocation", ",".join(map(str, allocation)),
                       "--threshold", str(threshold), "--batch", str(batch),
                       "--log", str(self.out/f"server{i}.jsonl"), "--audit", str(int(audit))]
                if self.relay:
                    cmd += ["--relay", str(self.base+20)]
                self.processes.append(subprocess.Popen(cmd, stdout=handle, stderr=subprocess.STDOUT))
            for i in range(3):
                deadline = time.monotonic()+5
                while True:
                    if self.processes[i].poll() is not None:
                        raise RuntimeError((self.out/f"server{i}.stdout").read_text())
                    try:
                        self.clients.append(Client(self.base+i))
                        break
                    except OSError:
                        if time.monotonic()>deadline:
                            raise
                        time.sleep(.01)
            self.status()
        except BaseException:
            self.close()
            raise

    def status(self):
        return [json.loads(c.command("STATUS")) for c in self.clients]

    def settle(self):
        if self.relay:
            self.relay.reconnect()
        for c in self.clients:
            c.command("QUIET")
            c.command("PAUSE")
        delay = .03 if not self.relay else self.relay.delay+self.relay.jitter+.04
        time.sleep(delay)
        for _ in range(3):
            for c in self.clients:
                c.command("SYNC")
            time.sleep(delay)
        deadline = time.monotonic()+3
        while self.relay and self.relay.stats()["queued"]:
            if time.monotonic()>deadline:
                raise AssertionError("relay did not drain")
            time.sleep(.01)
        states = self.status()
        if not all(s["state"]==states[0]["state"] for s in states):
            raise AssertionError(f"replicas did not converge: {states}")
        save(self.out/"final.json", states)
        if self.relay:
            save(self.out/"relay.json", self.relay.stats())
        self.finished = True
        return states

    def close(self):
        for c in self.clients:
            c.close()
        self.clients.clear()
        for p in self.processes:
            if p.poll() is None:
                p.terminate()
        for p in self.processes:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for h in self.handles:
            h.close()
        if self.relay:
            self.relay.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

def run_dir(parent):
    return Path(parent)/time.strftime("%Y%m%d-%H%M%S")
