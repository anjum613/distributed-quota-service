"""Streaming genuine admission workload. No faults, injected delays or worker sleeps."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import threading
import time
from cluster import Cluster, ROOT, run_dir, save

def rss_kib(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except FileNotFoundError:
        pass
    return 0

def run(out, seconds, target):
    allocation=[target//3+(i<target%3) for i in range(3)]
    counts=[0,0,0]
    peaks=[0,0,0]
    progress=[]
    barrier=threading.Barrier(4)
    stop=threading.Event()
    deadline=0
    with Cluster(out,"fixed",allocation,audit=False) as c:
        print(f"Target: {target:,} distinct admissions; waiting limit: {seconds:.1f} s.",flush=True)
        print("Three servers, normal loopback, no communication failures or injected delay.",flush=True)
        def worker(home):
            barrier.wait()
            done=0
            while done<allocation[home] and time.monotonic()<deadline and not stop.is_set():
                response=c.clients[home].request(home,done)
                if not response.startswith("OK NEW"):
                    stop.set()
                    raise AssertionError(response)
                done+=1
                counts[home]=done
            return done
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            futures=[pool.submit(worker,i) for i in range(3)]
            start=time.monotonic()
            deadline=start+seconds
            barrier.wait()
            while not all(f.done() for f in futures):
                elapsed=time.monotonic()-start
                memory=[rss_kib(p.pid) for p in c.processes]
                peaks=[max(a,b) for a,b in zip(peaks,memory)]
                progress.append(dict(elapsed_s=elapsed,completed=sum(counts),server_rss_kib=memory))
                # Monitoring only; the three workload threads never sleep.
                time.sleep(.5)
            completed=sum(f.result() for f in futures)
            elapsed=time.monotonic()-start
        states=c.settle()
        spent=sum(states[0]["state"][9:])
        assert spent==completed and spent<=target
        result=dict(target_requests=target,waiting_limit_s=seconds,elapsed_s=elapsed,
                    completed=completed,remaining=target-completed,
                    termination_reason="completed" if completed==target else "DNF: declared wall-clock waiting limit",
                    mode="fixed",network="normal loopback; no faults or sleeps in workers",
                    peak_sampled_server_rss_kib=peaks,client_counts=counts,quota=target,
                    audit="snapshot only, no full history verification for stress",
                    memory_bound="streamed IDs, no request list; <=2,000,000 retained successful IDs per server; 128 TCP connections and bounded buffers",
                    final_spending_matches_client_count=True)
    save(Path(out)/"progress.json",progress)
    save(Path(out)/"summary.json",result)
    print(json.dumps(result,indent=2),flush=True)
    return result

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path)
    parser.add_argument("--seconds",type=float,default=30)
    parser.add_argument("--target",type=int,default=1000000000)
    args=parser.parse_args()
    if not 1<=args.seconds<=3600 or not 3<=args.target<=1000000000000:
        parser.error("seconds 1..3600; target 3..10^12")
    run(args.out or run_dir(ROOT/"results/stress"),args.seconds,args.target)
