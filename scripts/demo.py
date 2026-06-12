"""Reproducible 34/33/33 peer-partition demonstration with fixed-home retries."""
import argparse
import json
from pathlib import Path
import time
from cluster import Cluster, ROOT, run_dir, save
from check_history import check

def demo(out, interactive=False, hold_seconds=0):
    records=[]
    with Cluster(out, faults={}, isolate=0) as cluster:
        print(f"Three C++ server processes: {[p.pid for p in cluster.processes]}",flush=True)
        print("Quota 100; initial rights [34, 33, 33]. Server 0 peer links are blocked.",flush=True)
        print("All three TCP client endpoints remain reachable.",flush=True)
        def request(seq):
            t=time.monotonic_ns()
            response=cluster.clients[0].request(0,seq)
            records.append(dict(rid=3*seq,response=response,start_ns=t,end_ns=time.monotonic_ns()))
            return response
        pending=[]
        for seq in range(40):
            if not request(seq).startswith("OK"):
                pending.append(seq)
        before=cluster.status()
        assert len(pending)==6 and before[0]["accepted_home"]==34
        assert [s["available"] for s in before]==[0,33,33]
        print("40 distinct requests to server 0: 34 accepted, 6 deferred.",flush=True)
        print("Local rights [0, 33, 33]; 66 rights inaccessible to waiting clients.",flush=True)
        assert request(0).startswith("OK DUP")
        print("Retry of accepted request 0:0 -> OK DUP; unique spending remains 34.",flush=True)
        save(Path(out)/"isolated.json",before)
        save(Path(out)/"stage.json",dict(stage="isolated",pids=[p.pid for p in cluster.processes]))
        if interactive:
            input("Press Enter to reconnect server 0 and retry the six deferred IDs. ")
        elif hold_seconds:
            time.sleep(hold_seconds)
        cluster.relay.reconnect()
        deadline=time.monotonic()+5
        while pending:
            pending=[seq for seq in pending if not request(seq).startswith("OK")]
            if time.monotonic()>deadline:
                raise AssertionError("deferred requests did not complete after reconnection")
            if pending:
                time.sleep(.025)
        final=cluster.settle()
        assert sum(final[0]["state"][9:])==40
        assert final[0]["accepted_home"]==40
        print("Reconnected: all 40 distinct requests accepted; 60 quota units remain.",flush=True)
        print(f"Final rights {[s['available'] for s in final]}; three replicas converged; relay queue empty.",flush=True)
        save(Path(out)/"stage.json",dict(stage="reconnected",pids=[p.pid for p in cluster.processes]))
        if interactive:
            input("Press Enter to stop the processes and run the independent checker. ")
        elif hold_seconds:
            time.sleep(hold_seconds)
    with (Path(out)/"client0.jsonl").open("w") as f:
        for e in records:
            f.write(json.dumps(e)+"\n")
    result=check(out)
    save(Path(out)/"check.json",result)
    print(f"Independent Python checker: PASS ({result['unique_spent']} unique spends, conservation and convergence).",flush=True)
    print(f"Evidence: {Path(out).resolve()}",flush=True)
    return result

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path,default=None)
    parser.add_argument("--interactive",action="store_true")
    parser.add_argument("--hold-seconds",type=float,default=0)
    args=parser.parse_args()
    demo(args.out or run_dir(ROOT/"results/demo"),args.interactive,args.hold_seconds)
