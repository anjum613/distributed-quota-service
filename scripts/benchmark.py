"""Paired, interleaved completion and scarcity experiments. Raw attempts retained."""
import argparse
import concurrent.futures
import csv
import hashlib
import json
from pathlib import Path
import random
import threading
import time
from cluster import Cluster, ROOT, run_dir, save
from check_history import check

MODES=("central","fixed","threshold")

def percentile(values, pct):
    values=sorted(values)
    if not values:
        return None
    x=(len(values)-1)*pct/100
    lo=int(x)
    return values[lo]+(values[min(lo+1,len(values)-1)]-values[lo])*(x-lo)

def workload(out, mode, n, seed, scenario="completion", delay_ms=0):
    # Exactly n home-routed requests, with independent deterministic per-home ID order.
    homes=([n//3+(i<n%3) for i in range(3)] if scenario=="completion" else [n,0,0])
    quota=2*n if scenario=="completion" else n
    alloc=([quota,0,0] if mode=="central" else [quota//3+(i<quota%3) for i in range(3)])
    rng=random.Random(seed)
    inputs=[]
    for home,count in enumerate(homes):
        ids=list(range(count));rng.shuffle(ids);inputs.append(ids)
    digest=hashlib.sha256(json.dumps(inputs).encode()).hexdigest()
    # Partition experiments isolate hotspot server 0. It is also the owner in the
    # central baseline; this is favourable to that baseline and explicitly reported.
    faults=({"delay_ms":delay_ms} if delay_ms or scenario=="partition" else None)
    with Cluster(out,mode,alloc,seed,faults=faults,isolate=0 if scenario=="partition" else None) as c:
        # Warm the exact connection / parsing path with STATUS (no quota consumption).
        for _ in range(50):
            c.status()
        start_metrics=c.status()
        barrier=threading.Barrier(4)
        records=[[] for _ in range(3)]
        def worker(home):
            client=c.clients[home]
            barrier.wait()
            for seq in inputs[home]:
                start=time.monotonic_ns()
                response=client.request(home,seq)
                records[home].append(dict(rid=seq*3+home,phase="initial",start_ns=start,
                                          end_ns=time.monotonic_ns(),response=response))
            return time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            futures=[pool.submit(worker,h) for h in range(3)]
            start_ns=time.monotonic_ns()
            barrier.wait()
            finish_ns=max(f.result() for f in futures)
        # Scarcity: one initial pass, followed by a fixed retry window. All modes
        # have the same opportunity window, even if one cannot borrow at all.
        window=.5 if scenario!="completion" else 0
        deadline=time.monotonic()+window
        while window and time.monotonic()<deadline:
            seen={e["rid"] for es in records for e in es if e["response"].startswith("OK")}
            for home,ids in enumerate(inputs):
                for seq in ids:
                    rid=seq*3+home
                    if rid in seen:
                        continue
                    start=time.monotonic_ns()
                    response=c.clients[home].request(home,seq)
                    records[home].append(dict(rid=rid,phase="retry",start_ns=start,end_ns=time.monotonic_ns(),response=response))
            time.sleep(.01)
        if window:
            finish_ns=time.monotonic_ns()
        finish_metrics=c.status()
        before_reconnect=finish_metrics
        all_records=[e for es in records for e in es]
        first,success={},{}
        for e in sorted(all_records,key=lambda e:e["start_ns"]):
            first.setdefault(e["rid"],e["start_ns"])
            if e["response"].startswith("OK"):
                success.setdefault(e["rid"],e["end_ns"])
        if scenario=="completion":
            assert len(success)==n,"incomplete work cannot be used for speedup"
        elapsed=(finish_ns-start_ns)/1e9
        waiting={rid%3 for rid in first if rid not in success}
        # Rights at other replicas are stranded for currently deferred homes. If
        # there is no waiting client, unused capacity is NOT labelled stranded.
        inaccessible=sum(s["available"] for s in finish_metrics if s["node"] not in waiting) if waiting else 0
        if mode=="central":
            inaccessible=0 # hotspot is owner; no distributed balance ambiguity here
        latency=[(e["end_ns"]-e["start_ns"])/1e6 for e in all_records]
        admission=[(end-first[rid])/1e6 for rid,end in success.items()]
        summary=dict(mode=mode,n=n,seed=seed,scenario=scenario,delay_ms=delay_ms,input_sha256=digest,
                     successes=len(success),uncompleted=n-len(success),attempts=len(all_records),
                     deferrals=sum(e["response"].startswith("DEFER") for e in all_records),
                     elapsed_s=elapsed,successful_per_s=len(success)/elapsed,
                     attempt_p50_ms=percentile(latency,50),attempt_p95_ms=percentile(latency,95),attempt_p99_ms=percentile(latency,99),
                     admission_p50_ms=percentile(admission,50),admission_p95_ms=percentile(admission,95),
                     peer_messages=sum(e["peer_tx"]-b["peer_tx"] for e,b in zip(finish_metrics,start_metrics)),
                     peer_bytes=sum(e["peer_bytes"]-b["peer_bytes"] for e,b in zip(finish_metrics,start_metrics)),
                     stranded_rights=inaccessible,unused_rights=quota-len(success),
                     observation_window_s=window)
        save(Path(out)/"observed.json",before_reconnect)
        save(Path(out)/"inputs.json",inputs)
        if c.relay:
            save(Path(out)/"relay_observed.json",c.relay.stats())
        c.settle()
    for home,entries in enumerate(records):
        (Path(out)/f"client{home}.jsonl").write_text(''.join(json.dumps(e)+'\n' for e in entries))
    save(Path(out)/"check.json",check(out))
    save(Path(out)/"summary.json",summary)
    return summary

def run(out,sizes,repeats,scenarios,delay_ms=0):
    out.mkdir(parents=True,exist_ok=True)
    if (out/"plan.json").exists():
        raise FileExistsError("choose a fresh benchmark output directory")
    save(out/"plan.json",dict(sizes=sizes,repeats=repeats,scenarios=scenarios,delay_ms=delay_ms,
                              seeds=[101+17*r for r in range(repeats)],order="Latin rotation by repetition and size"))
    rows=[]
    # Discard a successful warm-up workload for each mode; keep its raw evidence.
    for mode in MODES:
        workload(out/f"warmup_{mode}",mode,300,901,"completion",delay_ms)
    for rep in range(repeats):
        seed=101+17*rep
        for size_index,n in enumerate(sizes):
            for scenario in scenarios:
                rotation=(rep+size_index)%3
                order=MODES[rotation:]+MODES[:rotation]
                for mode in order:
                    path=out/f"{scenario}_{n}_{seed}_{mode}"
                    row=workload(path,mode,n,seed,scenario,delay_ms)
                    rows.append(row)
                    print(f"{scenario:10s} n={n:7d} seed={seed} {mode:9s} accepted={row['successes']:7d} time={row['elapsed_s']:.3f}s p95={row['attempt_p95_ms']:.3f}ms",flush=True)
                    with (out/"runs.csv").open("w",newline="") as f:
                        writer=csv.DictWriter(f,fieldnames=rows[0].keys());writer.writeheader();writer.writerows(rows)
    return rows

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path)
    parser.add_argument("--sizes",type=int,nargs="+",default=[30000,90000,270000])
    parser.add_argument("--repeats",type=int,default=5)
    parser.add_argument("--scenarios",nargs="+",choices=["completion","hotspot","partition"],default=["completion"])
    parser.add_argument("--delay-ms",type=float,default=0)
    args=parser.parse_args()
    if min(args.sizes)<3 or max(args.sizes)>1000000 or not 1<=args.repeats<=20 or not 0<=args.delay_ms<=30:
        parser.error("sizes 3..1000000; repeats 1..20; delay 0..30ms")
    run(args.out or run_dir(ROOT/"results/benchmarks"),args.sizes,args.repeats,args.scenarios,args.delay_ms)
