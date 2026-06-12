"""Real-process correctness scenarios, fault schedules and checker mutation tests."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import random
import shutil
import tempfile
import time
from cluster import Client, Cluster, ROOT, run_dir, save
from check_history import check
from demo import demo

def checked(out):
    result=check(out)
    save(out/"check.json",result)
    print(f"PASS {out.name}: {result['unique_spent']} spent, converged",flush=True)
    return result

def run(root):
    root.mkdir(parents=True,exist_ok=True)
    results={}
    results["partition_demo"]=demo(root/"partition_demo")
    for mode in ("central","fixed","threshold"):
        out=root/f"dedup_{mode}"
        with Cluster(out,mode,allocation=(30,0,0) if mode=="central" else (10,10,10)) as c:
            for bad in ("REQ 1:0","REQ -1:0","REQ 0:-1","REQ 0:00","REQ 0:1000000000001","TRANSFER 3 1","REQ 0:1 extra"):
                assert c.clients[0].command(bad)=="ERR INPUT"
            def repeat(_):
                client=Client(c.base)
                try:
                    return client.request(0,99)
                finally:
                    client.close()
            with concurrent.futures.ThreadPoolExecutor(8) as pool:
                replies=list(pool.map(repeat,range(24)))
            assert sum(r.startswith("OK NEW") for r in replies)==1
            assert sum(r.startswith("OK DUP") for r in replies)==23
            # Remote home additionally exercises central-owner RPC dedup.
            assert c.clients[1].request(1,88).startswith("OK NEW")
            assert c.clients[1].request(1,88).startswith("OK DUP")
            states=c.settle()
            assert sum(states[0]["state"][9:])==2
        results[out.name]=checked(out)

    out=root/"transfer_in_flight"
    with Cluster(out,faults=dict(delay_ms=30,jitter_ms=30,duplicate=1)) as c:
        for client in c.clients:
            client.command("QUIET")
        assert c.clients[1].command("TRANSFER 0 7")=="OK TRANSFER"
        assert c.clients[1].command("TRANSFER 0 999")=="DEFER NO_RIGHTS"
        first=c.status()
        assert first[1]["available"]==26
        assert first[0]["available"]==34
        states=c.settle()
        assert [s["available"] for s in states]==[41,26,33]
    results[out.name]=checked(out)
    assert json.loads((out/"relay.json").read_text())["duplicates"]>0

    for mode in ("central","fixed","threshold"):
        for seed in (7,19,41):
            out=root/f"faults_{mode}_{seed}"
            allocation=(75,0,0) if mode=="central" else (25,25,25)
            rng=random.Random(seed)
            homes=[rng.choices(range(3),weights=[6,2,2])[0] for _ in range(120)]
            ids=[3*k+home for k,home in enumerate(homes)]
            save(out.parent/(out.name+"_input.json"),ids)
            with Cluster(out,mode,allocation,seed,faults=dict(delay_ms=2,jitter_ms=15,duplicate=.5),isolate=0) as c:
                pending=[]
                records=[[] for _ in range(3)]
                for k,rid in enumerate(ids):
                    if k==45:
                        c.relay.reconnect()
                    if k==80:
                        c.relay.isolate(2)
                    home=rid%3
                    result=c.clients[home].request(home,rid//3)
                    records[home].append(dict(rid=rid,response=result))
                    if not result.startswith("OK"):
                        pending.append(rid)
                    elif rng.random()<.3:
                        assert c.clients[home].request(home,rid//3).startswith("OK DUP")
                c.relay.reconnect()
                deadline=time.monotonic()+1.5
                while pending and time.monotonic()<deadline:
                    retry=[]
                    for rid in pending:
                        result=c.clients[rid%3].request(rid%3,rid//3)
                        records[rid%3].append(dict(rid=rid,response=result))
                        if not result.startswith("OK"):
                            retry.append(rid)
                    pending=retry
                    time.sleep(.02)
                states=c.settle()
                assert sum(states[0]["state"][9:])<=75
                if mode in ("central","threshold"):
                    assert sum(states[0]["state"][9:])==75
            for home,entries in enumerate(records):
                (out/f"client{home}.jsonl").write_text(''.join(json.dumps(e)+'\n' for e in entries))
            results[out.name]=checked(out)
            stats=json.loads((out/"relay.json").read_text())
            assert stats["duplicates"]>0 and stats["dropped"]>0
            # Fixed mode has only periodic state traffic: short jitter need not
            # reorder it. Check actual reordering coverage across the suite.

    # An owner may commit before its response is delayed beyond a home timeout.
    out=root/"lost_owner_answer"
    with Cluster(out,"central",(10,0,0),faults=dict(delay_ms=65,jitter_ms=0,duplicate=1)) as c:
        first=c.clients[1].request(1,123)
        assert first.startswith("DEFER OWNER_TIMEOUT")
        second=c.clients[1].request(1,123)
        assert second.startswith("OK")
        assert c.clients[1].request(1,123).startswith("OK DUP")
        state=c.settle()
        assert sum(state[0]["state"][9:])==1
    results[out.name]=checked(out)

    # Corrupt copied evidence; checker must reject both unsafe spend and forged state.
    rejected=0
    with tempfile.TemporaryDirectory(prefix="quota-checker-") as temp:
        for mutation in ("extra_spend","forged_snapshot","missing_send"):
            dest=Path(temp)/mutation
            shutil.copytree(root/"partition_demo",dest)
            path=dest/"server0.jsonl"
            entries=[json.loads(line) for line in path.read_text().splitlines()]
            if mutation=="extra_spend":
                e=next(e for e in entries if e["kind"]=="reply" and e["result"]=="OK DUP")
                e["kind"]="spend"
            elif mutation=="forged_snapshot":
                e=next(e for e in entries if e["kind"]=="snapshot")
                e["state"][9]+=1
            else:
                e=next(e for e in entries if e["kind"]=="send" and e["payload"].startswith("STATE"))
                e["payload"]="STATE "+" ".join(["0"]*12)
            path.write_text(''.join(json.dumps(e)+'\n' for e in entries))
            try:
                check(dest)
            except AssertionError:
                rejected+=1
            else:
                raise AssertionError(f"checker accepted mutation {mutation}")
    assert sum(json.loads(p.read_text())["reordered"] for p in root.glob("*/relay.json"))>0
    results["checker_mutations_rejected"]=rejected
    save(root/"summary.json",results)
    print(f"PASS: {len(results)-1} process scenarios; {rejected} invalid histories rejected. Evidence: {root}",flush=True)

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out",type=Path)
    args=parser.parse_args()
    run(args.out or run_dir(ROOT/"results/correctness"))
