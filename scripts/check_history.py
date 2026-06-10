"""Independent offline audit: Python integers, event replay and message provenance.

Imports no C++ wrapper, counter module, cluster or production correctness function.
Requires complete audited logs from a graceful, quiescent run on one host.
"""
import argparse
import heapq
import json
from pathlib import Path

def require(ok, why):
    if not ok:
        raise AssertionError(why)

def balance(state, replica):
    incoming = sum(state[3*j+replica] for j in range(3) if j != replica)
    outgoing = sum(state[3*replica+j] for j in range(3) if j != replica)
    return state[4*replica] + incoming - outgoing - state[9+replica]

def events(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)

def check(directory, require_convergence=True):
    directory = Path(directory)
    cfg = json.loads((directory/"config.json").read_text())
    require(cfg["audit"], "full audit was disabled; history cannot be verified")
    initial = [0]*12
    for i, n in enumerate(cfg["allocation"]):
        initial[4*i] = n
    quota = sum(cfg["allocation"])
    views = [initial.copy() for _ in range(3)]
    seq = [0]*3
    stopped = set()
    sent, received = {}, {}
    spent_ids, home_ids = set(), set()
    count = 0
    streams = [events(directory/f"server{i}.jsonl") for i in range(3)]
    # CLOCK_MONOTONIC on this single host provides a common order. Do not reuse
    # this timestamp ordering assumption unchanged for physical-cluster logs.
    for event in heapq.merge(*streams, key=lambda e: (e["ns"], e["node"], e["seq"])):
        count += 1
        i, kind = event["node"], event["kind"]
        require(i in range(3), "invalid replica")
        require(i not in stopped, "event after stop")
        seq[i] += 1
        require(seq[i]==event["seq"], "missing or reordered audit entry")
        s = views[i]
        if kind=="init":
            require(seq[i]==1 and event["state"]==initial, "invalid initial state")
            require(event["epoch"]==cfg["epoch"] and event["mode"]==cfg["mode"], "configuration mismatch")
            require(event["audit"], "incomplete audit")
        else:
            require(seq[i]>1, "missing init")
        if kind=="spend":
            rid = event["rid"]
            require(rid not in spent_ids, f"request spent twice: {rid}")
            require((cfg["mode"]=="central" and i==0) or (cfg["mode"]!="central" and rid%3==i), "spending at wrong replica")
            require(balance(s,i)>=1, "spend without locally visible rights")
            s[9+i] += 1
            spent_ids.add(rid)
            require(len(spent_ids)<=quota, "global quota exceeded")
        elif kind=="transfer":
            dest, n = event["to"], event["amount"]
            require(cfg["mode"]=="threshold", "transfer in non-borrowing mode")
            require(dest in range(3) and dest!=i and n>0 and balance(s,i)>=n, "unsafe transfer")
            s[3*i+dest] += n
            require(s[3*i+dest]<2**64, "transfer arithmetic overflow")
        elif kind=="send":
            key = (i,event["mid"])
            require(key not in sent, "reused message identity")
            payload = event["payload"].split()
            if payload[0]=="STATE":
                require(list(map(int,payload[1:]))==s, "published state differs from replay")
            if payload[0]=="ANSWER" and payload[2]=="OK":
                home, number = map(int,payload[1].split(':'))
                require(3*number+home in spent_ids, "owner sent unearned success")
            sent[key] = (event["to"],event["payload"])
        elif kind=="recv":
            key = (event["from"],event["mid"])
            require(sent.get(key)==(i,event["payload"]), "received message lacks authentic prior send")
            received[(i,*key)] = event["payload"]
        elif kind=="merge":
            payload = received.get((i,event["from"],event["mid"]), "").split()
            require(len(payload)==13 and payload[0]=="STATE", "merge lacks state receive")
            remote = list(map(int,payload[1:]))
            for k in [3*i,3*i+1,3*i+2,9+i]:
                require(remote[k]<=s[k], "peer advanced receiver-owned component")
            views[i] = s = [max(a,b) for a,b in zip(s,remote)]
        elif kind=="reply":
            rid, result = event["rid"],event["result"]
            require(rid%3==i, "reply from wrong home")
            if result.startswith("OK"):
                require(rid in spent_ids, "acknowledgement without spending")
                if result=="OK NEW":
                    require(rid not in home_ids, "duplicate counted as new")
                    home_ids.add(rid)
                elif result=="OK DUP":
                    require(rid in home_ids, "DUP before NEW")
                else:
                    require(False,"invalid success result")
            else:
                require(result.startswith("DEFER "), "unexpected reply")
        elif kind in ("snapshot","stop"):
            require(event["state"]==s, "snapshot differs from independent replay")
            if kind=="stop":
                stopped.add(i)
        elif kind not in ("init","invalid_peer"):
            require(False,f"unknown audit event {kind}")
        require(all(balance(s,j)>=0 for j in range(3)), "negative rights in a view")
        require(sum(s[9:])<=quota, "view exceeds quota")
        # Global ownership computed independently from each single writer's row.
        global_state = initial.copy()
        for j in range(3):
            global_state[3*j:3*j+3] = views[j][3*j:3*j+3]
            global_state[9+j] = views[j][9+j]
        require(sum(global_state[9:])==len(spent_ids), "spend records disagree with writers")
        require(all(balance(global_state,j)>=0 for j in range(3)), "global over-allocation")
        require(sum(balance(global_state,j) for j in range(3))+len(spent_ids)==quota, "conservation failed")
    require(stopped=={0,1,2}, "missing graceful final logs")
    converged = all(v==views[0] for v in views)
    if require_convergence:
        require(converged, "replicas failed to converge")
        final = json.loads((directory/"final.json").read_text())
        require([x["state"] for x in final]==views, "final snapshot mismatch")
        if (directory/"relay.json").exists():
            require(json.loads((directory/"relay.json").read_text())["queued"]==0,"outstanding relay messages")
    # Check client-observed successes too, if supplied. A lost reply can make the
    # spending set larger; it must never make acknowledgements larger.
    observations = 0
    for path in directory.glob("client*.jsonl"):
        for e in events(path):
            if e["response"].startswith("OK "):
                require(e["rid"] in home_ids,"client success missing from server history")
                observations += 1
    return dict(passed=True, events=count, quota=quota, unique_spent=len(spent_ids),
                acknowledged=len(home_ids), client_success_observations=observations,
                conserved=True, converged=converged)

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    parser.add_argument("--allow-divergence",action="store_true")
    args=parser.parse_args()
    print(json.dumps(check(args.directory,not args.allow_divergence),indent=2))
