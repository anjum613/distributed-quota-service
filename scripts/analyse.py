"""Aggregate retained runs; draw publication-style static plots, no invented data."""
import argparse
import csv
from collections import defaultdict
import json
from pathlib import Path
import statistics
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from cluster import ROOT, save

STYLES={
    "central":dict(color="black",marker="o",linestyle="-",markerfacecolor="white"),
    "fixed":dict(color="black",marker="s",linestyle="--",markerfacecolor="white"),
    "threshold":dict(color="black",marker="^",linestyle=":",markerfacecolor="black"),
}

def quartile(xs,p):
    xs=sorted(xs);at=(len(xs)-1)*p;k=int(at)
    return xs[k]+(xs[min(k+1,len(xs)-1)]-xs[k])*(at-k)

def analyse(directories,out):
    out.mkdir(parents=True,exist_ok=True)
    rows=[]
    for directory in directories:
        with (directory/"runs.csv").open() as f:
            for row in csv.DictReader(f):
                for k,v in list(row.items()):
                    if k not in ("mode","scenario","input_sha256"):
                        row[k]=float(v) if v else None
                rows.append(row)
    groups=defaultdict(list)
    for row in rows:
        groups[(row["scenario"],row["delay_ms"],int(row["n"]),row["mode"])].append(row)
    summaries=[]
    for (scenario,delay,n,mode),runs in sorted(groups.items()):
        summary=dict(scenario=scenario,delay_ms=delay,n=n,mode=mode,repeats=len(runs))
        for metric in ("elapsed_s","successful_per_s","successes","deferrals","attempt_p50_ms","attempt_p95_ms","attempt_p99_ms",
                       "admission_p95_ms","peer_messages","peer_bytes","stranded_rights","unused_rights","uncompleted"):
            values=[r[metric] for r in runs if r[metric] is not None]
            for name,value in (("median",statistics.median(values)),("q1",quartile(values,.25)),
                               ("q3",quartile(values,.75)),("min",min(values)),("max",max(values))):
                summary[f"{metric}_{name}"]=value
        summaries.append(summary)
    save(out/"summary.json",summaries)
    with (out/"summary.csv").open("w",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=summaries[0].keys());writer.writeheader();writer.writerows(summaries)
    ratios=[]
    for scenario,delay,n,mode in groups:
        if scenario!="completion" or mode=="central":
            continue
        central={r["seed"]:r for r in groups[scenario,delay,n,"central"]}
        values=[]
        for r in groups[scenario,delay,n,mode]:
            b=central[r["seed"]]
            assert r["successes"]==b["successes"]==n and r["input_sha256"]==b["input_sha256"]
            values.append(b["elapsed_s"]/r["elapsed_s"])
        ratios.append(dict(mode=mode,n=n,delay_ms=delay,paired_speedup_median=statistics.median(values),
                           min=min(values),max=max(values),q1=quartile(values,.25),q3=quartile(values,.75)))
    save(out/"paired_speedup.json",ratios)
    plt.rcParams.update({"font.size":10,"axes.spines.top":False,"axes.spines.right":False,
                         "savefig.dpi":180,"font.family":"DejaVu Serif"})
    for scenario,delay in sorted({(s["scenario"],s["delay_ms"]) for s in summaries}):
        selected=[s for s in summaries if s["scenario"]==scenario and s["delay_ms"]==delay]
        sizes=sorted({s["n"] for s in selected})
        metrics=(("elapsed_s","Completion time (s)"),("attempt_p95_ms","Median per-run p95 attempt latency (ms)")) if scenario=="completion" else (("successes","Unique admitted requests"),("stranded_rights","Stranded rights at observation end"))
        fig,axes=plt.subplots(1,2,figsize=(10.2,3.7),layout="constrained")
        for ax,(metric,label) in zip(axes,metrics):
            for mode,style in STYLES.items():
                points=sorted([s for s in selected if s["mode"]==mode],key=lambda s:s["n"])
                median=[s[f"{metric}_median"] for s in points]
                low=[s[f"{metric}_median"]-s[f"{metric}_q1"] for s in points]
                high=[s[f"{metric}_q3"]-s[f"{metric}_median"] for s in points]
                ax.errorbar([s["n"] for s in points],median,yerr=[low,high],label=mode,capsize=4,**style)
            ax.set_xlabel("Distinct requests")
            ax.set_ylabel(label)
            ax.set_xticks(sizes,[f"{n:,}" for n in sizes])
            ax.grid(axis="y",alpha=.2)
            ax.set_ylim(bottom=0)
        axes[0].legend(frameon=False)
        label="Normal loopback" if not delay and scenario!="partition" else ("Peer partition; client links available" if scenario=="partition" else f"Emulated {delay:g} ms one-way peer delay")
        fig.suptitle(f"{label} - median and interquartile range",fontsize=11)
        stem=f"{scenario}_{delay:g}ms"
        fig.savefig(out/f"{stem}.png");plt.close(fig)
    # Show all repeated observations, including early slow runs. Do not hide drift.
    base=[r for r in rows if r["scenario"]=="completion" and r["delay_ms"]==0]
    if base:
        fig,ax=plt.subplots(figsize=(8,3),layout="constrained")
        for mode,style in STYLES.items():
            points=[(i+1,r["successful_per_s"]) for i,r in enumerate(base) if r["mode"]==mode]
            ax.scatter([p[0] for p in points],[p[1] for p in points],label=mode,
                       marker=style["marker"],facecolors=style["markerfacecolor"],edgecolors="black",s=25)
        ax.set_xlabel("Run order (all sizes interleaved)");ax.set_ylabel("Successful requests / s")
        ax.legend(frameon=False,ncol=3);ax.grid(alpha=.2)
        fig.savefig(out/"run_order.png");plt.close(fig)
    print(f"Analysed {len(rows)} runs; wrote {len(summaries)} groups to {out}")

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("directories",type=Path,nargs="*",default=[ROOT/"results/completion",ROOT/"results/scarcity",ROOT/"results/delay"])
    p.add_argument("--out",type=Path,default=ROOT/"results/analysis")
    a=p.parse_args();analyse(a.directories,a.out)
