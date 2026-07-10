"""Record the actual execution environment without dumping environment secrets."""
import json
from pathlib import Path
import platform
import subprocess
from cluster import ROOT, save

def capture():
    commands={"uname":["uname","-a"],"cpu":["lscpu"],"memory":["free","-b"],
              "compiler":["g++","--version"],"make":["make","--version"]}
    result={name:subprocess.check_output(cmd,text=True) for name,cmd in commands.items()}
    result.update(python=platform.python_version(),os_release=Path("/etc/os-release").read_text(),
                  execution="one physical computer, three local C++ processes; loopback networking; no CPU affinity")
    result["topology"]=[]
    for cpu in sorted(Path("/sys/devices/system/cpu").glob("cpu[0-9]*")):
        row={"cpu":cpu.name}
        for name in ("topology/core_id","cpufreq/cpuinfo_max_freq","topology/thread_siblings_list"):
            if (cpu/name).exists():
                row[name]=(cpu/name).read_text().strip()
        result["topology"].append(row)
    save(ROOT/"results/machine.json",result)
    print("Saved results/machine.json")

if __name__=="__main__":
    capture()
