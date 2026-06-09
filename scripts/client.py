"""One command to an existing server, using the same TCP request interface."""
import argparse
from cluster import Client
p=argparse.ArgumentParser(description=__doc__)
p.add_argument("--port",type=int,default=19000)
p.add_argument("command",nargs="+")
a=p.parse_args()
c=Client(a.port)
try:
    print(c.command(" ".join(a.command)))
finally:
    c.close()
