# Distributed Quota Service

Three C++ servers share one quota. Each request spends one unit, and the servers have to keep the total correct even when messages are late or a link goes down.

I tried three ways to manage the quota: one central server, a fixed split, and a version where servers can pass unused quota to each other. The demo cuts a link, retries requests, reconnects the servers, and checks that the numbers still add up.

## Try it

You need Linux, a C++20 compiler, Make, and Python 3.10 or newer.

```bash
make
make test
python3 scripts/demo.py
```

Logs and test results go in `results/`. The [design notes](docs/design.md) explain how it works.
