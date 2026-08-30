#!/usr/bin/env python3
# chart/scripts/quota-check.py — reads a `helm template` render on stdin and
# asserts the namespace ResourceQuota fits the actual aggregate resource
# demand of every pod-producing kind at this tier (audit 🟠 tier-quota fit).
# Usage: helm template chart -f values-<tier>.yaml | quota-check.py --tier <label>
import sys, re, argparse
import yaml

def cpu(v):
    if not v: return 0.0
    v = str(v)
    return float(v[:-1]) / 1000 if v.endswith("m") else float(v)

def mem(v):  # -> GiB
    if not v: return 0.0
    v = str(v); m = re.match(r"([0-9.]+)\s*([A-Za-z]*)", v)
    n = float(m.group(1)); u = m.group(2)
    f = {"": 1/2**30, "Ki": 1/2**20, "Mi": 1/1024, "Gi": 1, "Ti": 1024,
         "M": 1e6/2**30, "G": 1e9/2**30, "k": 1e3/2**30}.get(u, 1)
    return n * f

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", required=True)
    args = ap.parse_args()

    rc = rm = lc = lm = pods = 0.0
    quota = {}; lrmax = {}; worst = ("", 0.0, 0.0)
    def add(res, reps=1):
        nonlocal rc, rm, lc, lm, pods
        res = res or {}; rq = res.get("requests") or {}; li = res.get("limits") or {}
        rc += cpu(rq.get("cpu")) * reps; rm += mem(rq.get("memory")) * reps
        lc += cpu(li.get("cpu")) * reps; lm += mem(li.get("memory")) * reps
        pods += reps

    for d in yaml.safe_load_all(sys.stdin):
        if not d: continue
        k = d.get("kind"); spec = d.get("spec") or {}
        name = (d.get("metadata") or {}).get("name", "?")
        if k == "ResourceQuota":
            quota = spec.get("hard", {})
        elif k == "LimitRange":
            for lim in spec.get("limits", []):
                if lim.get("type") == "Container":
                    lrmax = lim.get("max", {})
        elif k in ("Deployment", "StatefulSet"):
            reps = spec.get("replicas", 1) or 1
            pspec = spec.get("template", {}).get("spec", {})
            for c in pspec.get("containers", []):
                add(c.get("resources"), reps)
                li = (c.get("resources") or {}).get("limits") or {}
                cl, ml = cpu(li.get("cpu")), mem(li.get("memory"))
                if cl > worst[1] or ml > worst[2]:
                    worst = (f"{k}/{name}/{c.get('name')}", max(cl, worst[1]), max(ml, worst[2]))
        elif k == "KafkaNodePool":
            add(spec.get("resources"), spec.get("replicas", 1) or 1)
        elif k == "KafkaConnect":
            add(spec.get("resources"), spec.get("replicas", 1) or 1)
        elif k == "Cluster":            # CNPG
            add(spec.get("resources"), spec.get("instances", 1) or 1)
        elif k == "Keycloak":
            add(spec.get("resources"), spec.get("instances", 1) or 1)
        elif k in ("SparkApplication", "ScheduledSparkApplication"):
            base = spec.get("template", spec)
            ex = base.get("executor", {}) or {}
            for role, cnt in [(base.get("driver", {}) or {}, 1),
                              (ex, ex.get("instances", 1) or 1)]:
                c = cpu(role.get("coreLimit") or role.get("cores")); m = mem(role.get("memory"))
                lc += c * cnt; lm += m * cnt; rc += cpu(role.get("cores")) * cnt; rm += m * cnt; pods += cnt

    if not quota:
        print(f"[{args.tier}] FAIL: no ResourceQuota in render"); return 2
    errs = []
    qrc, qrm = cpu(quota.get("requests.cpu")), mem(quota.get("requests.memory"))
    qlc, qlm = cpu(quota.get("limits.cpu")), mem(quota.get("limits.memory"))
    if qrc < rc: errs.append(f"requests.cpu {qrc} < aggregate {rc:.1f}")
    if qrm < rm: errs.append(f"requests.memory {qrm:.0f}Gi < aggregate {rm:.0f}Gi")
    if qlc < lc: errs.append(f"limits.cpu {qlc} < aggregate {lc:.1f}")
    if qlm < lm: errs.append(f"limits.memory {qlm:.0f}Gi < aggregate {lm:.0f}Gi")
    if lrmax:
        mc, mm = cpu(lrmax.get("cpu")), mem(lrmax.get("memory"))
        if worst[1] > mc: errs.append(f"container limit cpu {worst[1]} > limitRange.max {mc} ({worst[0]})")
        if worst[2] > mm: errs.append(f"container limit mem {worst[2]:.0f}Gi > limitRange.max {mm:.0f}Gi ({worst[0]})")
    qpods = quota.get("pods")
    if qpods and pods > float(qpods): errs.append(f"pods {pods:.0f} > quota.pods {qpods}")

    if errs:
        print(f"[{args.tier}] FAIL:")
        for e in errs: print(f"    - {e}")
        return 1
    print(f"[{args.tier}] OK — quota req={qrc}/{qrm:.0f}Gi lim={qlc}/{qlm:.0f}Gi >= agg req={rc:.1f}/{rm:.0f}Gi lim={lc:.1f}/{lm:.0f}Gi (pods {pods:.0f}/{qpods})")
    return 0

if __name__ == "__main__":
    sys.exit(main())
