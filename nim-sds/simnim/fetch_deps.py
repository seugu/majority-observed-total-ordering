"""Clone the dependencies pinned in nimble.lock at their exact commits and write
a --path list (paths.cfg). Used instead of `nimble setup`, which re-downloads Nim.

Usage: python3 fetch_deps.py nimble.lock <out_dir>
"""
import json, os, re, subprocess, sys
lock, out = sys.argv[1], sys.argv[2]
pk = json.load(open(lock))["packages"]
paths = []
for name, v in pk.items():
    if name == "nim":
        continue
    d = os.path.join(out, name)
    if not os.path.isdir(d):
        os.makedirs(d)
        r = lambda *a: subprocess.run(["git", "-C", d, *a], check=True, capture_output=True)
        r("init", "-q"); r("remote", "add", "origin", v["url"])
        r("fetch", "-q", "--depth", "1", "origin", v["vcsRevision"]); r("checkout", "-q", "FETCH_HEAD")
    src = d
    for f in os.listdir(d):
        if f.endswith(".nimble"):
            m = re.search(r'srcDir\s*=\s*"([^"]+)"', open(os.path.join(d, f)).read())
            if m:
                src = os.path.join(d, m.group(1))
    paths.append(src)
    print(name, "ok", src)
open(os.path.join(out, "paths.cfg"), "w").write("".join(f'--path:"{p}"\n' for p in paths))
