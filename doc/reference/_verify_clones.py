#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""独立验证 git.md 中的项目是否都已正确 clone 到本目录。
按 git remote 归一化匹配（https / git@ 互转），不依赖目录命名。"""
import os, re, subprocess, json, datetime

BASE = os.path.dirname(os.path.abspath(__file__))
GIT_MD = os.path.abspath(os.path.join(BASE, "..", "..", "..", "refrences", "git.md"))

def norm(url):
    if not url:
        return ""
    u = url.strip()
    u = re.sub(r"\.git$", "", u)
    u = u.replace("git@github.com:", "https://github.com/")
    u = u.rstrip("/").lower()
    return u

def parse():
    with open(GIT_MD, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    entries, cur = [], {}
    for raw in lines:
        line = raw.strip()
        if not line:
            if cur: entries.append(cur); cur = {}
            continue
        if line.startswith("https://"):
            if cur.get("https"): entries.append(cur); cur = {}
            cur["https"] = line
        elif line.startswith("git@"):
            cur["ssh"] = line
    if cur: entries.append(cur)
    return entries

def git(*args, cwd):
    try:
        r = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, timeout=30)
        return r.returncode, r.stdout.strip()
    except Exception as e:
        return 1, str(e)

def main():
    entries = parse()
    # 枚举已 clone 的仓库
    cloned = {}  # norm_remote -> (dir, head)
    for name in sorted(os.listdir(BASE)):
        d = os.path.join(BASE, name)
        if not os.path.isdir(d) or not os.path.isdir(os.path.join(d, ".git")):
            continue
        rc, remote = git("config", "remote.origin.url", cwd=d)
        if rc != 0 or not remote:
            # 可能无 origin，跳过远程匹配
            cloned.setdefault("__noremote__:" + name, (name, "?"))
            continue
        rcn, head = git("rev-parse", "HEAD", cwd=d)
        cloned[norm(remote)] = (name, head[:12] if rcn == 0 else "?")

    verified, missing = [], []
    for e in entries:
        nh, ns = norm(e.get("https", "")), norm(e.get("ssh", ""))
        hit = None
        for key in (nh, ns):
            if key and key in cloned:
                hit = cloned.pop(key)  # 命中后从池移除，便于发现多余
                break
        if hit:
            verified.append({"name": hit[0], "head": hit[1],
                             "https": e.get("https", ""), "ssh": e.get("ssh", "")})
        else:
            missing.append({"https": e.get("https", ""), "ssh": e.get("ssh", "")})

    extra = [{"dir": v[0], "head": v[1]} for k, v in cloned.items()]  # 未被匹配的已 clone 仓库

    # 输出
    md = (f"# Clone 验证报告\n\n"
          f"- 生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
          f"- git.md 项目数：{len(entries)}\n"
          f"- ✅ 已验证（远程匹配）：{len(verified)}\n"
          f"- ❌ 缺失/未验证：{len(missing)}\n"
          f"- ⚠️ 多余（doc/reference 下有但 git.md 未列）：{len(extra)}\n\n")
    md += "## 一、已验证成功（远程地址匹配）\n\n"
    for r in verified:
        md += f"- `{r['name']}` (HEAD `{r['head']}`) — {r['https']}\n"
    md += "\n## 二、缺失 / 未验证\n\n"
    if missing:
        for r in missing:
            md += f"- https: `{r['https']}` / ssh: `{r['ssh']}`\n"
    else:
        md += "_（无）_\n"
    md += "\n## 三、多余仓库（不在 git.md 中）\n\n"
    if extra:
        for r in extra:
            md += f"- `{r['dir']}` (HEAD `{r['head']}`)\n"
    else:
        md += "_（无）_\n"

    out_md = os.path.join(BASE, "_CLONE_VERIFY.md")
    out_json = os.path.join(BASE, "_CLONE_VERIFY.json")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"total": len(entries), "verified": len(verified),
                   "missing": len(missing), "extra": len(extra),
                   "verified": verified, "missing": missing, "extra": extra},
                  f, ensure_ascii=False, indent=2)
    print(f"total={len(entries)} verified={len(verified)} missing={len(missing)} extra={len(extra)}")
    if missing:
        print("MISSING:")
        for r in missing: print("  ", r["https"], r["ssh"])
    if extra:
        print("EXTRA:")
        for r in extra: print("  ", r["dir"], r["head"])

if __name__ == "__main__":
    main()
