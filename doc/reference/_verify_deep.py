#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""内容级验证：不止查目录存在，还验证每个仓库的工作树内容。
检查项：
  1) .git 是否存在、HEAD 是否可解析
  2) 磁盘实际文件数 / 已跟踪文件数（是否被检出）
  3) git status 中是否有缺失(D)文件（检出失败）
  4) 是否残留 Git LFS 指针文件（内容未拉取）
  5) 本地 HEAD 是否等于远端 HEAD（联网，走代理）
"""
import os, re, subprocess, json, datetime
from concurrent.futures import ThreadPoolExecutor

BASE = os.path.dirname(os.path.abspath(__file__))
GIT_MD = os.path.abspath(os.path.join(BASE, "..", "..", "..", "refrences", "git.md"))
PROXY = "http://127.0.0.1:7897"
MAX_SCAN_FILES = 30000

def norm(url):
    if not url: return ""
    u = url.strip()
    u = re.sub(r"\.git$", "", u).replace("git@github.com:", "https://github.com/")
    return u.rstrip("/").lower()

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

def git(args, cwd, timeout=60):
    try:
        r = subprocess.run(["git", "-C", cwd, *args], capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip()
    except Exception as e:
        return 1, ""

def count_disk_files(d):
    n = 0
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if x != ".git"]
        n += len(files)
        if n > MAX_SCAN_FILES: return n
    return n

def count_lfs_pointers(d):
    """磁盘上以 LFS 指针开头的文件数（说明大文件内容未拉取）"""
    n, scanned = 0, 0
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if x != ".git"]
        for fn in files:
            scanned += 1
            if scanned > MAX_SCAN_FILES: return n
            p = os.path.join(root, fn)
            try:
                with open(p, "rb") as f:
                    head = f.read(200)
                if b"version https://git-lfs" in head:
                    n += 1
            except Exception:
                pass
    return n

def check_repo(name):
    d = os.path.join(BASE, name)
    r = {"dir": name, "ok": True, "issues": []}
    if not os.path.isdir(os.path.join(d, ".git")):
        return {**r, "ok": False, "issues": ["缺少 .git"], "tracked": 0, "disk": 0,
                "deleted": 0, "lfs": 0, "head": "", "remote_head": "", "status": "BROKEN"}
    rc, head = git(["rev-parse", "HEAD"], d)
    if rc != 0:
        r["issues"].append("HEAD 不可解析")
    r["head"] = (head or "")[:12]
    # 已跟踪文件数
    _, tracked = git(["ls-files"], d)
    r["tracked"] = len([x for x in tracked.split("\n") if x.strip()])
    # 磁盘文件数
    r["disk"] = count_disk_files(d)
    # 缺失文件（检出失败）
    _, st = git(["status", "--porcelain"], d)
    deleted = [l for l in st.split("\n") if l.strip().startswith("D ") or l.strip() == "D"]
    r["deleted"] = len(deleted)
    # LFS 指针残留
    r["lfs"] = count_lfs_pointers(d)
    # 是否浅克隆
    _, shallow = git(["rev-parse", "--is-shallow-repository"], d)
    r["shallow"] = (shallow == "true")
    # 远端 HEAD（联网）
    _, origin = git(["config", "remote.origin.url"], d)
    r["origin"] = origin
    r["remote_head"] = ""
    if origin:
        try:
            pr = subprocess.run(["git", "-c", f"http.proxy={PROXY}", "-c", f"https.proxy={PROXY}",
                                 "ls-remote", origin, "HEAD"],
                                capture_output=True, text=True, timeout=60)
            for line in pr.stdout.split("\n"):
                if line.strip():
                    r["remote_head"] = line.split()[0][:12]
                    break
        except Exception:
            r["remote_head"] = ""

    # 判定
    if r["tracked"] == 0:
        r["ok"] = False; r["issues"].append("无已跟踪文件（空仓库或检出失败）")
    if r["disk"] == 0:
        r["ok"] = False; r["issues"].append("工作树为空（无任何文件）")
    if r["deleted"] > 0:
        r["ok"] = False; r["issues"].append(f"有 {r['deleted']} 个文件未检出")
    if r["lfs"] > 0:
        r["issues"].append(f"有 {r['lfs']} 个 LFS 指针未拉取真实内容")
    if r["head"] and r["remote_head"] and r["head"] != r["remote_head"]:
        r["issues"].append(f"本地 HEAD 与远端不一致（远端有更新?）")
    r["status"] = "OK" if r["ok"] else "BROKEN"
    return r

def main():
    entries = parse()
    # 建立 目录 -> 条目 映射（按 remote 归一化）
    dirs = sorted([n for n in os.listdir(BASE)
                   if os.path.isdir(os.path.join(BASE, n)) and os.path.isdir(os.path.join(BASE, n, ".git"))])
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(check_repo, dirs))
    res_by_dir = {x["dir"]: x for x in results}

    matched, missing = [], []
    used = set()
    for e in entries:
        nh, ns = norm(e.get("https", "")), norm(e.get("ssh", ""))
        hit = None
        for dd in dirs:
            if dd in used: continue
            if norm(res_by_dir[dd].get("origin", "")) in (nh, ns):
                hit = dd; break
        if hit:
            used.add(hit)
            matched.append({**res_by_dir[hit], "https": e.get("https", ""), "ssh": e.get("ssh", "")})
        else:
            missing.append({"https": e.get("https", ""), "ssh": e.get("ssh", "")})
    extra = [res_by_dir[d] for d in dirs if d not in used]

    ok_list = [x for x in matched if x["status"] == "OK" and not x["issues"]]
    ok_warn = [x for x in matched if x["status"] == "OK" and x["issues"]]
    broken = [x for x in matched if x["status"] != "OK"]
    stale = [x for x in matched if x["head"] and x["remote_head"] and x["head"] != x["remote_head"]]
    lfs_repos = [x for x in matched if x.get("lfs", 0) > 0]

    md = (f"# Clone 内容级验证报告\n\n"
          f"- 生成时间：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
          f"- git.md 项目数：{len(entries)}\n"
          f"- ✅ 匹配且内容完整：{len(ok_list)}\n"
          f"- ⚠️ 完整但有提示（LFS 未拉取 / 远端有更新）：{len(ok_warn)}\n"
          f"- ❌ 内容异常：{len(broken)}\n"
          f"- ❓ git.md 中未找到对应仓库：{len(missing)}\n"
          f"- ⚠️ 目录下多余仓库：{len(extra)}\n\n")

    md += "## 一、内容完整（正常）\n\n"
    md += "_（略去逐项，详见 JSON；此处仅列出有提示项与异常项）_\n\n"
    md += "## 二、有提示项（仍可用，但需注意）\n\n"
    if ok_warn:
        for x in ok_warn:
            md += (f"- `{x['dir']}` — 已跟踪 {x['tracked']} / 磁盘 {x['disk']} 文件，"
                   f"HEAD `{x['head']}`，浅克隆={x['shallow']}\n  - 提示：{'; '.join(x['issues'])}\n")
    else:
        md += "_（无）_\n"
    md += "\n## 三、内容异常（需处理）\n\n"
    if broken:
        for x in broken:
            md += (f"- **{x['dir']}** — 已跟踪 {x['tracked']} / 磁盘 {x['disk']}，"
                   f"未检出 {x['deleted']}\n  - 问题：{'; '.join(x['issues'])}\n")
    else:
        md += "_（无）_\n"
    md += "\n## 四、git.md 中未匹配到仓库\n\n"
    md += ("\n".join(f"- `{m['https']}`" for m in missing) + "\n") if missing else "_（无）_\n"
    md += "\n## 五、目录下多余仓库\n\n"
    md += ("\n".join(f"- `{x['dir']}` ({x.get('origin','')})" for x in extra) + "\n") if extra else "_（无）_\n"
    md += f"\n## 六、LFS 未拉取真实内容的仓库（{len(lfs_repos)} 个）\n\n"
    md += ("\n".join(f"- `{x['dir']}`：{x['lfs']} 个指针文件" for x in lfs_repos) + "\n") if lfs_repos else "_（无）_\n"

    with open(os.path.join(BASE, "_CLONE_VERIFY_DEEP.md"), "w", encoding="utf-8") as f:
        f.write(md)
    with open(os.path.join(BASE, "_CLONE_VERIFY_DEEP.json"), "w", encoding="utf-8") as f:
        json.dump({"total": len(entries), "ok": len(ok_list), "warn": len(ok_warn),
                   "broken": len(broken), "missing": len(missing), "extra": len(extra),
                   "stale": len(stale), "matched": matched, "missing_list": missing,
                   "extra_list": extra}, f, ensure_ascii=False, indent=2)
    print(f"total={len(entries)} ok={len(ok_list)} warn={len(ok_warn)} broken={len(broken)} "
          f"missing={len(missing)} extra={len(extra)} stale={len(stale)}")
    for x in broken:
        print("BROKEN:", x["dir"], x["issues"])
    for x in ok_warn:
        print("WARN:", x["dir"], x["issues"])

if __name__ == "__main__":
    main()
