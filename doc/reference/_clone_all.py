#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量 clone git.md 中的开源项目到本目录。
- 每两行 = 一个项目（https 行 + git@ 行），项目间空行分隔。
- 先用 https 地址 clone（经代理 127.0.0.1:7897）；失败则尝试 git@ 地址。
- 两者都失败 → 标记 failed，继续下一个，不耽误时间。
- 每完成一个就增量写 _CLONE_STATUS.md / _CLONE_STATUS.json，明早可直接看。
"""
import os, re, subprocess, json, datetime

BASE = os.path.dirname(os.path.abspath(__file__))
GIT_MD = os.path.join(BASE, "..", "..", "..", "refrences", "git.md")
GIT_MD = os.path.abspath(GIT_MD)
STATUS_MD = os.path.join(BASE, "_CLONE_STATUS.md")
STATUS_JSON = os.path.join(BASE, "_CLONE_STATUS.json")
PROXY = "http://127.0.0.1:7897"
PER_REPO_TIMEOUT = 600          # 单仓库 clone 超时（秒）
HTTPS_RETRY = 1                 # https 失败后在非鉴权/非私有错误下额外重试次数

def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def parse():
    with open(GIT_MD, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()
    entries = []
    cur = {}
    for raw in lines:
        line = raw.strip()
        if not line:
            if cur:
                entries.append(cur); cur = {}
            continue
        if line.startswith("https://"):
            if cur.get("https"):
                entries.append(cur); cur = {}
            cur["https"] = line
        elif line.startswith("git@"):
            cur["ssh"] = line
        else:
            # 其它说明性内容，忽略
            pass
    if cur:
        entries.append(cur)
    return entries

def repo_dir_of(url):
    # https://github.com/OWNER/REPO.git 或 git@github.com:OWNER/REPO.git
    m = re.search(r"github\.com[:/]([^/]+)/(.+?)(?:\.git)?$", url)
    if m:
        owner, name = m.group(1), m.group(2)
        return f"{owner}__{name}"
    # 退化：用最后一段
    return re.sub(r"[^A-Za-z0-9._-]", "_", url)

def run(cmd, timeout):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return 124, "", "TIMEOUT"

def try_clone(url, target, use_proxy):
    if use_proxy:
        cmd = ["git", "-c", f"http.proxy={PROXY}", "-c", f"https.proxy={PROXY}",
               "clone", "--depth", "1", url, target]
    else:
        cmd = ["git", "clone", url, target]
    rc, out, err = run(cmd, PER_REPO_TIMEOUT)
    return rc, err

def already_cloned(target):
    return os.path.isdir(os.path.join(target, ".git"))

def main():
    entries = parse()
    n = len(entries)
    log(f"解析到 {n} 个项目，开始 clone（目标目录 {BASE}）")
    results = []  # list of dict

    # 先写初始状态
    def dump():
        ok = [r for r in results if r["status"] == "ok"]
        skip = [r for r in results if r["status"] == "skip"]
        fail = [r for r in results if r["status"] == "fail"]
        pending = n - len(results)
        summary = (f"# 开源项目 Clone 状态\n\n"
                   f"- 总计项目数：{n}\n- 已成功（含跳过）：{len(ok)+len(skip)}\n"
                   f"- 失败：{len(fail)}\n- 待处理：{pending}\n"
                   f"- 最后更新：{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        with open(STATUS_JSON, "w", encoding="utf-8") as f:
            json.dump({"total": n, "ok": len(ok), "skip": len(skip),
                       "fail": len(fail), "pending": pending,
                       "results": results}, f, ensure_ascii=False, indent=2)
        md = summary + "## 一、Clone 成功\n\n"
        for r in ok + skip:
            tag = "（已存在，跳过）" if r["status"] == "skip" else ""
            md += f"- [{r['name']}]({r['url_used']}){tag}\n"
        md += "\n## 二、Clone 失败 / 未 clone\n\n"
        if fail:
            for r in fail:
                md += f"- **{r['name']}** — https: `{r.get('https','')}` / ssh: `{r.get('ssh','')}`\n  - 原因：{r.get('reason','')}\n"
        else:
            md += "_（无）_\n"
        with open(STATUS_MD, "w", encoding="utf-8") as f:
            f.write(md)

    dump()

    for i, e in enumerate(entries, 1):
        https = e.get("https")
        ssh = e.get("ssh")
        name = repo_dir_of(https or ssh)
        target = os.path.join(BASE, name)
        log(f"[{i}/{n}] {name}")
        if already_cloned(target):
            results.append({"name": name, "status": "skip", "url_used": https or ssh,
                            "https": https, "ssh": ssh, "reason": "目录已存在且为 git 仓库"})
            dump(); continue
        # 1) https（代理）+ 可选重试
        attempts = 1 + HTTPS_RETRY
        rc = -1; reason = ""
        for att in range(attempts):
            rc, reason = try_clone(https, target, use_proxy=True)
            if rc == 0:
                break
            # 鉴权/私有仓库错误不重试
            if "Authentication failed" in reason or "could not read Username" in reason \
               or "Permission denied" in reason or "repository not found" in reason \
               or "not found" in reason and "fatal" in reason:
                break
            log(f"   https 第{att+1}次失败，重试…")
        if rc == 0:
            results.append({"name": name, "status": "ok", "url_used": https,
                            "https": https, "ssh": ssh, "reason": ""})
            dump(); continue
        https_reason = reason
        # 2) ssh（直连）
        if ssh:
            rc2, reason2 = try_clone(ssh, target, use_proxy=False)
            if rc2 == 0:
                results.append({"name": name, "status": "ok", "url_used": ssh,
                                "https": https, "ssh": ssh, "reason": ""})
                dump(); continue
            ssh_reason = reason2
        else:
            ssh_reason = "（无 ssh 地址）"
        results.append({"name": name, "status": "fail", "url_used": "",
                        "https": https, "ssh": ssh,
                        "reason": f"https 失败: {https_reason[:200]} | ssh 失败: {ssh_reason[:200]}"})
        dump()
        log(f"   ❌ 两个地址均失败，继续下一个")

    log("全部完成。")
    # 最终汇总再写一次
    ok = [r for r in results if r["status"] in ("ok", "skip")]
    fail = [r for r in results if r["status"] == "fail"]
    log(f"成功(含跳过): {len(ok)} / 失败: {len(fail)} / 总计: {n}")

if __name__ == "__main__":
    main()
