"""财报PDF存量清理：删除近 N 年（报告期口径）以前的 PDF 并同步 manifest（用户决策 2026-10-01）

背景：磁盘余量不足（121Gi 剩 vs 预估还需 ~170GB），决策缩范围至近 10 年
（报告期 >= 2026-10+1-10 = 2017 年）。后续采集窗口由 cninfo.pdf_recent_years
在 manager 清单层强制（全模式生效），本脚本负责一次性清理存量并保持 manifest
一致（防"文件已删、manifest 仍在"的脏记录）。

用法（仓库根 stock_yuzh/ 下）:
    .venv/bin/python backend/scripts/prune_financial_pdf.py            # dry-run
    .venv/bin/python backend/scripts/prune_financial_pdf.py --apply    # 实删

行为：
1. 扫描 data/financial_pdf/{code}_{name}/{yyyy}_{NN}.pdf，删除 yyyy < floor 的文件；
2. manifest（tmp/financial_pdf_manifest.json）同步清理 files/failed 中对应键；
3. 删除清空后的股票目录；输出释放空间与文件数。
幂等可重跑。
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / 'backend'))

PDF_DIR = ROOT / 'data' / 'financial_pdf'
MANIFEST = ROOT / 'tmp' / 'financial_pdf_manifest.json'
FILE_RE = re.compile(r'^(\d{4})_\d+\.pdf$')


def main():
    apply = '--apply' in sys.argv
    from src.config.config_manager import ConfigManager
    recent = int((ConfigManager().load_config().get('cninfo', {}) or {})
                 .get('pdf_recent_years', 0) or 0)
    if recent <= 0:
        print('cninfo.pdf_recent_years 未启用（0），无清理口径，退出')
        return
    from datetime import datetime
    floor = datetime.now().year - recent + 1
    print(f'清理口径：报告期年份 < {floor}（近 {recent} 年窗口）{"【dry-run】" if not apply else "【实删】"}')

    freed_bytes = 0
    deleted_files = 0
    dirs_touched = set()
    for stock_dir in sorted(PDF_DIR.iterdir()) if PDF_DIR.exists() else []:
        if not stock_dir.is_dir():
            continue
        for f in stock_dir.iterdir():
            m = FILE_RE.match(f.name)
            if not m:
                continue
            if int(m.group(1)) < floor:
                freed_bytes += f.stat().st_size
                deleted_files += 1
                dirs_touched.add(stock_dir.name)
                if apply:
                    f.unlink()

    # manifest 同步清理
    manifest_pruned = 0
    failed_pruned = 0
    if MANIFEST.exists():
        manifest = json.loads(MANIFEST.read_text(encoding='utf-8'))
        for code, entry in manifest.get('stocks', {}).items():
            files = entry.get('files') or {}
            drop = [k for k in files if int(k.split('_')[0]) < floor]
            for k in drop:
                files.pop(k)
            manifest_pruned += len(drop)
            failed = entry.get('failed') or []
            keep = [f for f in failed if int((f.get('key') or '0_0').split('_')[0]) >= floor]
            failed_pruned += len(failed) - len(keep)
            if keep:
                entry['failed'] = keep
            else:
                entry.pop('failed', None)
        if apply:
            MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=1),
                                encoding='utf-8')

    # 清空的股票目录删除
    removed_dirs = 0
    if apply:
        for d in sorted(PDF_DIR.iterdir()) if PDF_DIR.exists() else []:
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
                removed_dirs += 1

    print(f"文件: 删 {deleted_files} 个（涉及 {len(dirs_touched)} 只股票），"
          f"释放 {freed_bytes / 1048576 / 1024:.1f} GB")
    print(f"manifest: 清 files {manifest_pruned} 条 / failed {failed_pruned} 条")
    if apply:
        print(f"清空目录删除: {removed_dirs} 个")
        print('✓ 清理完成（幂等，可重跑核验）')
    else:
        print('（dry-run 未实际删除；确认无误后加 --apply 执行）')


if __name__ == '__main__':
    main()
