# Clone 内容级验证报告

- 生成时间：2026-10-11 23:13:21
- git.md 项目数：127
- ✅ 匹配且内容完整：123
- ⚠️ 完整但有提示（LFS 未拉取 / 远端有更新）：4
- ❌ 内容异常：0
- ❓ git.md 中未找到对应仓库：0
- ⚠️ 目录下多余仓库：0

## 一、内容完整（正常）

_（略去逐项，详见 JSON；此处仅列出有提示项与异常项）_

## 二、有提示项（仍可用，但需注意）

- `TNT-Likely__PanWatch` — 已跟踪 786 / 磁盘 786 文件，HEAD `1ebec29a494b`，浅克隆=True
  - 提示：本地 HEAD 与远端不一致（远端有更新?）
- `UFund-Me__Qbot` — 已跟踪 4463 / 磁盘 4461 文件，HEAD `f0425ae4ae8b`，浅克隆=True
  - 提示：有 3 个 LFS 指针未拉取真实内容
- `charliedream1__ai_quant_trade` — 已跟踪 683 / 磁盘 683 文件，HEAD `a67a217aaea4`，浅克隆=True
  - 提示：本地 HEAD 与远端不一致（远端有更新?）
- `JerBouma__FinanceDatabase` — 已跟踪 379 / 磁盘 379 文件，HEAD `fe5f75ca72f7`，浅克隆=True
  - 提示：本地 HEAD 与远端不一致（远端有更新?）

## 三、内容异常（需处理）

_（无）_

## 四、git.md 中未匹配到仓库

_（无）_

## 五、目录下多余仓库

_（无）_

## 六、LFS 未拉取真实内容的仓库（1 个）

- `UFund-Me__Qbot`：3 个指针文件

## 七、提示项补充核实（结论）

### 1) 本地 HEAD 与远端不一致的 3 个仓库 —— 均为「上游新提交」，非克隆损坏

| 仓库 | 本地 HEAD（提交时间） | 远端 HEAD（提交时间） | 结论 |
|---|---|---|---|
| TNT-Likely__PanWatch | 1ebec29a494b（10-11 17:49） | 575b27a639f2（10-11 22:55） | 远端更新，克隆有效 |
| charliedream1__ai_quant_trade | a67a217aaea4（10-11 14:12） | 17327c6cd0ec（10-11 22:57） | 远端更新，克隆有效 |
| JerBouma__FinanceDatabase | fe5f75ca72f7（10-11 14:11） | 9d6ed48deb8e（10-11 17:02） | 远端更新，克隆有效 |

三者远端提交时间均晚于本地，属 clone（22:00–22:35 完成）后上游新推送。内容完整可用；
如需同步到最新，在各目录执行 `git pull --depth=1`（已做过 fetch，直接 reset 亦可）。

### 2) UFund-Me__Qbot 的 3 个 LFS 指针 —— 均为 Linux 版 wxPython wheel，参考无需拉取

- `dev/wxPython-4.2.1-cp38-cp38-linux_x86_64.whl`
- `dev/wxPython-4.2.0-cp38-cp38-linux_x86_64.whl`
- `dev/wxPython-4.2.0-cp39-cp39-linux_x86_64.whl`

本机为 macOS，这些 Linux 二进制对源码参考无意义；源码、文档、其余 4460 个文件均已完整落盘。
若确需真实文件：安装 git-lfs（`brew install git-lfs`）后在该目录执行 `git lfs pull`。
