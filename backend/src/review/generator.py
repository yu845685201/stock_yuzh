"""复盘报告生成器（复盘报告层方案 §5.2/§5.4/§5.5，M1）。

编排：取数（DataReader 60d 窗口）→ 聚合（六维）→ 渲染（MD+HTML）→ 落盘。
- 输出：config review.output_dir（默认 doc/reports/），文件名
  ``daily_review_YYYYMMDD.md/.html`` + ``.meta.json``（耗时等元信息）；
- **同锚定日重复生成逐字节一致**：正文不出现运行时间戳（§5.4）；
- 链 CSV 缺失自动降级：明细章节不受影响（独立重算），CSV 存在时做涨停家数交叉校验。
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from ..analysis.data_reader import DataReader
from ..database.connection import DatabaseConnection
from .aggregator import ReviewAggregator
from .renderer import blocks_to_html, blocks_to_markdown
from .sections import conclusion, fundamentals, market_env, sector_mainline, sentiment, stock_movers
from .sections import yesterday_review
from .verifier import build_predictions

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = 'doc/reports'


def generator_lede(agg: Dict[str, Any]) -> list:
    """结论先行导语（四期 §1.4）：情绪定位 + 核心读数一句话，置于全文最前。

    §4.3（2026-11-24 第二轮）：导语并入首页三件套——一段判断（风格×广度
    特征 + 核心读数 + 正式候选结论 + 采集缺行说明）、覆盖两行注记、三项
    次日回填表（冻结基准，N 日收盘后在「昨日核验」回填；缺键降级为仅情绪
    读数，向后兼容旧缓存/测试夹具）。
    """
    sent = (agg.get('sentiment') or {}).get('data') or {}
    cycle = sent.get('cycle') or {}
    position = cycle.get('position')
    links = _homepage_bits(agg)
    if not position and not links:
        return []
    bits = []
    if position:
        bits.append(f'情绪定位 {position}')
    if sent.get('max_streak') is not None:
        bits.append(f"空间板 {sent['max_streak']} 板")
    if sent.get('limit_up_count') is not None:
        bits.append(f"涨停 {sent['limit_up_count']} 家")
    if sent.get('burst_rate') is not None:
        bits.append(f"炸板率 {sent['burst_rate']:.0%}")
    if sent.get('premium_mean') is not None:
        bits.append(f"昨日涨停溢价均值 {sent['premium_mean']:+.2f}%")
    head = links.pop(0) if links else ''
    if head:
        # 首页判断句 + 情绪定位括注（§三：定位是判断句旁的判读依据）
        text = head + (f"（{'；'.join(bits)}）" if bits else '')
    else:
        # 降级（无 coverage/首页要素）：保留旧导语形态「结论先行：情绪定位 X（…）」
        text = ''
        if bits:
            first = bits.pop(0)
            text = f'结论先行：{first}' + (f"（{'；'.join(bits)}）" if bits else '')
    out = [{'type': 'para', 'text': text}] if text else []
    out.extend(links)
    return out


#: 情绪周期定位 → 首页特征短语外的判读（正文再展开，不重复）
_CN_NUM = {2: '二', 3: '三', 4: '四', 5: '五', 6: '六', 7: '七', 8: '八', 9: '九'}


def _cn_board(h: int) -> str:
    return f'{_CN_NUM.get(h, str(h))}板'


def _homepage_bits(agg: Dict[str, Any]) -> list:
    """首页判断句 + 覆盖注记 + 三项回填表（§三 样例口径；数据缺失逐项降级）。

    返回 blocks 列表；首元素为判断句正文（调用方拼接情绪定位括注）。
    """
    from .coverage import coverage_note, coverage_sensitive_note
    sent = (agg.get('sentiment') or {}).get('data') or {}
    env = (agg.get('market_env') or {}).get('data') or {}
    me = (agg.get('money_effect') or {}).get('data') or {}
    cov = agg.get('coverage') or {}
    out: list = []

    # ---- 特征短语：风格（沪深300 vs 中证2000）× 广度（上涨占比）----
    style = ''
    spread = env.get('style_spread') or {}
    s2000 = spread.get('spread_2000')
    if s2000 is not None:
        if s2000 <= -1.0:
            style = '小盘领涨的'
        elif s2000 >= 1.0:
            style = '大盘占优的'
        else:
            style = '大小盘均衡的'
    breadth = ''
    if cov.get('up_ratio_valid') is not None:
        r = float(cov['up_ratio_valid'])
        breadth = '普涨' if r >= 0.7 else ('普跌' if r <= 0.3 else '分化')
    head = f"{agg.get('as_of', '')}｜{(style or '') + (breadth or '行情')}" \
        if (style or breadth) else ''

    # ---- 核心读数 ----
    reads = []
    if cov.get('up_ratio_valid') is not None:
        reads.append(f"已交易样本中 {float(cov['up_ratio_valid']):.1%} 上涨")
    by_name = {i.get('name'): i for i in (env.get('indices') or [])}
    zq2000 = by_name.get('中证2000') or {}
    hs300 = by_name.get('沪深300') or {}
    if zq2000.get('change_rate') is not None and hs300.get('change_rate') is not None:
        reads.append(f"中证 2000 涨 {zq2000['change_rate']:+.2f}%，"
                     f"沪深 300 涨 {hs300['change_rate']:+.2f}%")
    # 昨日涨停股今日续板（feedback 分桶行合计 + 停牌缺行 = 昨日涨停总数；
    # 「合计」行是分桶行的汇总，求和时须剔除——否则双计）
    fb = me.get('feedback') or {}
    prev_lu_total = None
    if fb:
        prev_lu_total = (sum(r.get('count') or 0
                             for r in (fb.get('rows') or [])
                             if r.get('bucket') != '合计')
                         + int(fb.get('suspended') or 0)) or None
    cont = sent.get('premium_continued')
    if prev_lu_total and cont is not None:
        reads.append(f"昨日 {prev_lu_total} 只涨停股中有 {cont} 只继续涨停")
    if sent.get('max_streak'):
        gap_txt = ''
        gaps = (sent.get('ladder_gap') or {}).get('gaps') or []
        if gaps:
            gap_txt = '，' + '、'.join(_cn_board(h) + '空缺' for h in gaps[:2])
        reads.append(f"最高连板为{_cn_board(int(sent['max_streak']))}{gap_txt}")
    # 正式右侧候选结论（发布批次单一出处）
    pub = agg.get('candidate_publication') or {}
    pools = pub.get('pools') or {}
    n_published = sum((pools.get(k) or {}).get('n_published', 0)
                      for k in ('R1', 'R2', 'R3', 'R4'))
    reads.append(f"按收盘规则，下一交易日的正式右侧候选为 {n_published} 只"
                 + ('（各池均未开放）' if not (pub.get('open_types') or [])
                    and pub.get('close_reason') else ''))
    # 采集缺行（E 口径缺失时由覆盖注记行单独说明）
    if cov.get('available') and cov.get('n_missing'):
        reads.append(f"采集缺行 {cov['n_missing']} 只，涨停与题材排名按已采集样本统计")
    text = ''
    if head or reads:
        text = (f"**{head}。**" if head else '') + '。'.join(reads)
        if text and not text.endswith('。'):
            text += '。'
    out.append(text)

    # ---- 覆盖两行注记（§4.3：范围/入库/成交/缺行 + 敏感性）----
    note = coverage_note(cov)
    if note:
        out.append({'type': 'note', 'text': note})
    sens = coverage_sensitive_note(cov)
    if sens:
        out.append({'type': 'note', 'text': sens})

    # ---- 三项次日回填表（§三：跟踪问题 | 冻结基准 | 次日回填占位）----
    out.extend(_backfill_table_blocks(agg))
    return out


def _backfill_table_blocks(agg: Dict[str, Any]) -> list:
    """三项次日回填表（§4.3 E2）：T 首发时冻结基准，N 日收盘后回填。"""
    cohorts = agg.get('cohorts') or {}
    anchors = agg.get('backfill_anchors') or {}
    lu = cohorts.get('limit_up') or {}
    tb = cohorts.get('two_board') or {}
    vp = anchors.get('volume_price') or {}
    if not lu and not tb and not vp:
        return []
    rows = []
    if lu:
        rows.append(['涨停后的强势是否普遍延续',
                     f"{lu.get('anchor_date', agg.get('as_of', '—'))} 收盘涨停的 "
                     f"{lu.get('n_total', 0)} 只",
                     '次日涨跌幅中位数、翻红数 / 有效数及缺行情数'
                     '（下一交易日收盘后回填）'])
    if tb:
        base = (f"当日 {tb.get('n_total', 0)} 只可确认二板股"
                + (f"（身份未知 {tb.get('n_unknown', 0)} 只单列）"
                   if tb.get('n_unknown') else ''))
        rows.append(['连板队伍有没有新的承接', base,
                     '二进三家数 / 可判定数及不可判定数（下一交易日收盘后回填）'])
    if vp:
        chg = (f"{vp.get('chg_pct', 0):+.2f}%" if vp.get('chg_pct') is not None
               else '—')
        ratio = (f"{vp.get('ratio_5d'):.2f} 倍" if vp.get('ratio_5d') is not None
                 else '待核')
        base = f"中证全指当天 {chg}，成交额为自身近五日均额的 {ratio}"
        if vp.get('status') != 'ok':
            base += f"（锚待核：{vp.get('reason') or '窗口缺日'}）"
        rows.append(['普涨是否仍有成交配合', base,
                     '同指数次日涨跌幅、成交额相对其五日均额的倍数；注明交易日'
                     '（下一交易日收盘后回填）'])
    return [
        {'type': 'heading', 'level': 3, 'text': '三项次日回填（首页冻结，次日核验）'},
        {'type': 'table',
         'headers': ['跟踪的问题', '冻结的基准', '次日回填'],
         'rows': rows},
        {'type': 'note',
         'text': '股票名单在锚定日冻结（成员哈希随预测 payload 落库），'
                 '缺行情的成员保留在原分母说明中；回填结果见下一交易日报告'
                 '「〇、昨日核验」。量能倍数按交易日历应到日期核对六个端点，'
                 '窗口缺日或均额为零则记待核。'},
    ]


class ReviewReportGenerator:
    """每日复盘报告生成（六维 → MD/HTML）"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        review_cfg = self.config.get('review') or {}
        out_dir = review_cfg.get('output_dir') or DEFAULT_OUTPUT_DIR
        root = self._repo_root()
        self.output_dir = Path(out_dir) if Path(out_dir).is_absolute() else root / out_dir

    def generate(self, as_of: Optional[str] = None,
                 output_dir: Optional[str] = None) -> Dict[str, Any]:
        """生成报告；返回 {md_path, html_path, meta_path, as_of, duration_seconds}"""
        started = time.time()
        out_dir = Path(output_dir) if output_dir else self.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        reader = DataReader(self.config_manager)
        survivor_bias = None
        try:
            as_of = reader.resolve_as_of(as_of)
            aggregator = ReviewAggregator(reader, self.config)
            agg = aggregator.aggregate(as_of)
            # P4-3（B4）：幸存者偏差量化（meta 披露，不进报告正文/缓存）
            try:
                survivor_bias = reader.survivor_pool_stats(as_of)
            except Exception as e:  # pragma: no cover - 统计失败不影响报告
                logger.warning(f'幸存者偏差统计失败: {e!r}')
        finally:
            reader.close()

        # 预期落库 + 核验落库（四期方案 §1；写库失败不影响报告输出）
        self._persist_prediction_and_verification(as_of, agg)

        blocks = self._render_blocks(agg, self.config.get('review') or {})
        md_text = blocks_to_markdown(blocks)
        html_text = blocks_to_html(f'每日复盘 {self._fmt_date(as_of)}', blocks)

        stem = f'daily_review_{as_of}'
        md_path = out_dir / f'{stem}.md'
        html_path = out_dir / f'{stem}.html'
        meta_path = out_dir / f'{stem}.meta.json'
        md_path.write_text(md_text, encoding='utf-8')
        html_path.write_text(html_text, encoding='utf-8')
        duration = time.time() - started
        meta = {
            'as_of': as_of,
            'prev_date': agg.get('prev_date'),
            'window_start': agg.get('window_start'),
            'panel_rows': agg.get('panel_rows'),
            'stock_count': agg.get('stock_count'),
            'generator_version': '1.0',
            'generated_at': datetime.now().isoformat(timespec='seconds'),  # 仅 meta，不进正文
            'duration_seconds': round(duration, 1),
            'output_dir': str(out_dir),
        }
        cross = ((agg.get('movers') or {}).get('data') or {}).get('cross_check')
        if cross:
            meta['cross_check'] = cross
        if survivor_bias is not None:
            meta['survivor_bias'] = survivor_bias  # P4-3：as-of 应含退市股统计
        # §4.3 E1（2026-11-24）：采集覆盖事实与首页/正文同一对象
        if agg.get('coverage'):
            meta['coverage'] = agg['coverage']
        # P0-4：估值 calc 库版本（同日重算一致，原则 P7）
        from .valuation_calc import VALUATION_CALC_VERSION
        meta['valuation_calc_version'] = VALUATION_CALC_VERSION
        ind_parity = ((agg.get('indicators') or {}).get('data') or {}).get('parity') \
            if hasattr(agg.get('indicators'), 'get') else None
        if ind_parity:
            meta['indicator_parity'] = ind_parity
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                             encoding='utf-8')
        logger.info(f'复盘报告已生成: {md_path}（{duration:.1f}s）')
        return {'md_path': str(md_path), 'html_path': str(html_path),
                'meta_path': str(meta_path), 'as_of': as_of,
                'duration_seconds': round(duration, 1)}

    # ------------------------------------------------------------------ 章节组装

    @staticmethod
    def _render_blocks(agg: Dict[str, Any], review_cfg: Dict[str, Any]) -> list:
        """报告头（结论先行导语）+ 昨日核验 + 六维章节 + 交叉校验尾注"""
        as_of = agg['as_of']
        blocks = [
            {'type': 'heading', 'level': 1,
             'text': f'每日复盘报告（锚定日 {as_of}）'},
            {'type': 'para',
             'text': f"覆盖全市场 {agg.get('stock_count', 0)} 只个股（含北交所），"
                     f"指标窗口 60 交易日（起点 {agg.get('window_start') or '全历史'}，"
                     f"面板 {agg.get('panel_rows', 0)} 行）；"
                     f"上一交易日 {agg.get('prev_date') or '—'}。"},
            *generator_lede(agg),
            *yesterday_review.build(agg.get('verification') or {}),
            {'type': 'note',
             'text': '本报告指标由报告层基于库内日K独立重算（不依赖当日链运行）；'
                     '涨停/连板/炸板等判定与 daily_review 链模块同口径。'
                     '各章节数据口径详见 doc/复盘框架.MD。'},
            *market_env.build(agg.get('market_env'), agg.get('sentiment'), agg.get('margin'),
                              volume_price=agg.get('volume_price'),
                              capital_anchors=agg.get('capital_anchors'),
                              news=agg.get('news')),
            *sentiment.build(agg.get('sentiment'), agg.get('money_effect')),
            *sector_mainline.build(agg.get('sector')),
            *stock_movers.build(agg.get('movers'), agg.get('lhb'), agg.get('money_flow'),
                                sector_money=agg.get('sector_money'),
                                indicators=agg.get('indicators')),
            *fundamentals.build(agg.get('fundamentals'), agg.get('announcement'),
                                valuation_digest=agg.get('valuation_digest')),
            *conclusion.build(agg, review_cfg),
        ]
        cross = ((agg.get('movers') or {}).get('data') or {}).get('cross_check')
        if cross:
            if cross.get('ok') is True:
                blocks.append({'type': 'note',
                               'text': f"✓ 内部一致性（涨停重算）：重算涨停家数 "
                                       f"{cross['recomputed_limit_up']} 与链 CSV 一致"})
            elif cross.get('ok') is False:
                blocks.append({'type': 'note',
                               'text': f"⚠ 内部一致性（涨停重算）告警：重算涨停 "
                                       f"{cross['recomputed_limit_up']} 家 "
                                       f"vs CSV {cross['csv_limit_up']} 家不一致"
                                       '（可能是链运行时点与库内数据不一致，请复跑链后重出报告）'})
            elif cross.get('note'):
                blocks.append({'type': 'note', 'text': f'内部一致性（涨停重算）：{cross["note"]}'})
        # P0-1 指标对拍校验（链 CSV 指标列 vs 报告层面板复算，同一实现两入口）
        ind_res = agg.get('indicators')
        ind_data = (ind_res.data or {}) if hasattr(ind_res, 'data') else {}
        if ind_data.get('parity'):
            p = ind_data['parity']
            if p.get('ok'):
                blocks.append({'type': 'note',
                               'text': f"✓ 内部一致性（指标对拍）：报告层复算与链 CSV 指标列一致"
                                       f"（{p['compared']} 股×列，{p['columns']} 列）"})
            else:
                blocks.append({'type': 'note',
                               'text': f"⚠ 内部一致性（指标对拍）不一致：{p['mismatch_count']} 处"
                                       f"（样本 {p['mismatches'][:3]}）——链与报告层"
                                       f"窗口或数据时点可能不一致，请复跑链后重出报告"})
        elif ind_data:
            blocks.append({'type': 'note',
                           'text': '内部一致性（指标对拍）：链 CSV 未跑，本报告为面板独立复算'
                                   '（与链同一实现，跑链后可交叉校验）'})
        return blocks

    def _persist_prediction_and_verification(self, as_of: str, agg: Dict[str, Any]) -> None:
        """预期落库（当日）+ 核验落库（对昨日的核验结果）。

        失败仅告警不阻塞报告生成——落库服务闭环战绩，报告正文数据独立于落库。
        连接为按操作上下文管理（连接池），无需手动关闭。
        """
        try:
            pred = build_predictions(agg, self.config.get('review') or {})
            db = DatabaseConnection(self.config_manager)
            db.ensure_review_tables()   # 新表建表兜底（幂等；同 SentimentMetricsManager 模式）
            db.upsert_review_prediction(as_of, pred)
            ver = agg.get('verification')
            # AggregationResult（dataclass）非 dict：用属性访问判断
            if ver is not None and getattr(ver, 'available', False) \
                    and getattr(ver, 'data', None):
                db.upsert_review_verification(as_of, agg.get('prev_date'), ver.data)
        except Exception as e:
            logger.warning(f'预期/核验落库失败（不影响报告输出）: {e}')

    @staticmethod
    def _fmt_date(ymd: str) -> str:
        return f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}' if len(str(ymd)) == 8 else str(ymd)

    def _repo_root(self) -> Path:
        from ..sync.report_writer import resolve_repo_root
        return resolve_repo_root()
