# -*- coding: utf-8 -*-
"""
SOE Validator (All-in-One, fixed, 2025-10-30, sep3)
在 sep2 的基础上改动点：
1. “中长期激励工具” 的取值不再被规则表里不完整的枚举误杀：
   - 在字段级校验里，如果字段名是“中长期激励工具”，就用代码内置的字符集
     （abcdefghijklmnopqrz 和 |），而不是规则表里的枚举。
2. 中长期激励三件套的业务关系补全：
   - 参与=是 & 工具为空 → 报错（原有）
   - 参与=否 & 工具有值 → 报错（新增）
   - 中长期激励收入>0 & 工具为空 → 报错（原有 sep2 已加）
3. “其他一次性专项奖励、其他一次性专项奖励名称” 与
   “中长期激励三件套” 保持完全独立，互不干扰（沿用 sep2 的拆分）。
"""

import argparse
import re
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict
from decimal import Decimal

import numpy as np
import pandas as pd

# ---------- 枚举/上市类型 ----------
def check_listing_type(value: str) -> str | None:
    s = str(value).strip().replace("｜", "|")
    if not s:
        return None
    toks = [t.strip() for t in s.split("|") if t.strip()]
    # 只允许 a~i
    for t in toks:
        if len(t) != 1 or not ("a" <= t <= "i"):
            return "上市类型仅允许 a~i，用 | 分隔；i 不能与其他并存"
    # 不允许重复
    if len(set(toks)) != len(toks):
        return "上市类型不能出现重复值"
    # i 不能与其他并存
    if "i" in toks and len(toks) > 1:
        return "上市类型中的 i（非上市）不能与其他值同时出现"
    return None

warnings.simplefilter("ignore", FutureWarning)


def _is_empty_like(val: str) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    if s == "":
        return True
    s_low = s.lower()
    return s_low in ("nan", "none", "null", "na")


def norm(v: Any) -> str:
    if v is None:
        return ""
    try:
        if isinstance(v, float) and np.isnan(v):
            return ""
    except Exception:
        pass
    return str(v).strip()

def try_decimal(x: Any) -> Optional[Decimal]:
    s = norm(x)
    if s == "":
        return None
    try:
        return Decimal(s.replace(",", ""))
    except Exception:
        return None

def read_data_any(path: str) -> Dict[str, pd.DataFrame]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"未找到数据路径：{path}")
    if p.is_dir():
        dfs: Dict[str, pd.DataFrame] = {}
        for fp in sorted(p.glob("*.xls*")):
            try:
                dfs[fp.stem] = pd.read_excel(fp)
            except Exception as e:
                print(f"[WARN] 读取失败：{fp.name} - {e}")
        return dfs
    else:
        xls = pd.ExcelFile(p)
        return {name: pd.read_excel(xls, sheet_name=name) for name in xls.sheet_names}

def normalize_table_name(name: str) -> str:
    return norm(name).replace("\n", " ").replace("\r", " ").strip()

def chinese_or_pipe_strip(s: str) -> str:
    s = s.replace("｜", "|")
    parts = [p.strip() for p in s.split("|") if p.strip()]
    return "|".join(parts)

def split_enum_string(s: str) -> List[str]:
    s = chinese_or_pipe_strip(norm(s))
    if not s:
        return []
    return [p.strip() for p in s.split("|")]

def compile_rules_from_excel(xlsx_path: Path,
                             sheet_name: str = "央企端-表内校验",
                             codes_sheet: str = "码值") -> Dict[str, Dict[str, Dict[str, Any]]]:
    xls = pd.ExcelFile(xlsx_path)
    try:
        df_rules = pd.read_excel(xls, sheet_name=sheet_name)
    except Exception:
        raise RuntimeError(f"读取规则Sheet失败：{sheet_name}")

    def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = {str(c).strip(): c for c in df.columns}
        for cand in candidates:
            if cand in cols:
                return cols[cand]
        for c in df.columns:
            s = str(c).strip()
            if any(k in s for k in candidates):
                return c
        return None

    col_table = pick_col(df_rules, ["表名", "表", "对象", "表中文名", "表英文名"])
    col_field = pick_col(df_rules, ["字段", "字段名", "列名", "字段名称"])
    col_rule  = pick_col(df_rules, ["规则", "校验规则", "规则说明", "逻辑", "说明"])
    col_enum  = pick_col(df_rules, ["允许值", "枚举", "枚举值", "取值"])

    if not col_table or not col_field:
        raise RuntimeError("在规则表中未找到 表名/字段 列")

    rules: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for _, row in df_rules.iterrows():
        table = normalize_table_name(row[col_table])
        field = norm(row[col_field])
        if not table or not field:
            continue
        r = norm(row[col_rule]) if col_rule else ""
        enum_text = norm(row[col_enum]) if col_enum else ""

        t_rules = rules.setdefault(table, {})
        fr = t_rules.setdefault(field, {
            "required": False,
            "type": "",
            "len_eq": None,
            "len_max": None,
            "enum": set(),
            "tips": ""
        })

        text = f"{r} {enum_text}".strip()

        if "不为空" in text or "必填" in text:
            fr["required"] = True

        m_eq = re.search(r"长度\s*[=＝]\s*(\d+)", text)
        m_le = re.search(r"(长度\s*(?:≤|<=)|长度小于等于|长度不超过)\s*(\d+)", text)
        if m_eq:
            fr["len_eq"] = int(m_eq.group(1))
        if m_le:
            fr["len_max"] = int(m_le.group(2))

        may = []
        if enum_text:
            may += split_enum_string(enum_text)
        if ("|" in r) and (len(r.split("|")) >= 2):
            may += split_enum_string(r)
        if may:
            fr["enum"].update([x for x in may if x])

        if text:
            fr["tips"] = text

    codes_sheet_try = [codes_sheet]
    if codes_sheet != "码值表":
        codes_sheet_try.append("码值表")

    for cs in codes_sheet_try:
        if cs not in xls.sheet_names:
            continue
        df_codes = pd.read_excel(xls, sheet_name=cs)
        col_f = pick_col(df_codes, ["字段", "字段名", "列名", "字段名称"])
        col_enum2 = pick_col(df_codes, ["枚举", "允许值", "取值", "枚举值", "代码-名称", "代码名称"])
        col_code = pick_col(df_codes, ["代码", "编码", "值", "码"])
        col_name = pick_col(df_codes, ["名称", "含义", "文本"])
        if not col_f:
            continue

        if col_enum2 and (col_enum2 in df_codes.columns):
            for _, r2 in df_codes.iterrows():
                f = norm(r2[col_f])
                if not f:
                    continue
                items = split_enum_string(r2[col_enum2])
                if not items:
                    continue
                for t in rules:
                    if f in rules[t]:
                        rules[t][f]["enum"] = set(items)
                for t in rules:
                    if f not in rules[t]:
                        rules[t][f] = {
                            "required": False,
                            "type": "",
                            "len_eq": None,
                            "len_max": None,
                            "enum": set(items),
                            "tips": ""
                        }
        elif col_code and col_name and (col_code in df_codes.columns) and (col_name in df_codes.columns):
            for f, grp in df_codes.groupby(col_f):
                f = norm(f)
                items = []
                for _, rr in grp.iterrows():
                    code = norm(rr[col_code]); name = norm(rr[col_name])
                    if code and name:
                        items.append(f"{code}-{name}")
                    elif code:
                        items.append(code)
                    elif name:
                        items.append(name)
                if not items:
                    continue
                for t in rules:
                    if f in rules[t]:
                        rules[t][f]["enum"] = set(items)
                for t in rules:
                    if f not in rules[t]:
                        rules[t][f] = {
                            "required": False,
                            "type": "",
                            "len_eq": None,
                            "len_max": None,
                            "enum": set(items),
                            "tips": ""
                        }

    return rules

EXACT_LENGTH_FIELDS = {
    "发薪时间": 19,
}

EDU_STRICT_SET = {
    "10-博士研究生","20-硕士研究生","30-大学本科","40-大学专科",
    "50-中专/职高/技校","60-普通高中","70-初中","80-小学及以下"
}

FIELD_SYNONYMS: Dict[str, List[str]] = {
    "是否为专职外部董事": ["是否专职外部董事"],
    "派驻或派出企业名称": ["派驻或派驻出企业名称", "派驻或派驻企业名称"],
    "是否为派出或派驻人员": ["是否为派驻或派出人员","是否派出或派驻人员"]
}

def _get_cell(row: pd.Series, field: str):
    if field in row.index:
        return row[field]
    for alt in FIELD_SYNONYMS.get(field, []):
        if alt in row.index:
            return row[alt]
    return None

def _to_decimal_preserve(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if isinstance(v, float):
            return Decimal(repr(v))
        return Decimal(str(v).strip())
    except Exception:
        return None

def _money_columns_for_table(table_name: str, df_columns: List[str]) -> Set[str]:
    known = {
        "中央企业职工收入情况表": {
            "税前工资性收入","基本薪酬","绩效薪酬及奖金","津补贴","其中：境外工作补贴",
            "延期支付兑现部分","其他一次性专项奖励","中长期激励收入","总收入",
            "工资总额外的福利费用","五险个人缴纳","公积金个人缴纳",
            "补充养老保险个人缴纳","补充医疗保险个人缴纳","其他保险个人缴纳",
            "其他代扣代缴","个人所得税","应扣合计","实发数"
        },
        "中央企业各级单位人工成本情况表": {
            "企业人工成本总额","职工工资总额","社会保险费用","住房公积金","住房补贴",
            "企业年金和职业年金","补充医疗保险","福利费用","劳动保护费","工会经费",
            "教育培训经费","技术奖酬金及业务设计奖","辞退福利","股份支付","其他人工成本","劳务派遣费"
        },
        "中央企业农民工情况表": {
            "农民工总费用","其中：工资总额","其中：各类保险总额",
            "直接签订用工合同农民工费用总额","其中：工资总额.1","其中：各类保险总额.1",
            "劳务派遣形式农民工费用总额","其中：工资总额.2","其中：各类保险总额.2",
            "劳务外包和业务外包农民工费用总额","其中：工资总额.3","其中：各类保险总额.3",
            "其他农民工费用总额","其中：工资总额.4","其中：各类保险总额.4"
        },
    }
    cols = set(known.get(table_name, set()))
    tokens = ("金额","收入","工资","薪酬","费用","补贴","缴纳","合计","实发","应扣","成本","经费","支付","税")
    for c in df_columns:
        if any(tok in str(c) for tok in tokens):
            cols.add(c)
    return {c for c in cols if c in df_columns}

def check_money_precision_errors(df: pd.DataFrame, table: str, pk_cols_map: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    errs: List[Dict[str, Any]] = []
    money_cols = _money_columns_for_table(table, list(df.columns))
    if not money_cols:
        return errs

    def pk_of(i: int) -> str:
        cols = pk_cols_map.get(table, [])
        if not cols:
            return ""
        row = df.iloc[i]
        vals = [norm(row.get(c, "")) for c in cols]
        return "|" + "|".join(vals) if any(vals) else ""

    for idx, row in df.iterrows():
        for col in money_cols:
            val = row[col]
            d = _to_decimal_preserve(val)
            if d is None:
                continue
            q = d.quantize(Decimal("0.01"))
            if (d - q).copy_abs() > Decimal("0"):
                errs.append({
                    "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                    "字段": col, "错误类型": "精度超限",
                    "错误信息": "小数位应≤2位（金额保留到分）",
                    "原始值": str(val), "允许值": "", "建议修复": "四舍五入到2位小数"
                })
    return errs

def _dec(x):
    try:
        s = norm(x)
        if s == "":
            return None
        return Decimal(s)
    except Exception:
        return None

def _round2(x):
    try:
        return float(round(float(x), 2))
    except Exception:
        return None

def _run_tb5_strict(df: pd.DataFrame, table: str, pk_of):
    if "农民工" not in str(table):
        return defaultdict(list), []
    msgs = defaultdict(list); rows = []

    def col(c): return df[c] if c in df.columns else None

    s_total_fee = col("农民工总费用")
    s_dir_fee   = col("直接签订用工合同农民工费用总额")
    s_disp_fee  = col("劳务派遣形式农民工费用总额")
    s_outs_fee  = col("劳务外包和业务外包农民工费用总额")
    s_other_fee = col("其他农民工费用总额")

    gw = col("其中：工资总额"); gi = col("其中：各类保险总额")
    w1 = col("其中：工资总额.1"); i1 = col("其中：各类保险总额.1")
    w2 = col("其中：工资总额.2"); i2 = col("其中：各类保险总额.2")
    w3 = col("其中：工资总额.3"); i3 = col("其中：各类保险总额.3")
    w4 = col("其中：工资总额.4"); i4 = col("其中：各类保险总额.4")

    n = len(df)

    if s_total_fee is not None and gw is not None and gi is not None:
        for i in range(n):
            parts = [_dec(gw.iloc[i]), _dec(gi.iloc[i])]
            if any(p is None for p in parts):
                continue
            expected = _round2(parts[0] + parts[1])
            actual = _round2(_dec(s_total_fee.iloc[i]))
            if expected is None or actual is None:
                continue
            if abs(expected - actual) > 0.01:
                fields = ("农民工总费用","其中：工资总额","其中：各类保险总额")
                msg = f"农民工总费用不等于（其中：工资总额）+（其中：各类保险总额）：应={expected}，实={actual}。"
                msgs[i+2].append(f"[严格-TB5-007-ERROR] [{' | '.join(fields)}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": pk_of(i),
                    "字段": " | ".join(fields),
                    "错误类型": "严格校验-TB5-007-ERROR",
                    "错误信息": msg,
                    "原始值": f"{s_total_fee.iloc[i]} | {gw.iloc[i]} | {gi.iloc[i]}",
                    "允许值": "", "建议修复": ""
                })

    def _pair(fee_s, w_s, i_s, rule_id, fee_name, w_name, i_name):
        if fee_s is None or w_s is None or i_s is None:
            return
        for i in range(n):
            a = _dec(fee_s.iloc[i]); b = _dec(w_s.iloc[i]); c = _dec(i_s.iloc[i])
            if a is None or b is None or c is None:
                continue
            expected = _round2(b + c); actual = _round2(a)
            if expected is None or actual is None:
                continue
            if abs(expected - actual) > 0.01:
                fields = (fee_name, w_name, i_name)
                msg = f"{fee_name}不等于（{w_name}）+（{i_name}）：应={expected}，实={actual}。"
                msgs[i+2].append(f"[严格-{rule_id}-ERROR] [{' | '.join(fields)}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": pk_of(i),
                    "字段": " | ".join(fields),
                    "错误类型": f"严格校验-{rule_id}-ERROR",
                    "错误信息": msg,
                    "原始值": f"{fee_s.iloc[i]} | {w_s.iloc[i]} | {i_s.iloc[i]}",
                    "允许值": "", "建议修复": ""
                })

    _pair(s_dir_fee,  w1, i1, "TB5-003", "直接签订用工合同农民工费用总额", "其中：工资总额.1", "其中：各类保险总额.1")
    _pair(s_disp_fee, w2, i2, "TB5-004", "劳务派遣形式农民工费用总额",   "其中：工资总额.2", "其中：各类保险总额.2")
    _pair(s_outs_fee, w3, i3, "TB5-005", "劳务外包和业务外包农民工费用总额","其中：工资总额.3", "其中：各类保险总额.3")
    _pair(s_other_fee, w4, i4, "TB5-006", "其他农民工费用总额",           "其中：工资总额.4", "其中：各类保险总额.4")

    return msgs, rows

def _run_fk_strict(df: pd.DataFrame, table: str, pk_of):
    need = ["子企业统一社会信用代码","子企业单位名称","所属上级企业统一社会信用代码","所属上级企业名称"]
    if not all(c in df.columns for c in need):
        return defaultdict(list), []
    msgs = defaultdict(list); rows = []
    CC, CN, SC, SN = need
    all_children = {
        (norm(df.at[i, CC]), norm(df.at[i, CN]))
        for i in range(len(df))
        if (norm(df.at[i, CC]) or norm(df.at[i, CN]))
    }
    for i in range(len(df)):
        sup = (norm(df.at[i, SC]), norm(df.at[i, SN]))
        if sup == ("",""):
            continue
        if sup not in all_children:
            fields = (SC, SN)
            msg = "所属上级（代码, 名称）未在子企业集合中严格存在。"
            msgs[i+2].append(f"[严格-FK-ERROR] [{' | '.join(fields)}] {msg}")
            rows.append({
                "表名": table, "行号": i+2, "主键": pk_of(i),
                "字段": " | ".join(fields),
                "错误类型": "严格校验-FK-ERROR",
                "错误信息": msg,
                "原始值": f"{df.at[i, SC]} | {df.at[i, SN]}",
                "允许值": "", "建议修复": ""
            })
    return msgs, rows

def _run_name_level_clean(df: pd.DataFrame, table: str):
    msgs = defaultdict(list); rows = []
    name = "子企业单位名称" if "子企业单位名称" in df.columns else None
    pair = "子企业单位名称（单位代码）" if "子企业单位名称（单位代码）" in df.columns else None
    upname = "所属上级企业名称" if "所属上级企业名称" in df.columns else None
    level = "子企业所属层级" if "子企业所属层级" in df.columns else None

    if name and pair:
        for i in range(len(df)):
            a = norm(df.at[i, name]).replace("（","(").replace("）",")").replace(" ","")
            b = norm(df.at[i, pair]).replace("（","(").replace("）",")").replace(" ","")
            if a.count("(") != b.count("(") or a.count(")") != b.count(")"):
                flds = "子企业单位名称 | 子企业单位名称（单位代码）"
                msg = "括号不一致：单位名称与名称（代码）括号数量/匹配不一致。"
                msgs[i+2].append(f"[清洗-CLEAN-001-WARN] [{flds}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": "",
                    "字段": flds, "错误类型": "清洗-CLEAN-001-WARN",
                    "错误信息": msg,
                    "原始值": f"{df.at[i, name]} | {df.at[i, pair]}",
                    "允许值": "", "建议修复": ""
                })

    if name:
        for i in range(len(df)):
            v = str(df.at[i, name])
            if any(ord(ch) > 65280 for ch in v) or ("　" in v) or ("  " in v):
                flds = "子企业单位名称"
                msg = "全角或空格异常：请统一半角并去除多余空格。"
                msgs[i+2].append(f"[清洗-CLEAN-002-WARN] [{flds}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": "",
                    "字段": flds, "错误类型": "清洗-CLEAN-002-WARN",
                    "错误信息": msg,
                    "原始值": v, "允许值": "", "建议修复": ""
                })

    if level and upname:
        for i in range(len(df)):
            try:
                lv = int(str(df.at[i, level]).split("-")[0])
            except Exception:
                continue
            if lv == 6:
                upn = str(df.at[i, upname])
                if "一级" in upn and "二级" not in upn and "三级" not in upn:
                    flds = "子企业所属层级 | 所属上级企业名称"
                    msg = "层级不一致（四级挂一级）。"
                    msgs[i+2].append(f"[清洗-CLEAN-004-ERROR] [{flds}] {msg}")
                    rows.append({
                        "表名": table, "行号": i+2, "主键": "",
                        "字段": flds, "错误类型": "清洗-CLEAN-004-ERROR",
                        "错误信息": msg,
                        "原始值": f"{df.at[i, level]} | {upn}",
                        "允许值": "", "建议修复": ""
                    })
    return msgs, rows


def _pick_first_col(df, names):
    for name in names:
        if name in df.columns:
            return df[name]
    return None

def _run_staff_income_checks(df: pd.DataFrame, table: str, pk_of):
    """
    职工收入专项
    分拆逻辑：
    - “其他一次性专项奖励*” 这条只管自己；
    - “是否参与中长期激励 / 中长期激励工具 / 中长期激励收入” 三件套自己一组；
    - 总收入≤0 → 备注必填；
    """
    if not (("职工收入" in str(table)) or ("收入情况" in str(table))):
        return defaultdict(list), []
    msgs = defaultdict(list); rows = []

    def col(n): return df[n] if n in df.columns else None
    def dec(v):
        try:
            s = norm(v)
            if s == "":
                return None
            return Decimal(s)
        except Exception:
            return None
    def f2(x):
        try:
            return float(round(float(x),2))
        except Exception:
            return None

    n = len(df)

    c_tax_income = col("税前工资性收入")
    c_basic = col("基本薪酬"); c_perf = col("绩效薪酬及奖金"); c_allow = col("津补贴")
    c_defer = col("延期支付兑现部分"); c_other_bonus = col("其他一次性专项奖励")
    c_other_bonus_name = col("其他一次性专项奖励名称")

    c_overseas = col("其中：境外工作补贴")
    if c_overseas is None:
        c_overseas = col("境外工作补贴")

    c_deduct_total = col("应扣合计")
    deduct_cols = [
        col("五险个人缴纳"), col("公积金个人缴纳"), col("补充养老保险个人缴纳"),
        col("补充医疗保险个人缴纳"), col("其他保险个人缴纳"),
        col("其他代扣代缴"), col("个人所得税")
    ]

    c_long_flag = col("是否参与中长期激励")
    c_long_tools = col("中长期激励工具")
    c_long_income = col("中长期激励收入")

    c_realpay = col("实发数")
    c_total_income = col("总收入")
    c_extra_benefit = col("工资总额外的福利费用")

    c_paytime = col("发薪时间")
    if c_paytime is None:
        c_paytime = col("发薪日期")

    c_post_level = col("岗位层级")
    c_on_job = col("是否在岗")

    c_is_sci = col("是否为科技人员")
    c_is_rnd = _pick_first_col(df, [
        "是否直接从事研发工作",
        "是否直接从事科技研发工作",
        "是否直接从事研发",
    ])

    c_org_pair = col("子企业单位名称（单位代码）")

    # 中长期激励工具允许字符（这里跟下面的字段级校验保持一致）
    allowed_long_tool = set("abcdefghijklmnopqrz|")

    for i in range(n):
        r = i+2; pk = pk_of(i)

        # 1 税前工资性收入 = 各组成
        if c_tax_income is not None and all(x is not None for x in [c_basic,c_perf,c_allow,c_defer,c_other_bonus]):
            parts=[dec(c_basic.iloc[i]), dec(c_perf.iloc[i]), dec(c_allow.iloc[i]), dec(c_defer.iloc[i]), dec(c_other_bonus.iloc[i])]
            if not any(p is None for p in parts):
                ex=f2(sum(parts)); ac=f2(dec(c_tax_income.iloc[i]))
                if ex is not None and ac is not None and abs(ex-ac)>0.01:
                    flds=("税前工资性收入","基本薪酬","绩效薪酬及奖金","津补贴","延期支付兑现部分","其他一次性专项奖励")
                    msg=f"税前工资性收入不等于各组成之和：应={ex}，实={ac}。"
                    msgs[r].append(f"[职收-001-ERROR] [{' | '.join(flds)}] {msg}")
                    rows.append({
                        "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                        "错误类型":"职收-001-ERROR","错误信息":msg,
                        "原始值":f"{c_tax_income.iloc[i]} | {c_basic.iloc[i]} | {c_perf.iloc[i]} | {c_allow.iloc[i]} | {c_defer.iloc[i]} | {c_other_bonus.iloc[i]}",
                        "允许值":"", "建议修复":""
                    })

        # 2 境外工作补贴 <= 津补贴
        if c_overseas is not None and c_allow is not None:
            ov=dec(c_overseas.iloc[i]); al=dec(c_allow.iloc[i])
            if ov is not None and al is not None and ov>al:
                flds=("境外工作补贴","津补贴"); msg="境外工作补贴应不大于津补贴。"
                msgs[r].append(f"[职收-002-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                    "错误类型":"职收-002-ERROR","错误信息":msg,
                    "原始值":f"{c_overseas.iloc[i]} | {c_allow.iloc[i]}",
                    "允许值":"", "建议修复":""
                })

        # 2.1 发薪时间格式
        if c_paytime is not None:
            _pt = str(c_paytime.iloc[i] or "").strip()
            if _pt != "":
                import re as _re_pt
                if not _re_pt.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", _pt):
                    flds = ("发薪时间",)
                    msg = "发薪时间格式应为 YYYY-MM-DD hh:mm:ss（仅半角数字和符号）"
                    msgs[r].append(f"[职收-002A-ERROR] [发薪时间] {msg}")
                    rows.append({
                        "表名": table,
                        "行号": r,
                        "主键": pk,
                        "字段": "发薪时间",
                        "错误类型": "职收-002A-ERROR",
                        "错误信息": msg,
                        "原始值": _pt,
                        "允许值": "例如：2025-09-08 19:27:56",
                        "建议修复": "确认发薪日期并按示例填写"
                    })

        # 3 应扣合计 = 各扣款
        if c_deduct_total is not None and all(x is not None for x in deduct_cols):
            vals=[dec(x.iloc[i]) for x in deduct_cols]
            if not any(v is None for v in vals):
                ex=f2(sum(vals)); ac=f2(dec(c_deduct_total.iloc[i]))
                if ex is not None and ac is not None and abs(ex-ac)>0.01:
                    flds=("应扣合计","五险个人缴纳","公积金个人缴纳","补充养老保险个人缴纳","补充医疗保险个人缴纳","其他保险个人缴纳","其他代扣代缴","个人所得税")
                    msg=f"应扣合计不等于各项扣款之和：应={ex}，实={ac}。"
                    msgs[r].append(f"[职收-003-ERROR] [{' | '.join(flds)}] {msg}")
                    rows.append({
                        "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                        "错误类型":"职收-003-ERROR","错误信息":msg,
                        "原始值":" | ".join([str(c_deduct_total.iloc[i])] + [str(x.iloc[i]) for x in deduct_cols]),
                        "允许值":"", "建议修复":""
                    })

        # 4 其他一次性专项奖励 —— 独立
        if c_other_bonus is not None and c_other_bonus_name is not None:
            amt = dec(c_other_bonus.iloc[i])
            raw_name = c_other_bonus_name.iloc[i]
            name = "" if _is_empty_like(raw_name) else str(raw_name).strip()
            if amt is not None and amt > 0 and name == "":
                flds = ("其他一次性专项奖励","其他一次性专项奖励名称")
                msg = "其他一次性专项奖励>0时必须填写名称。"
                msgs[r].append(f"[职收-004-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名": table, "行号": r, "主键": pk, "字段": " | ".join(flds),
                    "错误类型": "职收-004-ERROR", "错误信息": msg,
                    "原始值": f"{c_other_bonus.iloc[i]} | {raw_name}",
                    "允许值": "", "建议修复": ""
                })
            if (amt is None or amt == 0) and name != "":
                flds = ("其他一次性专项奖励","其他一次性专项奖励名称")
                msg = "其他一次性专项奖励=0时，名称应为空。"
                msgs[r].append(f"[职收-004-WARN] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名": table, "行号": r, "主键": pk, "字段": " | ".join(flds),
                    "错误类型": "职收-004-WARN", "错误信息": msg,
                    "原始值": f"{c_other_bonus.iloc[i]} | {raw_name}",
                    "允许值": "", "建议修复": ""
                })
            # 不再去碰中长期激励工具

        # 5 中长期激励 —— 独立逻辑
        long_flag_val = None if c_long_flag is None else c_long_flag.iloc[i]
        long_tools_val = "" if c_long_tools is None else str(c_long_tools.iloc[i] or "").strip()
        long_inc_val = None if c_long_income is not None else None
        if c_long_income is not None:
            # 原来你的写法里是 dec(...)，这里还是按原来的方式取
            long_inc_val = dec(c_long_income.iloc[i])

        # 5.1 参与=是 → 工具必填
        def _is_true_like(v):
            if v is None:
                return False
            s = str(v).strip()
            if s in ("是", "1", "1-是"):
                return True
            return s.lower() in ("true", "y", "yes")

        def _is_false_like(v):
            if v is None:
                return False
            s = str(v).strip()
            if s in ("否", "0", "2-否"):
                return True
            return s.lower() in ("false", "n", "no")

        def _is_empty_like2(v):
            if v is None:
                return True
            s = str(v).strip()
            if s == "":
                return True
            # 下面这些都当成“没填”
            if s.lower() in ("nan", "none", "null", "na"):
                return True
            if s in ("无", "0", "0-否"):
                return True
            return False

        # 参与 = 是
        if _is_true_like(long_flag_val):
            if _is_empty_like2(long_tools_val):
                flds = ("是否参与中长期激励", "中长期激励工具")
                msg = "已参与中长期激励但未填写中长期激励工具。"
                msgs[r].append(f"[职收-005-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名": table, "行号": r, "主键": pk, "字段": " | ".join(flds),
                    "错误类型": "职收-005-ERROR", "错误信息": msg,
                    "原始值": f"{long_flag_val} | {long_tools_val}",
                    "允许值": "", "建议修复": ""
                })

        # 不参与 = 否 → 只有真写了东西才报
        elif _is_false_like(long_flag_val):
            if not _is_empty_like2(long_tools_val):
                flds = ("是否参与中长期激励", "中长期激励工具")
                msg = "未参与中长期激励但填写了中长期激励工具，请清空。"
                msgs[r].append(f"[职收-005B-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名": table, "行号": r, "主键": pk, "字段": " | ".join(flds),
                    "错误类型": "职收-005B-ERROR", "错误信息": msg,
                    "原始值": f"{long_flag_val} | {long_tools_val}",
                    "允许值": "为空", "建议修复": "清空“中长期激励工具”"
                })

        # 5.2 中长期激励收入>0 → 工具必填（这个逻辑还是保留的）
        if long_inc_val is not None and long_inc_val > 0:
            if _is_empty_like2(long_tools_val):
                flds = ("中长期激励收入", "中长期激励工具")
                msg = "中长期激励收入>0时，中长期激励工具必须填写。"
                msgs[r].append(f"[职收-005A-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名": table, "行号": r, "主键": pk, "字段": " | ".join(flds),
                    "错误类型": "职收-005A-ERROR", "错误信息": msg,
                    "原始值": f"{long_inc_val} | ",
                    "允许值": "填写实际使用的中长期激励工具编码，如 a|b|g", "建议修复": ""
                })

        # 5.3 中长期激励收入>0 → 工具也要填
        if c_long_income is not None:
            inc_val = dec(c_long_income.iloc[i])
            if inc_val is not None and inc_val > 0:
                tools_val = "" if c_long_tools is None else str(c_long_tools.iloc[i] or "").strip()
                if tools_val == "":
                    flds=("中长期激励收入","中长期激励工具")
                    msg="中长期激励收入>0时，中长期激励工具必须填写。"
                    msgs[r].append(f"[职收-005A-ERROR] [{' | '.join(flds)}] {msg}")
                    rows.append({
                        "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                        "错误类型":"职收-005A-ERROR","错误信息":msg,
                        "原始值":f"{c_long_income.iloc[i]} | ",
                        "允许值":"填写实际使用的中长期激励工具编码，如 a|b|g", "建议修复":""
                    })

        # 6 实发数
        if c_realpay is not None and c_total_income is not None and c_deduct_total is not None:
            inc=dec(c_total_income.iloc[i])
            ben=dec(c_extra_benefit.iloc[i]) if c_extra_benefit is not None else None
            ded=dec(c_deduct_total.iloc[i])
            if inc is not None and ded is not None:
                if ben is None:
                    ben = Decimal("0")
                ex=f2(inc+ben-ded); ac=f2(dec(c_realpay.iloc[i]))
                if ex is not None and ac is not None and abs(ex-ac)>0.01:
                    flds=("实发数","总收入","工资总额外的福利费用","应扣合计"); msg=f"实发数不等于总收入+工资总额外的福利费用−应扣合计：应={ex}，实={ac}。"
                    msgs[r].append(f"[职收-006-ERROR] [{' | '.join(flds)}] {msg}")
                    rows.append({
                        "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                        "错误类型":"职收-006-ERROR","错误信息":msg,
                        "原始值":f"{c_realpay.iloc[i]} | {c_total_income.iloc[i]} | {'' if c_extra_benefit is None else c_extra_benefit.iloc[i]} | {c_deduct_total.iloc[i]}",
                        "允许值":"", "建议修复":""
                    })

        # 7 岗位层级=91-其他 → 是否在岗为否类
        if c_post_level is not None and c_on_job is not None:
            lv=str(c_post_level.iloc[i] or "").strip(); on=str(c_on_job.iloc[i] or "").strip()
            if lv.startswith("91") and not any(on.startswith(x) for x in ("2","3","4","5")):
                flds=("岗位层级","是否在岗"); msg="岗位层级=91-其他时，“是否在岗”应为否类（2/3/4/5开头）。"
                msgs[r].append(f"[职收-007-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                    "错误类型":"职收-007-ERROR","错误信息":msg,
                    "原始值":f"{c_post_level.iloc[i]} | {c_on_job.iloc[i]}",
                    "允许值":"", "建议修复":""
                })

        # 8 科技人员一致性
        if c_is_sci is not None and c_is_rnd is not None:
            sci=str(c_is_sci.iloc[i] or "").strip()
            rnd=str(c_is_rnd.iloc[i] or "").strip()
            if sci in ("否","0","false","False","N","n","2-否") and rnd in ("是","1","true","True","Y","y","1-是"):
                flds=("是否为科技人员","是否直接从事研发工作"); msg="“是否为科技人员”=否，但“是否直接从事研发工作”=是，存在矛盾，请统一口径。"
                msgs[r].append(f"[职收-008-ERROR] [{' | '.join(flds)}] {msg}")
                rows.append({
                    "表名":table,"行号":r,"主键":pk,"字段":" | ".join(flds),
                    "错误类型":"职收-008-ERROR","错误信息":msg,
                    "原始值":f"{c_is_sci.iloc[i]} | {c_is_rnd.iloc[i]}",
                    "允许值":"", "建议修复":""
                })

        # 9 子企业单位名称（单位代码）
        if c_org_pair is not None:
            val=str(c_org_pair.iloc[i] or "").strip()
            if val == "":
                flds = ("子企业单位名称（单位代码）",)
                msg = "子企业单位名称（单位代码）不得为空，请按组织基础库信息准确填写。"
                msgs[r].append(f"[职收-009-ERROR] [{flds[0]}] {msg}")
                rows.append({
                    "表名":table,"行号":r,"主键":pk,"字段":flds[0],
                    "错误类型":"职收-009-ERROR","错误信息":msg,
                    "原始值":"", "允许值":"", "建议修复":""
                })

        # 10 总收入≤0 → 备注必填
        if c_total_income is not None:
            _inc = dec(c_total_income.iloc[i])
            if _inc is not None and _inc <= 0:
                remark_cols = [c for c in ("备注","备注/说明","说明","备注说明") if c in df.columns]
                _has = False
                _vals = []
                for rc in remark_cols:
                    rv_raw = df[rc].iloc[i]
                    rv = "" if rv_raw is None else str(rv_raw).strip()
                    if rv.lower() in ("nan", "none", "null"):
                        rv = ""
                    _vals.append(rv)
                    if rv != "":
                        _has = True
                if not _has:
                    flds = ("总收入", "备注/说明")
                    msg = "总收入为0或为负数，必须在备注/说明中填写原因。"
                    msgs[r].append(f"[职收-010-ERROR] [{' | '.join(flds)}] {msg}")
                    rows.append({
                        "表名": table,
                        "行号": r,
                        "主键": pk,
                        "字段": " | ".join(flds),
                        "错误类型": "职收-010-ERROR",
                        "错误信息": msg,
                        "原始值": f"{c_total_income.iloc[i]} | {' | '.join(_vals)}",
                        "允许值": "请填写如：停薪留职、当期无实发、长期病假、年度一次性计提等原因",
                        "建议修复": "在备注或说明列中补充0或负数的具体原因"
                    })

        # 11 中长期激励工具格式
        if c_long_tools is not None:
            val=str(c_long_tools.iloc[i] or "").strip()
            if val != "":
                if not all(ch in allowed_long_tool for ch in val):
                    flds=("中长期激励工具",)
                    msg="中长期激励工具格式不正确，应由abcdefghijklmnopqrz及符号|组成（多项示例：a|b|g）。"
                    msgs[r].append(f"[职收-011-ERROR] [{flds[0]}] {msg}")
                    rows.append({
                        "表名":table,"行号":r,"主键":pk,"字段":flds[0],
                        "错误类型":"职收-011-ERROR","错误信息":msg,
                        "原始值":val, "允许值":"", "建议修复":""
                    })

    return msgs, rows

def _norm_str_for_fk(v):
    try:
        s = str(v).strip()
    except Exception:
        s = ""
    return s.replace("｜", "|")

def build_master_pairs(dfs: Dict[str, pd.DataFrame]):
    MASTER = "中央企业各级次单位信息情况表"
    if MASTER not in dfs:
        return set()
    dfm = dfs[MASTER]
    code_cols = ["子企业统一社会信用代码","统一社会信用代码","社会信用代码"]
    name_cols = ["子企业单位名称","子企业名称","单位名称","企业名称"]
    code_col = next((c for c in code_cols if c in dfm.columns), None)
    name_col = next((c for c in name_cols if c in dfm.columns), None)
    if not code_col or not name_col:
        return set()
    pairs = set()
    for _, r in dfm.iterrows():
        c = _norm_str_for_fk(r.get(code_col, ""))
        n = _norm_str_for_fk(r.get(name_col, ""))
        if c and n:
            pairs.add((c, n))
    return pairs

def cross_check_fk_merged(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    out_rows = []
    master_pairs = build_master_pairs(dfs)
    if not master_pairs:
        return pd.DataFrame([{"提示":"未找到主数据的名称+代码主键集合，已跳过"}])

    child_code_cands = ["子企业统一社会信用代码","统一社会信用代码","社会信用代码"]
    child_name_cands = ["子企业单位名称","子企业名称","单位名称","企业名称"]

    sup_code_cands = ["所属上级企业统一社会信用代码","上级单位统一社会信用代码","上级企业统一社会信用代码","上级统一社会信用代码"]
    sup_name_cands = ["所属上级企业单位名称","所属上级企业名称","上级单位名称","上级企业单位名称"]

    for t, df in dfs.items():
        c_code = next((c for c in child_code_cands if c in df.columns), None)
        c_name = next((c for c in child_name_cands if c in df.columns), None)
        if c_code and c_name:
            for idx, r in df.iterrows():
                code = _norm_str_for_fk(r.get(c_code))
                name = _norm_str_for_fk(r.get(c_name))
                if not code or not name:
                    out_rows.append({"表名":t,"行号":idx+2,"检查项":"子企业名称+代码","问题":"引用缺失（名称或代码为空）","名称":name,"统一社会信用代码":code})
                elif (code, name) not in master_pairs:
                    out_rows.append({"表名":t,"行号":idx+2,"检查项":"子企业名称+代码","问题":"不在主数据集合（名称+代码必须同时匹配）","名称":name,"统一社会信用代码":code})

        if t == "中央企业职工收入情况表":
            s_code = next((c for c in sup_code_cands if c in df.columns), None)
            s_name = next((c for c in sup_name_cands if c in df.columns), None)
            if s_code and s_name:
                for idx, r in df.iterrows():
                    sc = _norm_str_for_fk(r.get(s_code))
                    sn = _norm_str_for_fk(r.get(s_name))
                    if not sc or not sn:
                        out_rows.append({"表名":t,"行号":idx+2,"检查项":"所属上级企业名称+代码","问题":"引用缺失（名称或代码为空）","名称":sn,"统一社会信用代码":sc})
                    elif (sc, sn) not in master_pairs:
                        out_rows.append({"表名":t,"行号":idx+2,"检查项":"所属上级企业名称+代码","问题":"不在主数据集合（名称+代码必须同时匹配）","名称":sn,"统一社会信用代码":sc})

    if out_rows:
        return pd.DataFrame(out_rows)[["表名","行号","检查项","问题","名称","统一社会信用代码"]]
    else:
        return pd.DataFrame(columns=["表名","行号","检查项","问题","名称","统一社会信用代码"])

def cross_check_employee_ops(df_emp: pd.DataFrame) -> pd.DataFrame:
    errs: List[Dict[str, Any]] = []
    need = ["统计年月","子企业统一社会信用代码","操作类型"]
    if not all(c in df_emp.columns for c in need):
        return pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"])
    for (ym, code), grp in df_emp.groupby(["统计年月","子企业统一社会信用代码"], dropna=False):
        vals = grp["操作类型"].astype(str).str.strip().tolist()
        vals = [v for v in vals if v and v.lower() != "nan"]
        if len(set(vals)) > 1:
            rows = (grp.index + 2).tolist()
            errs.append({
                "统计年月": ym,
                "子企业统一社会信用代码": code,
                "问题描述": "同一子企业在同一统计年月的【操作类型】不一致",
                "涉及行号": ",".join(map(str, rows)),
                "涉及取值": " | ".join(sorted(set(vals)))
            })
    return pd.DataFrame(errs)

def cross_check_fk_pairs(dfs: Dict[str, pd.DataFrame], writer) -> None:
    MASTER = "中央企业各级次单位信息情况表"
    KEY_CODE = "子企业统一社会信用代码"
    KEY_NAME = "子企业单位名称"
    SUP_CODE = "所属上级企业统一社会信用代码"
    SUP_NAME = "所属上级企业单位名称"

    if MASTER not in dfs:
        pd.DataFrame([{"提示":"未找到主数据表，已跳过"}]).to_excel(writer, index=False, sheet_name="表间-主键&上级-提示")
        return

    dfm = dfs[MASTER]
    if not all(c in dfm.columns for c in [KEY_CODE, KEY_NAME]):
        pd.DataFrame([{"提示":"主数据缺少代码/名称列，已跳过"}]).to_excel(writer, index=False, sheet_name="表间-主键&上级-提示")
        return

    def _n(x):
        try: return str(x).strip()
        except Exception: return ""

    master_pairs = set()
    master_super = {}
    for _, r in dfm.iterrows():
        c = _n(r.get(KEY_CODE,"")); n = _n(r.get(KEY_NAME,""))
        if c and n:
            master_pairs.add((c,n))
            up_c = _n(r.get(SUP_CODE,"")); up_n = _n(r.get(SUP_NAME,""))
            if up_c and up_n:
                master_super[(c,n)] = (up_c, up_n)

    for t, df in dfs.items():
        if t == MASTER:
            continue
        has_pair = all(c in df.columns for c in [KEY_CODE, KEY_NAME])
        has_super = all(c in df.columns for c in [KEY_CODE, KEY_NAME, SUP_CODE, SUP_NAME])
        rows = []
        for idx, r in df.iterrows():
            c = _n(r.get(KEY_CODE,"")); n = _n(r.get(KEY_NAME,""))
            sc = _n(r.get(SUP_CODE,"")); sn = _n(r.get(SUP_NAME,""))
            if has_pair:
                if not c or not n:
                    rows.append({
                        KEY_CODE:c, KEY_NAME:n, SUP_CODE:sc, SUP_NAME:sn,
                        "行号":idx+2, "问题描述":"引用缺失（子企业代码+名称必须同时填写）",
                        "期望上级(代码)":"", "期望上级(名称)":" "
                    })
                elif (c,n) not in master_pairs:
                    rows.append({
                        KEY_CODE:c, KEY_NAME:n, SUP_CODE:sc, SUP_NAME:sn,
                        "行号":idx+2, "问题描述":"不在主数据（子企业代码+名称）集合中",
                        "期望上级(代码)":"", "期望上级(名称)":" "
                    })
            if has_super and c and n and (sc or sn):
                exp = master_super.get((c,n))
                if exp:
                    ec, en = exp
                    if not (sc == ec and sn == en):
                        rows.append({
                            KEY_CODE:c, KEY_NAME:n, SUP_CODE:sc, SUP_NAME:sn,
                            "行号":idx+2, "问题描述":"所属上级不一致（需与主表完全一致：代码+名称均相同）",
                            "期望上级(代码)":ec, "期望上级(名称)":en
                        })
                else:
                    rows.append({
                        KEY_CODE:c, KEY_NAME:n, SUP_CODE:sc, SUP_NAME:sn,
                        "行号":idx+2, "问题描述":"主表未提供该单位的上级信息，仅提示",
                        "期望上级(代码)":"", "期望上级(名称)":" "
                    })

        sheet = f"表间-主键&上级-{t}"[:31]
        if rows:
            pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet)
        else:
            pd.DataFrame(columns=[KEY_CODE,KEY_NAME,SUP_CODE,SUP_NAME,"行号","问题描述","期望上级(代码)","期望上级(名称)"]).to_excel(
                writer, index=False, sheet_name=sheet
            )

def validate_dataframe(df: pd.DataFrame,
                       table: str,
                       rules: Dict[str, Dict[str, Any]],
                       length_mode: str = "max",
                       pk_map: Optional[Dict[str, List[str]]] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:

    def pk_of(row_idx: int) -> str:
        cols: List[str] = []
        if pk_map:
            if table in pk_map:
                cols = pk_map[table]
            else:
                for k in pk_map:
                    if (k in table) or (table in k):
                        cols = pk_map[k]; break
        if not cols:
            candidates = [
                ["统一社会信用代码","证件号码","姓名"],
                ["统一社会信用代码","姓名"],
                ["证件号码","姓名"],
                ["子企业统一社会信用代码","子企业单位名称"],
                ["统一社会信用代码"],
            ]
            for group in candidates:
                if all(c in df.columns for c in group):
                    cols = group; break
        parts = []
        for c in cols:
            v = df.at[row_idx, c] if c in df.columns else ""
            parts.append(norm(v))
        parts = [p for p in parts if p != ""]
        return "|" + "|".join(parts) if parts else ""

    errors: List[Dict[str, Any]] = []
    anno_col = "__校验错误__"
    if anno_col not in df.columns:
        df[anno_col] = ""

    strict_msgs = defaultdict(list); strict_rows = []
    clean_msgs = defaultdict(list);  clean_rows = []

    tb5_msgs, tb5_rows = _run_tb5_strict(df, table, pk_of)
    for k, v in tb5_msgs.items():
        strict_msgs[k].extend(v)
    strict_rows.extend(tb5_rows)

    fk_msgs, fk_rows = _run_fk_strict(df, table, pk_of)
    for k, v in fk_msgs.items():
        strict_msgs[k].extend(v)
    strict_rows.extend(fk_rows)

    cl_msgs, cl_rows = _run_name_level_clean(df, table)
    for k, v in cl_msgs.items():
        clean_msgs[k].extend(v)
    clean_rows.extend(cl_rows)

    staff_msgs, staff_rows = _run_staff_income_checks(df, table, pk_of)
    for k, v in staff_msgs.items():
        strict_msgs[k].extend(v)
    strict_rows.extend(staff_rows)

    n = len(df)
    for idx in range(n):
        row = df.iloc[idx]
        row_msgs: List[str] = []

        row_msgs.extend(strict_msgs.get(idx+2, []))
        row_msgs.extend(clean_msgs.get(idx+2, []))

        for field, fr in rules.items():
            raw_val = _get_cell(row, field)
            sval = norm(raw_val)

            if fr.get("required") and sval == "":
                if not (table == "中央企业职工收入情况表" and field == "派驻或派出企业名称"):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "缺失", "错误信息": "不能为空",
                        "原始值": sval, "允许值": ""
                    })
                    row_msgs.append(f"[{field}] 不能为空")
                    continue

            if sval == "":
                continue

            if field in EXACT_LENGTH_FIELDS:
                L = EXACT_LENGTH_FIELDS[field]
                if len(sval) != L:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "长度不符",
                        "错误信息": f"长度应={L}",
                        "原始值": sval, "允许值": f"长度={L}"
                    })
                    row_msgs.append(f"[{field}] 长度应={L}")
            else:
                len_eq = fr.get("len_eq")
                len_max = fr.get("len_max")
                if length_mode == "max" and len_eq is not None:
                    len_max = max(len_max or 0, len_eq)
                    len_eq = None
                if len_eq is not None and len(sval) != int(len_eq):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "长度不符",
                        "错误信息": f"长度应={len_eq}", "原始值": sval, "允许值": f"长度={len_eq}"
                    })
                    row_msgs.append(f"[{field}] 长度应={len_eq}")
                if len_max is not None and len(sval) > int(len_max):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "长度超限",
                        "错误信息": f"长度应≤{len_max}", "原始值": sval, "允许值": f"长度≤{len_max}"
                    })
                    row_msgs.append(f"[{field}] 长度应≤{len_max}")

            if field == "最高学历":
                if sval not in EDU_STRICT_SET:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "取值非法",
                        "错误信息": "取值不在枚举中", "原始值": sval,
                        "允许值": " | ".join(sorted(EDU_STRICT_SET))
                    })
                    row_msgs.append(f"[{field}] 取值不在枚举中")

            enum_set: Set[str] = set(rules.get(field, {}).get("enum") or fr.get("enum") or [])
            # ★这里加了一个特判：中长期激励工具我们不用规则表的枚举
            if field == "中长期激励工具":
                # 只要是 a~q / r / z / | 组成就过；具体格式检查在表内逻辑已经做了
                ok = all(ch in "abcdefghijklmnopqrz|" for ch in sval)
                if not ok:
                    errors.append({
                        "表名": table,
                        "行号": idx + 2,
                        "主键": pk_of(idx),
                        "字段": field,
                        "错误类型": "取值非法",
                        "错误信息": "中长期激励工具格式不正确，应由abcdefghijklmnopqrz及符号|组成",
                        "原始值": sval,
                        "允许值": "如 a 或 a|b|g"
                    })
                    row_msgs.append(f"[{field}] 中长期激励工具格式不正确")
            elif field == "上市类型":
                msg_lt = check_listing_type(sval)
                if msg_lt:
                    errors.append({
                        "表名": table,
                        "行号": idx + 2,
                        "主键": pk_of(idx),
                        "字段": field,
                        "错误类型": "取值非法",
                        "错误信息": msg_lt,
                        "原始值": sval,
                        "允许值": "a~i，用 | 分隔；i 需单独出现"
                    })
                    row_msgs.append(f"[{field}] {msg_lt}")
            elif enum_set:
                if sval not in enum_set:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "取值非法",
                        "错误信息": "取值不在枚举中", "原始值": sval,
                        "允许值": " | ".join(sorted(enum_set))
                    })
                    row_msgs.append(f"[{field}] 取值不在枚举中")

        if table == "中央企业各级单位人工成本情况表":
            HR_COST_SUM_FIELDS = [
                "职工工资总额","社会保险费用","住房公积金","住房补贴","企业年金和职业年金",
                "补充医疗保险","福利费用","劳动保护费","工会经费","教育培训经费",
                "技术奖酬金及业务设计奖","辞退福利","股份支付","其他人工成本","劳务派遣费"
            ]
            total = try_decimal(_get_cell(row, "企业人工成本总额"))
            if total is not None:
                subs = [try_decimal(_get_cell(row, c)) or Decimal("0") for c in HR_COST_SUM_FIELDS]
                sumv = sum(subs, Decimal("0"))
                if total < sumv:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "企业人工成本总额", "错误类型": "不满足下限",
                        "错误信息": f"应≥ 明细之和（{HR_COST_SUM_FIELDS}）",
                        "原始值": str(total), "允许值": f"期望≥{sumv}"
                    })
                    row_msgs.append(f"[企业人工成本总额] 应≥明细之和（期望≥{sumv} 实际{total}）")

        if table == "中央企业农民工情况表":
            tol = Decimal("0.01")
            g_total = try_decimal(_get_cell(row, "农民工总费用"))
            g_w = try_decimal(_get_cell(row, "其中：工资总额"))
            g_i = try_decimal(_get_cell(row, "其中：各类保险总额"))
            if all(v is not None for v in [g_total,g_w,g_i]):
                expect1 = (g_w or Decimal("0")) + (g_i or Decimal("0"))
                if (g_total - expect1).copy_abs() > tol:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "农民工总费用", "错误类型": "等式不满足",
                        "错误信息": "农民工总费用应=其中：工资总额+其中：各类保险总额",
                        "原始值": str(g_total), "允许值": f"期望={expect1}"
                    })
                    row_msgs.append(f"[农民工总费用] 应=工资总额+各类保险总额（期望={expect1} 实际{g_total}）")

            parts_total = sum([
                try_decimal(_get_cell(row, "直接签订用工合同农民工费用总额")) or Decimal("0"),
                try_decimal(_get_cell(row, "劳务派遣形式农民工费用总额")) or Decimal("0"),
                try_decimal(_get_cell(row, "劳务外包和业务外包农民工费用总额")) or Decimal("0"),
                try_decimal(_get_cell(row, "其他农民工费用总额")) or Decimal("0"),
            ], Decimal("0"))
            if g_total is not None and (g_total - parts_total).copy_abs() > tol:
                errors.append({
                    "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                    "字段": "农民工总费用", "错误类型": "等式不满足",
                    "错误信息": "农民工总费用应=四类费用合计",
                    "原始值": str(g_total), "允许值": f"期望={parts_total}"
                })
                row_msgs.append(f"[农民工总费用] 应=四类费用合计（期望={parts_total} 实际={g_total}）")

        if row_msgs:
            df.at[idx, anno_col] = "；".join([m for m in row_msgs if m])

    if strict_rows:
        errors.extend(strict_rows)
    if clean_rows:
        errors.extend(clean_rows)

    err_df = pd.DataFrame(errors, columns=[
        "表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"
    ])

    return err_df, df

def parse_pk_map(pk_arg: Optional[str]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not pk_arg:
        return out
    for seg in re.split(r"[;|]", pk_arg):
        seg = seg.strip()
        if not seg or ":" not in seg:
            continue
        t, cols = seg.split(":", 1)
        t = t.strip()
        cols_list = [c.strip() for c in re.split(r"[，,]", cols) if c.strip()]
        if t and cols_list:
            out[t] = cols_list
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="待校验数据：Excel 文件或目录")
    ap.add_argument("--rules-xlsx", required=True, help="规则Excel（V2.x）")
    ap.add_argument("--sheet", default="央企端-表内校验", help="规则Sheet名")
    ap.add_argument("--codes-sheet", default="码值", help="码值Sheet名")
    ap.add_argument("--length-mode", choices=["max","strict"], default="max")
    ap.add_argument("--pk", default="", help="主键映射，如：中央企业职工收入情况表:统一社会信用代码,证件号码,姓名")
    ap.add_argument("--output", required=True, help="输出Excel路径")
    args = ap.parse_args()

    dfs = read_data_any(args.data)
    rules_all = compile_rules_from_excel(Path(args.rules_xlsx),
                                         sheet_name=args.sheet,
                                         codes_sheet=args.codes_sheet)
    pk_map = parse_pk_map(args.pk)

    try:
        import xlsxwriter
        engine = "xlsxwriter"
    except Exception:
        engine = None

    with pd.ExcelWriter(args.output, engine=engine) as xw:
        try:
            merged = cross_check_fk_merged(dfs)
            merged.to_excel(xw, index=False, sheet_name="表间-一致性检查"[:31])
        except Exception as e:
            pd.DataFrame([{"提示":f"表间-一致性检查异常：{e}"}]).to_excel(xw, index=False, sheet_name="表间-一致性检查"[:31])

        try:
            cross_check_fk_pairs(dfs, xw)
        except Exception:
            pass

        all_errs = []

        for t, df in dfs.items():
            table = normalize_table_name(t)
            table_rules = rules_all.get(table, {})
            err_df, annotated_df = validate_dataframe(df.copy(), table, table_rules,
                                                      length_mode=args.length_mode,
                                                      pk_map=pk_map)

            try:
                prec_errs = check_money_precision_errors(annotated_df, table, pk_map)
                if prec_errs:
                    err_df = pd.concat([err_df, pd.DataFrame(prec_errs)], ignore_index=True)
            except Exception:
                pass

            err_sheet = f"错误-{table}"[:31]
            if err_df.empty:
                pd.DataFrame(columns=[
                    "表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"
                ]).to_excel(xw, index=False, sheet_name=err_sheet)
            else:
                tmp = err_df.copy()
                tmp.insert(0, "源表", table)
                all_errs.append(tmp)
                err_df.to_excel(xw, index=False, sheet_name=err_sheet)

            annotated_df.to_excel(xw, index=False, sheet_name=f"标注-{table}"[:31])

        if "中央企业职工收入情况表" in dfs:
            cross_df = cross_check_employee_ops(dfs["中央企业职工收入情况表"])
            sh = "跨行-中央企业职工收入"[:31]
            if cross_df.empty:
                pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"]).to_excel(xw, index=False, sheet_name=sh)
            else:
                cross_df.to_excel(xw, index=False, sheet_name=sh)

        try:
            if all_errs:
                pd.concat(all_errs, ignore_index=True).to_excel(xw, index=False, sheet_name="错误汇总")
            else:
                pd.DataFrame(columns=["源表","表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"]).to_excel(
                    xw, index=False, sheet_name="错误汇总"
                )
        except Exception:
            pass

    print(f"完成：输出 {args.output}")

if __name__ == "__main__":
    sys.exit(main())
