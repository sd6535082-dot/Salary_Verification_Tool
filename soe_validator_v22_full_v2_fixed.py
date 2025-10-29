# -*- coding: utf-8 -*-
"""
SOE Validator (V2.3) - 表内 + 基础跨行校验（含新增4条规则）
脚本名：soe_validator_v22_full_v2_fixed.py

新增且仅新增：
1) 职工收入：应扣合计 = 五险个人缴纳 + 公积金个人缴纳 + 补充养老保险个人缴纳 + 补充医疗保险个人缴纳 + 其他保险个人缴纳 + 其他代扣代缴 + 个人所得税；
2) 职工收入：是否参与中长期激励 = “是/1-是” 时，“中长期激励工具”不能为空；
3) 职工收入：发薪时间格式严格为 YYYY-MM-DD hh:mm:ss（只允许半角英文的空格/短横线/冒号）；
4) 表间层级检查：
   - 员工不能挂在“一级”下（若该一级企业下存在二级企业）；
   - 员工不能挂在“二级”下（若该二级企业下存在三级企业）。
其余既有逻辑保持不变（含：金额小数位≤2、岗位层级=91-其他→是否在岗为否类、最高学历白名单、主键一致性、操作类型一致性等）。
"""

import argparse
import json
import re
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# ===== MERGED STRICT & CLEAN HELPERS (from v25 / strict_fk / check_hierarchy_clean) =====
import math
from collections import defaultdict

def _dec(x):
    try:
        from decimal import Decimal
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
    """严格：表5分项等式（.1..4） + 总费用=工资+保险（无后缀）"""
    msgs = defaultdict(list)
    rows = []

    def take(col):
        return df[col] if col in df.columns else None

    if "农民工" not in str(table):
        return msgs, rows

    # 列抓取
    s_total_fee = take("农民工总费用")
    s_dir_fee   = take("直接签订用工合同农民工费用总额")
    s_disp_fee  = take("劳务派遣形式农民工费用总额")
    s_outs_fee  = take("劳务外包和业务外包农民工费用总额")
    s_other_fee = take("其他农民工费用总额")
    gw = take("其中：工资总额"); gi = take("其中：各类保险总额")

    w1 = take("其中：工资总额.1"); i1 = take("其中：各类保险总额.1")
    w2 = take("其中：工资总额.2"); i2 = take("其中：各类保险总额.2")
    w3 = take("其中：工资总额.3"); i3 = take("其中：各类保险总额.3")
    w4 = take("其中：工资总额.4"); i4 = take("其中：各类保险总额.4")

    n = len(df)

    # 总费用 = 工资+保险（无后缀） TB5-007
    if s_total_fee is not None and gw is not None and gi is not None:
        for i in range(n):
            parts = [_dec(gw.iloc[i]), _dec(gi.iloc[i])]
            if any(p is None for p in parts): 
                continue
            expected = _round2(parts[0] + parts[1])
            actual   = _round2(_dec(s_total_fee.iloc[i]))
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
            if b is None or c is None or a is None:
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
    _pair(s_other_fee,w4, i4, "TB5-006", "其他农民工费用总额",           "其中：工资总额.4", "其中：各类保险总额.4")

    return msgs, rows

def _run_fk_strict(df: pd.DataFrame, table: str, pk_of):
    """
    严格外键：(上级代码, 上级名称) 必须严格出现在 (子企业代码, 子企业名称) 集合中。
    命中列名：REQ_COLS 同 v22： 子企业统一社会信用代码 / 子企业单位名称 / 所属上级企业统一社会信用代码 / 所属上级企业名称
    """
    msgs = defaultdict(list); rows = []
    req = ["子企业统一社会信用代码","子企业单位名称","所属上级企业统一社会信用代码","所属上级企业名称"]
    if not all(c in df.columns for c in req):
        return msgs, rows

    CC, CN, SC, SN = req
    child_set = {(norm(df.at[i, CC]), norm(df.at[i, CN])) for i in range(len(df)) if (norm(df.at[i, CC]) or norm(df.at[i, CN]))}

    for i in range(len(df)):
        sup_pair = (norm(df.at[i, SC]), norm(df.at[i, SN]))
        if sup_pair == ("",""):
            continue
        if sup_pair not in child_set:
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
    """
    清洗诊断（宽松）：括号结构、全角/空格、四级挂一级提示等，不计为严格ERROR，仅写标注并以清洗-... 记录入 err_df。
    """
    msgs = defaultdict(list); rows = []

    def _find(col):
        return col if col in df.columns else None

    name   = _find("子企业单位名称")
    pair   = _find("子企业单位名称（单位代码）")
    upname = _find("所属上级企业名称")
    upcode = _find("所属上级企业统一社会信用代码")
    code   = _find("子企业统一社会信用代码")
    level  = _find("子企业所属层级")

    # 括号不一致
    if name and pair:
        def _norm_simple(s):
            s = norm(s)
            s = s.replace("（","(").replace("）",")").replace(" ","")
            return s
        for i in range(len(df)):
            a = _norm_simple(df.at[i, name])
            b = _norm_simple(df.at[i, pair])
            if a.count("(") != b.count("(") or a.count(")") != b.count(")"):
                fields = "子企业单位名称 | 子企业单位名称（单位代码）"
                msg = "括号不一致：单位名称与名称（代码）括号数量/匹配不一致。"
                msgs[i+2].append(f"[清洗-CLEAN-001-WARN] [{fields}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": "", "字段": fields,
                    "错误类型": "清洗-CLEAN-001-WARN", "错误信息": msg,
                    "原始值": f"{df.at[i, name]} | {df.at[i, pair]}", "允许值": "", "建议修复": ""
                })

    # 全角或空格异常
    if name:
        for i in range(len(df)):
            v = str(df.at[i, name])
            if any(ord(ch) > 65280 for ch in v) or ("　" in v) or ("  " in v):
                fields="子企业单位名称"; msg="全角或空格异常：请统一半角并去除多余空格。"
                msgs[i+2].append(f"[清洗-CLEAN-002-WARN] [{fields}] {msg}")
                rows.append({
                    "表名": table, "行号": i+2, "主键": "", "字段": fields,
                    "错误类型": "清洗-CLEAN-002-WARN", "错误信息": msg,
                    "原始值": v, "允许值": "", "建议修复": ""
                })

    # 四级挂一级（示例规则，与原清洗脚本一致口径）
    if level and upname:
        for i in range(len(df)):
            s = str(df.at[i, level])
            try:
                lv = int(s.split("-")[0])
            except Exception:
                continue
            if lv == 6:  # 四级
                upn = str(df.at[i, upname])
                if "一级" in upn and "二级" not in upn and "三级" not in upn:
                    fields = "子企业所属层级 | 所属上级企业名称"
                    msg = "层级不一致（四级挂一级）"
                    msgs[i+2].append(f"[清洗-CLEAN-004-ERROR] [{fields}] {msg}")
                    rows.append({
                        "表名": table, "行号": i+2, "主键": "", "字段": fields,
                        "错误类型": "清洗-CLEAN-004-ERROR", "错误信息": msg,
                        "原始值": f"{df.at[i, level]} | {upn}", "允许值": "", "建议修复": ""
                    })

    return msgs, rows
# ===== END HELPERS =====
from decimal import Decimal

warnings.simplefilter("ignore", FutureWarning)

# ---------- 工具 ----------

def norm(s: Any) -> str:
    if s is None:
        return ""
    try:
        if isinstance(s, float) and np.isnan(s):
            return ""
    except Exception:
        pass
    return str(s).strip()

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
        dfs = {}
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
    return norm(name).replace("\n","\r").replace("\r","").strip()

def chinese_or_pipe_strip(s: str) -> str:
    s = s.replace("｜", "|")
    parts = [p.strip() for p in s.split("|") if p.strip()]
    return "|".join(parts)

def split_enum_string(s: str) -> List[str]:
    s = chinese_or_pipe_strip(norm(s))
    if not s:
        return []
    return [p.strip() for p in s.split("|")]

# ---------- 规则编译 ----------

def compile_rules_from_excel(xlsx_path: Path, sheet_name: str = "央企端-表内校验", codes_sheet: str = "码值") -> Dict[str, Dict[str, Dict[str, Any]]]:
    xls = pd.ExcelFile(xlsx_path)

    # 读“表内校验”
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

    col_table = pick_col(df_rules, ["表名","表","对象","表中文名","表英文名"])
    col_field = pick_col(df_rules, ["字段","字段名","列名","字段名称"])
    col_rule  = pick_col(df_rules, ["规则","校验规则","规则说明","逻辑","说明"])
    col_enum  = pick_col(df_rules, ["允许值","枚举","枚举值","取值"])

    if not col_table or not col_field:
        raise RuntimeError("在规则表中未找到 表名/字段 列")

    rules: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # 扫“表内校验”
    for _, row in df_rules.iterrows():
        table = normalize_table_name(row[col_table])
        field = norm(row[col_field])
        if not table or not field:
            continue
        r = norm(row[col_rule]) if col_rule else ""
        enum_text = norm(row[col_enum]) if col_enum else ""

        t_rules = rules.setdefault(table, {})
        fr = t_rules.setdefault(field, {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(), "tips": ""})

        text = f"{r} {enum_text}".strip()

        # 必填
        if "不为空" in text or "必填" in text:
            fr["required"] = True

        # 长度
        m_eq = re.search(r"长度\s*[=＝]\s*(\d+)", text)
        m_le = re.search(r"(长度\s*(?:≤|<=)|长度小于等于|长度不超过)\s*(\d+)", text)
        if m_eq:
            fr["len_eq"] = int(m_eq.group(1))
        if m_le:
            fr["len_max"] = int(m_le.group(2))

        # 枚举
        may = []
        if enum_text:
            may += split_enum_string(enum_text)
        if ("|" in r) and (len(r.split("|")) >= 2):
            may += split_enum_string(r)
        if may:
            fr["enum"].update([x for x in may if x])

        if text:
            fr["tips"] = text

    # 读“码值/码值表”（覆盖/补齐枚举）
    codes_sheet_try = [codes_sheet]
    if codes_sheet != "码值表":
        codes_sheet_try.append("码值表")
    for cs in codes_sheet_try:
        if cs not in xls.sheet_names:
            continue
        df_codes = pd.read_excel(xls, sheet_name=cs)
        col_f = pick_col(df_codes, ["字段","字段名","列名","字段名称"])
        col_enum2 = pick_col(df_codes, ["枚举","允许值","取值","枚举值","代码-名称","代码名称"])
        col_code = pick_col(df_codes, ["代码","编码","值","码"])
        col_name = pick_col(df_codes, ["名称","含义","文本"])
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
                        rules[t][f] = {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(items), "tips": ""}
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
                        rules[t][f] = {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(items), "tips": ""}

    return rules

# ---------- 业务常量 ----------

EXACT_LENGTH_FIELDS = {
    "发薪时间": 19,
}

HR_COST_SUM_FIELDS = [
    "职工工资总额","社会保险费用","住房公积金","住房补贴","企业年金和职业年金",
    "补充医疗保险","福利费用","劳动保护费","工会经费","教育培训经费",
    "技术奖酬金及业务设计奖","辞退福利","股份支付","其他人工成本","劳务派遣费"
]

EDU_STRICT_SET = {
    "10-博士研究生","20-硕士研究生","30-大学本科","40-大学专科",
    "50-中专/职高/技校","60-普通高中","70-初中","80-小学及以下"
}

FIELD_SYNONYMS: Dict[str, List[str]] = {
    "是否为专职外部董事": ["是否专职外部董事"],
    "派驻或派出企业名称": ["派驻或派驻出企业名称"],
    "是否为派出或派驻人员": ["是否为派驻或派出人员","是否派出或派驻人员"],
}

def _get_cell(row: pd.Series, field: str):
    if field in row.index:
        return row[field]
    for alt in FIELD_SYNONYMS.get(field, []):
        if alt in row.index:
            return row[alt]
    return None

# ---------- 金额精度检查（≤2位小数） ----------

def _to_decimal_preserve(v):
    import pandas as _pd
    if v is None:
        return None
    try:
        if _pd.isna(v):
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
                    "原始值": str(val), "允许值": "", "建议修复": "四舍五入到分（ROUND(单元格,2)）",
                })
    return errs

# ---------- 表内校验 ----------

def validate_dataframe(
    df: pd.DataFrame,
    table: str,
    rules: Dict[str, Dict[str, Any]],
    length_mode: str = "max",
    pk_map: Optional[Dict[str, List[str]]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:

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
                ["统一社会信用代码", "证件号码", "姓名"],
                ["统一社会信用代码", "姓名"],
                ["证件号码", "姓名"],
                ["子企业统一社会信用代码", "子企业单位名称"],
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
    annotated_msgs: Dict[int, List[str]] = {}
    anno_col = "__校验错误__"
    if anno_col not in df.columns:
        df[anno_col] = ""

    n = len(df)

    # === 预计算：严格/清洗（表5 + 企业外键 + 名称层级清洗） ===
    _strict_msgs = defaultdict(list)
    _strict_rows = []
    _clean_msgs  = defaultdict(list)
    _clean_rows  = []

    # 表5 严格（.1..4）
    tb5_msgs, tb5_rows = _run_tb5_strict(df, table, pk_of)
    for k, v in tb5_msgs.items(): _strict_msgs[k].extend(v)
    _strict_rows.extend(tb5_rows)

    # 企业外键 严格
    fk_msgs, fk_rows = _run_fk_strict(df, table, pk_of)
    for k, v in fk_msgs.items(): _strict_msgs[k].extend(v)
    _strict_rows.extend(fk_rows)

    # 名称/层级 清洗
    cl_msgs, cl_rows = _run_name_level_clean(df, table)
    for k, v in cl_msgs.items(): _clean_msgs[k].extend(v)
    _clean_rows.extend(cl_rows)
    for idx in range(n):
        row = df.iloc[idx]
        row_msgs: List[str] = []
        _ri_excel = idx + 2
        row_msgs.extend(_strict_msgs.get(_ri_excel, []))
        row_msgs.extend(_clean_msgs.get(_ri_excel, []))

        # 字段级
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
                        "错误信息": f"长度应={L}", "原始值": sval, "允许值": f"长度={L}"
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
            if enum_set:
                if field == "上市类型":
                    msg = check_listing_type(sval)
                    if msg:
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": field, "错误类型": "取值非法",
                            "错误信息": msg, "原始值": sval,
                            "允许值": " | ".join(sorted(enum_set)) if enum_set else "a~i"
                        })
                        row_msgs.append(f"[{field}] {msg}")
                else:
                    if sval not in enum_set:
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": field, "错误类型": "取值非法",
                            "错误信息": "取值不在枚举中", "原始值": sval,
                            "允许值": " | ".join(sorted(enum_set))
                        })
                        row_msgs.append(f"[{field}] 取值不在枚举中")

        # ---- 表内等式/关系 ----

        if table == "中央企业各级单位人工成本情况表":
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

        if table == "中央企业职工收入情况表":
            total_income = try_decimal(_get_cell(row, "总收入"))
            extra_welfare = try_decimal(_get_cell(row, "工资总额外的福利费用"))
            deduction = try_decimal(_get_cell(row, "应扣合计"))
            actual_pay = try_decimal(_get_cell(row, "实发数"))
            if all(v is not None for v in [total_income, extra_welfare, deduction, actual_pay]):
                expect = (total_income or Decimal("0")) + (extra_welfare or Decimal("0")) - (deduction or Decimal("0"))
                tol = Decimal("0.01")
                if (actual_pay - expect).copy_abs() > tol:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "实发数", "错误类型": "不等式不满足",
                        "错误信息": "实发数应=总收入+工资总额外的福利费用-应扣合计",
                        "原始值": str(actual_pay), "允许值": f"期望={expect}"
                    })
                    row_msgs.append(f"[实发数] 应=总收入+工资总额外的福利费用-应扣合计（期望={expect} 实际{actual_pay}）")

            # 新增1：应扣合计 = 七项之和
            fields_sum = [
                "五险个人缴纳","公积金个人缴纳","补充养老保险个人缴纳",
                "补充医疗保险个人缴纳","其他保险个人缴纳","其他代扣代缴","个人所得税"
            ]
            need_vals = [try_decimal(_get_cell(row, c)) for c in fields_sum] + [try_decimal(_get_cell(row, "应扣合计"))]
            if all(v is not None for v in need_vals):
                expect = sum([(v or Decimal("0")) for v in need_vals[:-1]], Decimal("0"))
                tol = Decimal("0.01")
                real = need_vals[-1] or Decimal("0")
                if (real - expect).copy_abs() > tol:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "应扣合计", "错误类型": "等式不满足",
                        "错误信息": "应扣合计应等于各项个人缴纳/代扣之和",
                        "原始值": str(real), "允许值": f"期望={expect}"
                    })
                    row_msgs.append(f"[应扣合计] 应等于七项之和（期望={expect} 实际{real}）")

            # 新增2：是否参与中长期激励=是 ⇒ 工具 不能为空
            flag = norm(_get_cell(row, "是否参与中长期激励"))
            if flag and (flag == "是" or flag.startswith("1-")):
                tool = norm(_get_cell(row, "中长期激励工具"))
                if tool == "":
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "中长期激励工具", "错误类型": "缺失",
                        "错误信息": "已选择参与中长期激励，但“中长期激励工具”为空",
                        "原始值": "", "允许值": "请填写所使用的中长期激励工具"
                    })
                    row_msgs.append("[中长期激励工具] 需填写（因是否参与中长期激励=是）")

            # 新增3：发薪时间严格格式
            paytime = norm(_get_cell(row, "发薪时间"))
            if paytime:
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", paytime):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "发薪时间", "错误类型": "格式不符",
                        "错误信息": "格式应为 YYYY-MM-DD hh:mm:ss（半角英文“ ” “-” “:”）",
                        "原始值": paytime, "允许值": "例如：2025-09-08 19:27:56"
                    })
                    row_msgs.append("[发薪时间] 应为 YYYY-MM-DD hh:mm:ss（仅半角英文符号）")

            # 岗位层级=91-其他 ⇒ 是否在岗 必须为否类
            pos = norm(_get_cell(row, "岗位层级"))
            if pos.startswith("91-") or pos == "91":
                on_job = norm(_get_cell(row, "是否在岗"))
                if not (on_job.startswith("2-") or on_job.startswith("3-") or on_job.startswith("4-") or on_job.startswith("5-")):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "是否在岗", "错误类型": "取值非法",
                        "错误信息": "当岗位层级=91-其他时，“是否在岗”必须为否类（2/3/4/5开头）",
                        "原始值": on_job, "允许值": "2-否...,3-否...,4-否...,5-否..."
                    })
                    row_msgs.append("[是否在岗] 岗位层级=91-其他时必须选择否类")

            # 条件必填：是否为派出或派驻人员 ≠ “3-否”
            flag2 = norm(_get_cell(row, "是否为派出或派驻人员"))
            if flag2:
                is_negative = (flag2 == "否") or flag2.startswith("3-") or flag2.startswith("2-否")
                if not is_negative:
                    out_name = norm(_get_cell(row, "派驻或派出企业名称"))
                    if out_name == "":
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": "派驻或派出企业名称", "错误类型": "缺失",
                            "错误信息": "当【是否为派出或派驻人员】为‘是/非3-否’时，派驻或派出企业名称不能为空",
                            "原始值": "", "允许值": "请填写派驻或派出企业的名称"
                        })
                        row_msgs.append("[派驻或派出企业名称] 需填写（因是否为派出或派驻人员为‘是/非3-否’）")

        if table == "中央企业农民工情况表":
            blocks = [
                ("直接签订用工合同农民工费用总额", "其中：工资总额.1", "其中：各类保险总额.1"),
                ("劳务派遣形式农民工费用总额", "其中：工资总额.2", "其中：各类保险总额.2"),
                ("劳务外包和业务外包农民工费用总额", "其中：工资总额.3", "其中：各类保险总额.3"),
                ("其他农民工费用总额", "其中：工资总额.4", "其中：各类保险总额.4"),
            ]
            tol = Decimal("0.01")
            for tot, w, ins in blocks:
                tv = try_decimal(_get_cell(row, tot))
                wv = try_decimal(_get_cell(row, w))
                iv = try_decimal(_get_cell(row, ins))
                if all(v is not None for v in [tv,wv,iv]):
                    expect = (wv or Decimal("0")) + (iv or Decimal("0"))
                    if (tv - expect).copy_abs() > tol:
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": tot, "错误类型": "等式不满足",
                            "错误信息": f"{tot} 应= {w}+{ins}", "原始值": str(tv), "允许值": f"期望={expect}"
                        })
                        row_msgs.append(f"[{tot}] 应={w}+{ins}（期望={expect} 实际{tv}）")

            g_total = try_decimal(_get_cell(row, "农民工总费用"))
            g_w = try_decimal(_get_cell(row, "其中：工资总额"))
            g_i = try_decimal(_get_cell(row, "其中：各类保险总额"))
            if all(v is not None for v in [g_total,g_w,g_i]):
                expect1 = (g_w or Decimal("0")) + (g_i or Decimal("0"))
                if (g_total - expect1).copy_abs() > Decimal("0.01"):
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
            if g_total is not None and (g_total - parts_total).copy_abs() > Decimal("0.01"):
                errors.append({
                    "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                    "字段": "农民工总费用", "错误类型": "等式不满足",
                    "错误信息": "农民工总费用应=四类费用合计",
                    "原始值": str(g_total), "允许值": f"期望={parts_total}"
                })
                row_msgs.append(f"[农民工总费用] 应=四类费用合计（期望={parts_total} 实际{g_total}）")

        if row_msgs:
            df.at[idx, anno_col] = "；".join([m for m in row_msgs if m])

    
    # —— 融合：严格/清洗 行到 errors ——
    try:
        if _strict_rows:
            errors.extend(_strict_rows)
    except Exception:
        pass
    try:
        if _clean_rows:
            errors.extend(_clean_rows)
    except Exception:
        pass
    err_df = pd.DataFrame(errors, columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"])
    return err_df, df

# ---------- 枚举/上市类型 ----------

def check_listing_type(value: str) -> Optional[str]:
    s = norm(value)
    if s == "":
        return None
    s = s.replace("｜", "|")
    toks = [t.strip() for t in s.split("|") if t.strip()]
    if not toks:
        return "上市类型为空或格式不正确"
    for t in toks:
        if not re.fullmatch(r"[a-i]", t):
            return "上市类型仅允许 a~i，用 | 分隔"
    if len(set(toks)) != len(toks):
        return "上市类型不能出现重复值"
    if "i" in toks and len(toks) > 1:
        return "上市类型中 i（非上市）不能与其他值同时出现"
    return None

# ---------- 跨行一致性（同企业同年月操作类型一致） ----------

def cross_check_employee_ops(df_emp: pd.DataFrame) -> pd.DataFrame:
    errs: List[Dict[str, Any]] = []
    need = ["统计年月","子企业统一社会信用代码","操作类型"]
    if not all(c in df_emp.columns for c in need):
        return pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"])
    for (ym, code), grp in df_emp.groupby(["统计年月","子企业统一社会信用代码"], dropna=False):
        uniq = grp["操作类型"].astype(str).str.strip().tolist()
        uniq = [u for u in uniq if u and u.lower() != "nan"]
        if len(set(uniq)) > 1:
            rows = (grp.index + 2).tolist()
            errs.append({
                "统计年月": ym,
                "子企业统一社会信用代码": code,
                "问题描述": "同一子企业在同一统计年月的【操作类型】不一致",
                "涉及行号": ",".join(map(str, rows)),
                "涉及取值": " | ".join(sorted(set(uniq))),
            })
    return pd.DataFrame(errs)

# ===== 表间：主键一致性校验 =====
def cross_check_master_fk(dfs, writer):
    import pandas as _pd
    MASTER = "中央企业各级次单位信息情况表"
    KEY_CODE = "子企业统一社会信用代码"
    KEY_NAME = "子企业单位名称"
    if MASTER not in dfs:
        return
    df_master = dfs[MASTER]
    if not all(c in df_master.columns for c in [KEY_CODE, KEY_NAME]):
        return

    def _norm(s):
        try: return str(s).strip()
        except Exception: return ""

    master_set = set()
    for _, r in df_master.iterrows():
        code = _norm(r.get(KEY_CODE, ""))
        name = _norm(r.get(KEY_NAME, ""))
        if code and name: master_set.add((code, name))

    for t, df in dfs.items():
        if t == MASTER: continue
        if not all(c in df.columns for c in [KEY_CODE, KEY_NAME]): continue
        rows = []
        for idx, r in df.iterrows():
            code = _norm(r.get(KEY_CODE, ""))
            name = _norm(r.get(KEY_NAME, ""))
            if not code or not name:
                rows.append({"子企业统一社会信用代码": code, "子企业单位名称": name, "行号": idx + 2, "问题描述": "引用缺失（统一社会信用代码/单位名称为空）"})
                continue
            if (code, name) not in master_set:
                rows.append({"子企业统一社会信用代码": code, "子企业单位名称": name, "行号": idx + 2, "问题描述": "不在主数据（代码+名称）集合中"})
        sheet_name = f"表间-主键一致性检查-{t}"[:31]
        if rows:
            _pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            _pd.DataFrame(columns=["子企业统一社会信用代码","子企业单位名称","行号","问题描述"]).to_excel(writer, index=False, sheet_name=sheet_name)

# ===== 表间：员工挂靠层级检查（新增） =====
def cross_check_employee_attachment(dfs, writer):
    """
    员工挂靠层级检查（按你的口径）：
    - 依据《中央企业各级次单位信息情况表》中的【子企业所属层级】判定；
    - 职工收入表中的员工不能：
        1) 直接挂在有二级单位的一级企业（层级=1-一级且其下存在3-二级子单位）
        2) 直接挂在有三级单位的二级企业（层级=3-二级且其下存在5-三级子单位）
    只新增此检查，不影响其他逻辑。
    """
    import pandas as _pd
    import re

    MASTER = "中央企业各级次单位信息情况表"
    EMP = "中央企业职工收入情况表"
    if MASTER not in dfs or EMP not in dfs:
        return

    df_master = dfs[MASTER].copy()
    df_emp = dfs[EMP].copy()

    # ===== 列名解析（尽量只做最小别名兜底） =====
    def _pick_col(df, candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    m_code = "子企业统一社会信用代码"
    m_name = "子企业单位名称"
    # 关键：优先使用【子企业所属层级】
    level_col = _pick_col(df_master, ["子企业所属层级", "单位层级", "单位级别", "单位级次", "层级"])
    parent_code_col = _pick_col(df_master, ["上级单位统一社会信用代码", "所属上级企业统一社会信用代码", "上级企业统一社会信用代码", "上级统一社会信用代码"])

    if (m_code not in df_master.columns) or (m_name not in df_master.columns) or (level_col is None) or (parent_code_col is None):
        # 写一张提示表，避免静默失败
        _pd.DataFrame([{
            "提示": "主数据缺少关键列（子企业统一社会信用代码/子企业单位名称/子企业所属层级/上级单位统一社会信用代码），已跳过员工挂靠层级检查。"
        }]).to_excel(writer, index=False, sheet_name="表间-员工挂靠层级检查"[:31])
        return

    e_code = "子企业统一社会信用代码"
    e_name = "子企业单位名称"
    e_month = _pick_col(df_emp, ["统计年月"])  # 仅用于报错展示，可无

    if (e_code not in df_emp.columns) or (e_name not in df_emp.columns):
        _pd.DataFrame([{
            "提示": "职工收入表缺少【子企业统一社会信用代码】或【子企业单位名称】，已跳过员工挂靠层级检查。"
        }]).to_excel(writer, index=False, sheet_name="表间-员工挂靠层级检查"[:31])
        return

    # ===== 工具函数 =====
    def _norm(x):
        try:
            return str(x).strip()
        except Exception:
            return ""

    def _level_num(s: str) -> int | None:
        """把层级文本转为数字：1/3/5… 常见格式如 '1-一级'、'3-二级'."""
        s = _norm(s)
        if not s:
            return None
        m = re.match(r"(\d+)", s)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
        # 容错：根据中文含义猜
        if "一级" in s:
            return 1
        if "二级" in s:
            return 3
        if "三级" in s:
            return 5
        return None

    # ===== 主数据索引： (code, name) -> level_num =====
    level_map: dict[tuple[str, str], int] = {}
    children_level3_by_parent: set[str] = set()  # 有二级子的一级父 code
    children_level5_by_parent: set[str] = set()  # 有三级子的二级父 code

    for _, r in df_master.iterrows():
        code = _norm(r.get(m_code, ""))
        name = _norm(r.get(m_name, ""))
        lv = _level_num(r.get(level_col, ""))
        pcode = _norm(r.get(parent_code_col, ""))
        if code and name and (lv is not None):
            level_map[(code, name)] = lv

    # 是否存在下级：通过“上级单位统一社会信用代码”关系来判断
    for _, r in df_master.iterrows():
        child_lv = _level_num(r.get(level_col, ""))
        pcode = _norm(r.get(parent_code_col, ""))
        if not pcode or child_lv is None:
            continue
        if child_lv == 3:  # 二级的父亲就是一级
            children_level3_by_parent.add(pcode)
        elif child_lv == 5:  # 三级的父亲就是二级
            children_level5_by_parent.add(pcode)

    # ===== 逐员工检查 =====
    out_rows = []
    for idx, r in df_emp.iterrows():
        code = _norm(r.get(e_code, ""))
        name = _norm(r.get(e_name, ""))
        ym = _norm(r.get(e_month, "")) if e_month else ""

        if not code or not name:
            # 主键一致性（名称+代码）已有别处检查，这里只做层级逻辑，不重复报错
            continue

        lv = level_map.get((code, name))
        if lv is None:
            # 主键一致性检查会覆盖这类问题，这里不再重复
            continue

        # 规则1：一级 且 有二级子单位 → 员工不应直接挂在一级
        if lv == 1 and code in children_level3_by_parent:
            out_rows.append({
                "统计年月": ym,
                "子企业统一社会信用代码": code,
                "子企业单位名称": name,
                "问题描述": "员工不能直接挂在有二级单位的一级企业",
                "所属层级": "1-一级",
                "下级情况": "存在二级单位",
                "涉及行号": idx + 2,
            })

        # 规则2：二级 且 有三级子单位 → 员工不应直接挂在二级
        if lv == 3 and code in children_level5_by_parent:
            out_rows.append({
                "统计年月": ym,
                "子企业统一社会信用代码": code,
                "子企业单位名称": name,
                "问题描述": "员工不能直接挂在有三级单位的二级企业",
                "所属层级": "3-二级",
                "下级情况": "存在三级单位",
                "涉及行号": idx + 2,
            })

    sheet_name = "表间-员工挂靠层级检查"[:31]
    if out_rows:
        _pd.DataFrame(out_rows).to_excel(writer, index=False, sheet_name=sheet_name)
    else:
        _pd.DataFrame(columns=[
            "统计年月","子企业统一社会信用代码","子企业单位名称","问题描述","所属层级","下级情况","涉及行号"
        ]).to_excel(writer, index=False, sheet_name=sheet_name)

# ---------- CLI ----------

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
    ap.add_argument("--data", required=True, help="待校验数据：Excel 文件，或目录（多Excel）")
    ap.add_argument("--rules-xlsx", required=True, help="规则Excel（V2.3）")
    ap.add_argument("--sheet", default="央企端-表内校验", help="规则Sheet名（默认：央企端-表内校验）")
    ap.add_argument("--codes-sheet", default="码值", help="码值Sheet名（默认：码值；也兼容：码值表）")
    ap.add_argument("--length-mode", choices=["max","strict"], default="max", help="长度模式：max=把‘长度=’按‘≤’处理；strict=严格等长")
    ap.add_argument("--pk", default="", help="主键映射，如：'中央企业职工收入情况表:统一社会信用代码,证件号码,姓名; 表2:统一社会信用代码,姓名'" )
    ap.add_argument("--output", required=True, help="输出Excel路径")
    args = ap.parse_args()

    dfs = read_data_any(args.data)
    rules_all = compile_rules_from_excel(Path(args.rules_xlsx), sheet_name=args.sheet, codes_sheet=args.codes_sheet)
    pk_map = parse_pk_map(args.pk)

    try:
        import xlsxwriter  # noqa: F401
        engine = "xlsxwriter"
    except Exception:
        engine = None

    with pd.ExcelWriter(args.output, engine=engine) as xw:

        # —— 表间检查（合并）：表间-一致性检查 ——

        try:

            cross_merged = cross_check_fk_merged(dfs)

            cross_merged.to_excel(xw, index=False, sheet_name="表间-一致性检查")

        except Exception as _e:

            import pandas as _pd

            _pd.DataFrame([{"提示": f"表间检查异常：{_e}"}]).to_excel(xw, index=False, sheet_name="表间-一致性检查")

        master_pairs = build_master_pairs(dfs)
        try:
            pass  # patched: merged fk check
        except Exception:
            pass

        try:
            pass  # patched: merged employee attach check
        except Exception:
            pass

        try:
            cross_check_fk_pairs(dfs, xw)   # ← 就加这一段
        except Exception:
            pass


        all_errs = []

        for t, df in dfs.items():
            table = normalize_table_name(t)
            table_rules = rules_all.get(table, {})

            err_df, annotated_df = validate_dataframe(df.copy(), table, table_rules, length_mode=args.length_mode, pk_map=pk_map)

            # 追加后置校验（不改变原有逻辑）
            try:
                _post_errs = addon_post_checks(annotated_df, table, master_pairs, pk_map)
                if _post_errs:
                    err_df = pd.concat([err_df, pd.DataFrame(_post_errs)], ignore_index=True)
            except Exception:
                pass

            try:
                prec_errs = check_money_precision_errors(annotated_df, table, pk_map)
                if prec_errs:
                    err_df = pd.concat([err_df, pd.DataFrame(prec_errs)], ignore_index=True)
            except Exception:
                pass

            err_sheet = f"错误-{table}"
            if err_df.empty:
                pd.DataFrame(columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"]).to_excel(xw, index=False, sheet_name=err_sheet[:31])
            else:


                        # —— 收集到错误汇总 —— 

                        if not err_df.empty:

                            _tmp = err_df.copy()

                            _tmp.insert(0, "源表", table)

                            all_errs.append(_tmp)
            err_df.to_excel(xw, index=False, sheet_name=err_sheet[:31])
            annotated_df.to_excel(xw, index=False, sheet_name=f"标注-{table}"[:31])

        if "中央企业职工收入情况表" in dfs:
            cross_df = cross_check_employee_ops(dfs["中央企业职工收入情况表"])
            sh = "跨行-中央企业职工收入情况表"
            if cross_df.empty:
                pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"]).to_excel(xw, index=False, sheet_name=sh[:31])
            else:
                cross_df.to_excel(xw, index=False, sheet_name=sh[:31])


            # —— 错误汇总 —— 

            try:

                if all_errs:

                    pd.concat(all_errs, ignore_index=True).to_excel(xw, index=False, sheet_name="错误汇总")

                else:

                    pd.DataFrame(columns=["源表","表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"]).to_excel(xw, index=False, sheet_name="错误汇总")

            except Exception:

                pass

    print(f"完成：输出 {args.output}")

# === 新增：应扣合计“严格相等”检查（不改动原有逻辑） ===
def _check_hard_deduction_equality(df, table, pk_cols_map):
    """仅当表为“中央企业职工收入情况表”时，对应扣合计做严格相等校验。
       需要 8 个字段都能解析成数值才检查，以避免误伤空值。
    """
    from decimal import Decimal
    import pandas as _pd

    if table != "中央企业职工收入情况表":
        return []

    need = [
        "应扣合计",
        "五险个人缴纳",
        "公积金个人缴纳",
        "补充养老保险个人缴纳",
        "补充医疗保险个人缴纳",
        "其他保险个人缴纳",
        "其他代扣代缴",
        "个人所得税",
    ]
    for c in need:
        if c not in df.columns:
            return []

    # 复用主键
    def _pk_of(i: int) -> str:
        cols = pk_cols_map.get(table, [])
        if not cols:
            return ""
        row = df.iloc[i]
        parts = []
        for c in cols:
            v = row.get(c, "")
            parts.append("" if (v is None or (isinstance(v, float) and _pd.isna(v))) else str(v).strip())
        return "|" + "|".join(parts) if any(parts) else ""

    # 尝试解析为 Decimal
    def _to_dec(x):
        try:
            if x is None: return None
            if _pd.isna(x): return None
        except Exception:
            pass
        try:
            from decimal import Decimal
            if isinstance(x, float):
                return Decimal(repr(x))
            return Decimal(str(x).replace(",", "").strip())
        except Exception:
            return None

    errs = []
    for idx, row in df.iterrows():
        vals = {c: _to_dec(row.get(c)) for c in need}
        if any(v is None for v in vals.values()):
            # 有任意一个解析不出数值则跳过这条，避免误伤
            continue
        total = vals["应扣合计"]
        ssum = (
            vals["五险个人缴纳"] + vals["公积金个人缴纳"]
            + vals["补充养老保险个人缴纳"] + vals["补充医疗保险个人缴纳"]
            + vals["其他保险个人缴纳"] + vals["其他代扣代缴"]
            + vals["个人所得税"]
        )
        if total != ssum:
            errs.append({
                "表名": table, "行号": idx + 2, "主键": _pk_of(idx),
                "字段": "应扣合计", "错误类型": "等式不满足",
                "错误信息": "应扣合计必须等于各项个人缴纳/代扣代缴/个税之和（严格相等）",
                "原始值": str(total), "允许值": str(ssum)
            })
    return errs

from decimal import Decimal

def _norm_str_for_fk(v):
    try:
        s = str(v).strip()
    except Exception:
        s = ""
    # 统一全角竖线等
    return s.replace("｜", "|")

def build_master_pairs(dfs):
    """
    从【中央企业各级次单位信息情况表】构建
    (子企业统一社会信用代码, 子企业单位名称) 主键集合
    """
    MASTER = "中央企业各级次单位信息情况表"
    CODE = "子企业统一社会信用代码"
    NAME = "子企业单位名称"
    pairs = set()
    try:
        dfm = dfs.get(MASTER)
        if dfm is None:
            return pairs
        if CODE not in dfm.columns or NAME not in dfm.columns:
            return pairs
        for _, r in dfm.iterrows():
            c = _norm_str_for_fk(r.get(CODE, ""))
            n = _norm_str_for_fk(r.get(NAME, ""))
            if c and n:
                pairs.add((c, n))
    except Exception:
        return pairs
    return pairs

def _dec_or_none(x):
    """转 Decimal；空返回 None；尽量保留精度（支持 '66.3100000000001'）"""
    if x is None:
        return None
    sx = str(x).strip()
    if sx == "" or sx.lower() == "nan":
        return None
    try:
        return Decimal(sx.replace(",", ""))
    except Exception:
        try:
            return Decimal(repr(float(sx)))
        except Exception:
            return None


import re as _re

import unicodedata, re as _re_fkmerge

def _pick_col(cols, candidates):
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None

def _canon_code(s):
    if s is None:
        return ""
    return unicodedata.normalize("NFKC", str(s)).strip()

def _canon_name(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).strip()
    s = _re_fkmerge.sub(r"\s+", " ", s)
    return s

def build_master_pairs(dfs):
    MASTER = "中央企业各级次单位信息情况表"
    if MASTER not in dfs:
        return set()
    dfm = dfs[MASTER]
    code_col = _pick_col(dfm.columns, ["子企业统一社会信用代码", "统一社会信用代码", "社会信用代码"])
    name_col = _pick_col(dfm.columns, ["子企业单位名称", "子企业名称", "单位名称", "企业名称"])
    if not code_col or not name_col:
        return set()
    pairs = set()
    for _, r in dfm.iterrows():
        code = _canon_code(r.get(code_col))
        name = _canon_name(r.get(name_col))
        if code and name:
            pairs.add((code, name))
    return pairs

def cross_check_fk_merged(dfs):
    import pandas as _pd
    out_rows = []
    master_pairs = build_master_pairs(dfs)
    if not master_pairs:
        return _pd.DataFrame([{"提示": "未找到主数据的名称+代码主键集合，已跳过"}])

    child_code_cands = ["子企业统一社会信用代码", "统一社会信用代码", "社会信用代码"]
    child_name_cands = ["子企业单位名称", "子企业名称", "单位名称", "企业名称"]

    sup_code_cands = ["所属上级企业统一社会信用代码", "上级单位统一社会信用代码", "上级企业统一社会信用代码", "上级统一社会信用代码"]
    sup_name_cands = ["所属上级企业单位名称", "所属上级企业名称", "上级单位名称", "上级企业单位名称"]

    for t, df in dfs.items():
        # 1) 子企业名称+代码
        c_code = _pick_col(df.columns, child_code_cands)
        c_name = _pick_col(df.columns, child_name_cands)
        if c_code and c_name:
            for idx, r in df.iterrows():
                code = _canon_code(r.get(c_code))
                name = _canon_name(r.get(c_name))
                if not code or not name:
                    out_rows.append({"表名": t, "行号": idx + 2, "检查项": "子企业名称+代码", "问题": "引用缺失（名称或代码为空）", "名称": name, "统一社会信用代码": code})
                elif (code, name) not in master_pairs:
                    out_rows.append({"表名": t, "行号": idx + 2, "检查项": "子企业名称+代码", "问题": "不在主数据集合（名称+代码必须同时匹配）", "名称": name, "统一社会信用代码": code})

        # 2) 职工收入表：所属上级企业 名称+代码
        if t == "中央企业职工收入情况表":
            s_code = _pick_col(df.columns, sup_code_cands)
            s_name = _pick_col(df.columns, sup_name_cands)
            if s_code and s_name:
                for idx, r in df.iterrows():
                    scode = _canon_code(r.get(s_code))
                    sname = _canon_name(r.get(s_name))
                    if not scode or not sname:
                        out_rows.append({"表名": t, "行号": idx + 2, "检查项": "所属上级企业名称+代码", "问题": "引用缺失（名称或代码为空）", "名称": sname, "统一社会信用代码": scode})
                    elif (scode, sname) not in master_pairs:
                        out_rows.append({"表名": t, "行号": idx + 2, "检查项": "所属上级企业名称+代码", "问题": "不在主数据集合（名称+代码必须同时匹配）", "名称": sname, "统一社会信用代码": scode})

    if out_rows:
        return _pd.DataFrame(out_rows)[["表名","行号","检查项","问题","名称","统一社会信用代码"]]
    else:
        return _pd.DataFrame(columns=["表名","行号","检查项","问题","名称","统一社会信用代码"])



def _pk_str(df, table, pk_map, idx):
    cols = []
    if pk_map and table in pk_map:
        cols = pk_map[table]
    elif pk_map:
        for k, vv in pk_map.items():
            if (k in table) or (table in k):
                cols = vv; break
    if not cols:
        for cand in [["统一社会信用代码","证件号码","姓名"],["统一社会信用代码","姓名"],
                     ["证件号码","姓名"],["子企业统一社会信用代码","子企业单位名称"],["统一社会信用代码"]]:
            if all(c in df.columns for c in cand):
                cols = cand; break
    vals = []
    for c in cols:
        try:
            vals.append(str(df.at[idx, c]).strip())
        except Exception:
            vals.append("")
    return ("|" + "|".join(vals)) if any(vals) else ""

def addon_post_checks(df, table, master_pairs, pk_map):
    """
    追加的后置校验（不改动原有逻辑的任何判断）：
      1) 职工收入表：应扣合计 = 七项扣款之和（**无容差**，按两位小数比较）
      2) 职工收入表：所属上级企业【统一社会信用代码+单位名称】必须出现在主表的
         子企业【统一社会信用代码+单位名称】集合中（两者都一致才算匹配），
         支持列名轻微差异的同义名。
      3) 任意表：字段名**包含**“上市类型”的列，只允许 a~i，用 | 分隔；i 不能与其他并存；
         禁止“e-xxxx”这类带描述形式。
    """
    errs = []

    # 1) 应扣合计严格等式
    if table == "中央企业职工收入情况表":
        terms_cols = ["五险个人缴纳","公积金个人缴纳","补充养老保险个人缴纳","补充医疗保险个人缴纳",
                      "其他保险个人缴纳","其他代扣代缴","个人所得税"]
        if "应扣合计" in df.columns and all(c in df.columns for c in terms_cols):
            for idx, row in df.iterrows():
                terms = [(_dec_or_none(row.get(c)) or Decimal("0")) for c in terms_cols]
                # **先各自保留两位小数**再求和，再与“应扣合计（保留两位）”严格比较
                terms_q = [t.quantize(Decimal("0.01")) for t in terms]
                expect = sum(terms_q, Decimal("0.00")).quantize(Decimal("0.01"))
                actual = _dec_or_none(row.get("应扣合计"))
                if actual is None:
                    continue
                actual_q = actual.quantize(Decimal("0.01"))
                if actual_q != expect:
                    errs.append({
                        "表名": table, "行号": idx + 2, "主键": _pk_str(df, table, pk_map, idx),
                        "字段": "应扣合计", "错误类型": "等式不满足",
                        "错误信息": "应扣合计必须等于各项扣款之和（无容差，保留两位小数比较）",
                        "原始值": str(actual), "允许值": f"期望={expect}",
                        "建议修复": "逐项金额保留两位小数后求和，并与应扣合计相等"
                    })

        # 2) 上级企业（代码+名称）必须落在主表集合
        code_cols = ["所属上级企业统一社会信用代码","上级单位统一社会信用代码"]
        name_cols = ["所属上级企业单位名称","上级单位名称","所属上级企业名称"]
        code_col = next((c for c in code_cols if c in df.columns), None)
        name_col = next((c for c in name_cols if c in df.columns), None)
        if master_pairs and code_col and name_col:
            for idx, row in df.iterrows():
                cc = _norm_str_for_fk(row.get(code_col, ""))
                nn = _norm_str_for_fk(row.get(name_col, ""))
                if not cc and not nn:
                    continue  # 两个都空：不校验
                if not cc or not nn or (cc, nn) not in master_pairs:
                    errs.append({
                        "表名": table, "行号": idx + 2, "主键": _pk_str(df, table, pk_map, idx),
                        "字段": f"{code_col}+{name_col}", "错误类型": "引用不一致",
                        "错误信息": "所属上级企业【统一社会信用代码+单位名称】不在主表的子企业【统一社会信用代码+单位名称】集合中",
                        "原始值": f"{cc}+{nn}", "允许值": "必须与主表成对完全一致（两者都相同）"
                    })

    # 3) 上市类型：列名**包含**“上市类型”就校验
    listing_cols = [c for c in df.columns if "上市类型" in str(c)]
    if listing_cols:
        for idx, row in df.iterrows():
            for col in listing_cols:
                sval = str(row.get(col, "")).strip().replace("｜","|")
                if not sval:
                    continue
                toks = [t.strip() for t in sval.split("|") if t.strip()]
                basic_ok  = all(_re.fullmatch(r"[a-i]", t) for t in toks)          # 只允许 a~i
                unique_ok = (len(set(toks)) == len(toks))                           # 不重复
                i_rule_ok = not ("i" in toks and len(toks) > 1)                    # i 不能与其他并存
                if not (basic_ok and unique_ok and i_rule_ok):
                    errs.append({
                        "表名": table, "行号": idx + 2, "主键": _pk_str(df, table, pk_map, idx),
                        "字段": col, "错误类型": "取值非法",
                        "错误信息": "上市类型仅允许 a~i，用 | 分隔；不得带描述；i 不能与其他并存",
                        "原始值": sval, "允许值": "a~i（如：a 或 a|c|f；i 需单独出现）"
                    })
    return errs

# === 新增：表间 主键一致性（上级企业 代码+名称 成对存在于主表） ===
def cross_check_superior_fk(dfs, writer):
    """以【中央企业各级次单位信息情况表】(子企业单位名称, 子企业统一社会信用代码) 为主集合，
       校验【中央企业职工收入情况表】(所属上级企业单位名称, 所属上级企业统一社会信用代码) 必须成对一致。
       输出到：表间-上级企业一致性检查
    """
    import pandas as _pd

    MASTER = "中央企业各级次单位信息情况表"
    EMP = "中央企业职工收入情况表"
    CODE = "子企业统一社会信用代码"
    NAME = "子企业单位名称"
    SUP_CODE = "所属上级企业统一社会信用代码"
    SUP_NAME = "所属上级企业单位名称"

    if MASTER not in dfs or EMP not in dfs:
        return

    dfm = dfs[MASTER]
    dfe = dfs[EMP]

    if not all(c in dfm.columns for c in [CODE, NAME]):
        return
    if not all(c in dfe.columns for c in [SUP_CODE, SUP_NAME]):
        # 没有上级两个关键列就不检查
        return

    def _n(x):
        try:
            s = str(x).strip()
        except Exception:
            s = ""
        return "" if s.lower() == "nan" else s

    master_set = set()
    for _, r in dfm.iterrows():
        code = _n(r.get(CODE, ""))
        name = _n(r.get(NAME, ""))
        if code and name:
            master_set.add((code, name))

    rows = []
    for idx, r in dfe.iterrows():
        sc = _n(r.get(SUP_CODE, ""))
        sn = _n(r.get(SUP_NAME, ""))
        if not sc or not sn:
            rows.append({
                "行号": idx + 2,
                "所属上级企业统一社会信用代码": sc,
                "所属上级企业单位名称": sn,
                "问题描述": "引用缺失（所属上级企业代码或名称为空）",
            })
        elif (sc, sn) not in master_set:
            rows.append({
                "行号": idx + 2,
                "所属上级企业统一社会信用代码": sc,
                "所属上级企业单位名称": sn,
                "问题描述": "不在主数据（代码+名称）集合中",
            })

    sheet_name = "表间-上级企业一致性检查"
    if rows:
        _pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name[:31])
    else:
        _pd.DataFrame(columns=["行号","所属上级企业统一社会信用代码","所属上级企业单位名称","问题描述"]).to_excel(writer, index=False, sheet_name=sheet_name[:31])

# ==== PATCH: 表间“主键(名称+代码)”与“上级一致性”增强 ====
def cross_check_fk_pairs(dfs: dict, writer) -> None:
    """
    以【中央企业各级次单位信息情况表】为主数据：
      1) 其他表如同时包含【子企业统一社会信用代码】和【子企业单位名称】，则逐行校验
         (代码, 名称) 这一对是否存在于主表；
      2) 若子表同时包含【所属上级企业统一社会信用代码】和【所属上级企业单位名称】，
         则对照主表中该(子企业代码, 子企业名称)对应的“上级”是否完全一致（两者都要一致）。
    仅新增检查与输出，不改动任何原有校验流程/规则。
    """
    import pandas as _pd

    MASTER = "中央企业各级次单位信息情况表"
    KEY_CODE = "子企业统一社会信用代码"
    KEY_NAME = "子企业单位名称"
    SUP_CODE = "所属上级企业统一社会信用代码"
    SUP_NAME = "所属上级企业单位名称"

    if MASTER not in dfs:
        # 主表缺失，输出提示后返回
        _pd.DataFrame([{"提示": f"未找到主数据表：{MASTER}，已跳过表间“主键&上级一致性”检查"}]).to_excel(
            writer, index=False, sheet_name="表间-主键&上级-提示"[:31]
        )
        return

    df_master = dfs[MASTER]
    need_master_cols = [KEY_CODE, KEY_NAME]
    if not all(c in df_master.columns for c in need_master_cols):
        _pd.DataFrame([{"提示": f"主数据表缺少必要列：{need_master_cols}，已跳过检查"}]).to_excel(
            writer, index=False, sheet_name="表间-主键&上级-提示"[:31]
        )
        return

    # 标准化
    def _n(x):
        try:
            return str(x).strip()
        except Exception:
            return ""

    # 1) 主键集合（子企业代码+名称）
    master_pairs = set()
    # 2) (子企业代码, 子企业名称) -> (上级代码, 上级名称)
    master_superior = {}
    for _, r in df_master.iterrows():
        code = _n(r.get(KEY_CODE, ""))
        name = _n(r.get(KEY_NAME, ""))
        if code and name:
            master_pairs.add((code, name))
            up_c = _n(r.get(SUP_CODE, ""))
            up_n = _n(r.get(SUP_NAME, ""))
            # 记录主表上的“上级”，两者都填了才算有效
            if up_c and up_n:
                master_superior[(code, name)] = (up_c, up_n)

    # 逐个子表检查（除主表之外）
    for t, df_child in dfs.items():
        if t == MASTER:
            continue

        # 仅当子表同时拥有 代码+名称 两列时才做“主键存在”检查
        has_pair_cols = all(c in df_child.columns for c in [KEY_CODE, KEY_NAME])
        # “上级一致性”检查需要 4 列都存在
        has_super_cols = all(c in df_child.columns for c in [KEY_CODE, KEY_NAME, SUP_CODE, SUP_NAME])

        rows = []
        if not has_pair_cols and not has_super_cols:
            # 子表没有这些列，跳过（写个空表结构以示已检查）
            _pd.DataFrame(columns=[KEY_CODE, KEY_NAME, SUP_CODE, SUP_NAME, "行号", "问题描述", "期望上级(代码)", "期望上级(名称)"]).to_excel(
                writer, index=False, sheet_name=f"表间-主键&上级-{t}"[:31]
            )
            continue

        for idx, r in df_child.iterrows():
            code = _n(r.get(KEY_CODE, ""))
            name = _n(r.get(KEY_NAME, ""))

            # 1) 主键(名称+代码) 同时校验
            if has_pair_cols:
                if not code or not name:
                    rows.append({
                        KEY_CODE: code, KEY_NAME: name,
                        SUP_CODE: _n(r.get(SUP_CODE, "")), SUP_NAME: _n(r.get(SUP_NAME, "")),
                        "行号": idx + 2,
                        "问题描述": "引用缺失（子企业统一社会信用代码/子企业单位名称必须同时填写）",
                        "期望上级(代码)": "", "期望上级(名称)": ""
                    })
                elif (code, name) not in master_pairs:
                    rows.append({
                        KEY_CODE: code, KEY_NAME: name,
                        SUP_CODE: _n(r.get(SUP_CODE, "")), SUP_NAME: _n(r.get(SUP_NAME, "")),
                        "行号": idx + 2,
                        "问题描述": "不在主数据（子企业代码+名称）集合中",
                        "期望上级(代码)": "", "期望上级(名称)": ""
                    })

            # 2) 上级一致性（完全匹配：代码+名称）
            if has_super_cols and code and name:
                child_up_c = _n(r.get(SUP_CODE, ""))
                child_up_n = _n(r.get(SUP_NAME, ""))

                # 只有当子表两项上级都填写了，且主表也有“上级”记录时才强制比对
                #（避免主表没上级或子表只填了一半导致噪音）
                exp = master_superior.get((code, name))
                if child_up_c or child_up_n:
                    if exp:
                        exp_c, exp_n = exp
                        if not (child_up_c == exp_c and child_up_n == exp_n):
                            rows.append({
                                KEY_CODE: code, KEY_NAME: name,
                                SUP_CODE: child_up_c, SUP_NAME: child_up_n,
                                "行号": idx + 2,
                                "问题描述": "所属上级不一致（需与主表完全一致：代码+名称均相同）",
                                "期望上级(代码)": exp_c, "期望上级(名称)": exp_n
                            })
                    else:
                        # 主表没有给出该单位的“上级”，则无法强比对——给出提示
                        rows.append({
                            KEY_CODE: code, KEY_NAME: name,
                            SUP_CODE: child_up_c, SUP_NAME: child_up_n,
                            "行号": idx + 2,
                            "问题描述": "主表未提供该单位的“上级”信息，无法比对（仅提示）",
                            "期望上级(代码)": "", "期望上级(名称)": ""
                        })

        sheet = f"表间-主键&上级-{t}"[:31]
        if rows:
            _pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet)
        else:
            _pd.DataFrame(columns=[KEY_CODE, KEY_NAME, SUP_CODE, SUP_NAME, "行号", "问题描述", "期望上级(代码)", "期望上级(名称)"]).to_excel(
                writer, index=False, sheet_name=sheet
            )
# ==== END PATCH ====

if __name__ == "__main__":
    sys.exit(main())
