# -*- coding: utf-8 -*-
"""
SOE Validator (V2.3) - 表内 + 基础跨行校验
脚本名：soe_validator_v22_full_v2_fixed.py

仅做最小变更，保留你已确认可用的所有逻辑；本版只补三点：
1) “最高学历”强制白名单（31/41 等均会报错）；
2) “岗位层级=91-其他 ⇒ 是否在岗 必须为否类(2/3/4/5)”；
3) 读取值时的极小范围“同义名”兜底（不改列名、不做全局别名），
   仅针对：
     - “是否为专职外部董事” ←→ 读取“是否专职外部董事”的值做兜底
     - “派驻或派出企业名称” ←  读取“派驻或派驻出企业名称”的值做兜底
   目的：避免已填写却被误判“缺失”。
4) “是否为派出或派驻人员”≠“3-否” 且已选择时，“派驻或派出企业名称”必须填写（条件必填）。
5) 金额精度（≤2位小数）保留，使用精度检查追加错误，而不改动既有判断。
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
    return norm(name).replace("\n","").replace("\r","").strip()

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

        # 长度（尽量捕捉“长度=”“长度≤”等）
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

    # 读“码值”/“码值表”（优先覆盖/补齐枚举）
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
    "发薪时间": 19,  # 例如 2025-09-08 19:27:56
}

HR_COST_SUM_FIELDS = [
    "职工工资总额","社会保险费用","住房公积金","住房补贴","企业年金和职业年金",
    "补充医疗保险","福利费用","劳动保护费","工会经费","教育培训经费",
    "技术奖酬金及业务设计奖","辞退福利","股份支付","其他人工成本","劳务派遣费"
]

# 最高学历：严格白名单
EDU_STRICT_SET = {
    "10-博士研究生","20-硕士研究生","30-大学本科","40-大学专科",
    "50-中专/职高/技校","60-普通高中","70-初中","80-小学及以下"
}

# 极小范围“同义名”兜底（仅用于取值，不改列名、不过度匹配）
FIELD_SYNONYMS: Dict[str, List[str]] = {
    "是否为专职外部董事": ["是否专职外部董事"],
    "派驻或派出企业名称": ["派驻或派驻出企业名称"],
    "是否为派出或派驻人员": ["是否为派驻或派出人员","是否派出或派驻人员"],
}

def _get_cell(row: pd.Series, field: str):
    # 优先用标准名；若没有，再用少量同义名兜底
    if field in row.index:
        return row[field]
    for alt in FIELD_SYNONYMS.get(field, []):
        if alt in row.index:
            return row[alt]
    return None

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
    for idx in range(n):
        row = df.iloc[idx]
        row_msgs: List[str] = []

        # 字段级
        for field, fr in rules.items():
            raw_val = _get_cell(row, field)  # 使用兜底取值
            sval = norm(raw_val)

            # “派驻或派出企业名称”在本版本只走“条件必填”，跳过通用的 required 判断
            if fr.get("required") and sval == "":
                if not (table == "中央企业职工收入情况表" and field == "派驻或派出企业名称"):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "缺失", "错误信息": "不能为空",
                        "原始值": sval, "允许值": ""
                    })
                    row_msgs.append(f"[{field}] 不能为空")
                    continue  # 缺失则跳过后续长度/枚举
                # 否则让条件必填去判断

            if sval == "":
                continue

            # exact length（强制）
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

            # 最高学历：强制白名单（覆盖码值表可能的扩展）
            if field == "最高学历":
                if sval not in EDU_STRICT_SET:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "取值非法",
                        "错误信息": "取值不在枚举中", "原始值": sval,
                        "允许值": " | ".join(sorted(EDU_STRICT_SET))
                    })
                    row_msgs.append(f"[{field}] 取值不在枚举中")
                # 即便不在，仍继续后面的常规枚举以保持行为一致（不 return）

            # 常规枚举
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

            # 岗位层级=91-其他 ⇒ 是否在岗 必须为否类（2/3/4/5）
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

            # 条件必填：是否为派出或派驻人员 ≠ “3-否” 时，派驻或派出企业名称必须填写
            flag = norm(_get_cell(row, "是否为派出或派驻人员"))
            if flag:  # 仅当用户有选择时才判断
                is_negative = (flag == "否") or flag.startswith("3-") or flag.startswith("2-否")
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

    err_df = pd.DataFrame(errors, columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"])
    return err_df, df

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
    ap.add_argument("--pk", default="", help="主键映射，如：'中央企业职工收入情况表:统一社会信用代码,证件号码,姓名; 表2:统一社会信用代码,姓名'")
    ap.add_argument("--output", required=True, help="输出Excel路径")
    args = ap.parse_args()

    dfs = read_data_any(args.data)
    rules_all = compile_rules_from_excel(Path(args.rules_xlsx), sheet_name=args.sheet, codes_sheet=args.codes_sheet)
    pk_map = parse_pk_map(args.pk)

    # 写出
    try:
        import xlsxwriter  # noqa: F401
        engine = "xlsxwriter"
    except Exception:
        engine = None

    with pd.ExcelWriter(args.output, engine=engine) as xw:
        # 主键一致性（新增调用，不影响其它逻辑）
        try:
            cross_check_master_fk(dfs, xw)
        except Exception:
            pass
        for t, df in dfs.items():
            table = normalize_table_name(t)
            table_rules = rules_all.get(table, {})

            err_df, annotated_df = validate_dataframe(df.copy(), table, table_rules, length_mode=args.length_mode, pk_map=pk_map)

            # 金额精度错误追加（不改变其它逻辑产出的错误）
            try:
                prec_errs = check_money_precision_errors(annotated_df, table, pk_map)
                if prec_errs:
                    err_df = pd.concat([err_df, pd.DataFrame(prec_errs)], ignore_index=True)
            except Exception:
                pass

            # 输出错误与标注
            err_sheet = f"错误-{table}"
            if err_df.empty:
                pd.DataFrame(columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值","建议修复"]).to_excel(xw, index=False, sheet_name=err_sheet[:31])
            else:
                err_df.to_excel(xw, index=False, sheet_name=err_sheet[:31])
            annotated_df.to_excel(xw, index=False, sheet_name=f"标注-{table}"[:31])

        # 跨行一致性（职工收入）
        if "中央企业职工收入情况表" in dfs:
            cross_df = cross_check_employee_ops(dfs["中央企业职工收入情况表"])
            sh = "跨行-中央企业职工收入情况表"
            if cross_df.empty:
                pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"]).to_excel(xw, index=False, sheet_name=sh[:31])
            else:
                cross_df.to_excel(xw, index=False, sheet_name=sh[:31])

    print(f"完成：输出 {args.output}")

# ===== 表间：主键一致性校验（新增，其他逻辑不改） =====
def cross_check_master_fk(dfs, writer):
    """
    以【中央企业各级次单位信息情况表】为主数据表，使用
    （子企业统一社会信用代码, 子企业单位名称）作为主键集合，
    校验其他表中出现的同名两列是否都在主集合中。
    仅新增检查与输出，不改动任何原有校验流程。
    """
    import pandas as _pd

    MASTER = "中央企业各级次单位信息情况表"
    KEY_CODE = "子企业统一社会信用代码"
    KEY_NAME = "子企业单位名称"

    if MASTER not in dfs:
        # 没有主数据表就直接跳过
        return

    df_master = dfs[MASTER]
    if not all(c in df_master.columns for c in [KEY_CODE, KEY_NAME]):
        return

    # 构建主键集合（去除空值）
    def _norm(s):
        try:
            return str(s).strip()
        except Exception:
            return ""

    master_set = set()
    for _, r in df_master.iterrows():
        code = _norm(r.get(KEY_CODE, ""))
        name = _norm(r.get(KEY_NAME, ""))
        if code and name:
            master_set.add((code, name))

    # 逐子表检查（除主表之外）
    for t, df in dfs.items():
        if t == MASTER:
            continue
        # 只有当两个关键列都存在时才检查
        if not all(c in df.columns for c in [KEY_CODE, KEY_NAME]):
            continue

        rows = []
        for idx, r in df.iterrows():
            code = _norm(r.get(KEY_CODE, ""))
            name = _norm(r.get(KEY_NAME, ""))
            if not code or not name:
                rows.append({
                    "子企业统一社会信用代码": code,
                    "子企业单位名称": name,
                    "行号": idx + 2,
                    "问题描述": "引用缺失（统一社会信用代码/单位名称为空）",
                })
                continue
            if (code, name) not in master_set:
                rows.append({
                    "子企业统一社会信用代码": code,
                    "子企业单位名称": name,
                    "行号": idx + 2,
                    "问题描述": "不在主数据（代码+名称）集合中",
                })

        sheet_name = f"表间-主键一致性检查-{t}"[:31]
        if rows:
            _pd.DataFrame(rows).to_excel(writer, index=False, sheet_name=sheet_name)
        else:
            _pd.DataFrame(columns=["子企业统一社会信用代码","子企业单位名称","行号","问题描述"]).to_excel(writer, index=False, sheet_name=sheet_name)

if __name__ == "__main__":
    sys.exit(main())
