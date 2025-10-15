# -*- coding: utf-8 -*-
"""
SOE Validator (V2.3) - 表内 + 基础跨行校验
脚本名：soe_validator_v22_full_v2_fixed.py

主要能力
- 从“央企端-表内校验”读取字段规则（必填、类型/长度、等式/不等式提示文本）
- 从“码值/码值表”优先读取各字段枚举；如果同字段在“表内校验”里也给了枚举，则以码值为主、表内补充
- 支持 exact / max 两种长度模式（--length-mode：strict 或 max；默认 max）
- 重点内置：
  * 人工成本总额 ≥ 指定 15 个科目之和（去掉“非货币性福利”，避免重复计算）
  * 实发数 = 总收入 + 工资总额外的福利费用 - 应扣合计
  * 农民工表：A = A1 + A2（分四块：直接、派遣、外包、其他）
  * 上市类型：仅允许 a~i 字符，'i' 不能与其他并存，值用 | 连接且不可重复
  * 岗位层级=91-其他 ⇒ 是否在岗 ∈ {2-否...,3-否...,4-否...,5-否...}
  * 同一统计年月、同一子企业（统一社会信用代码）下，操作类型必须一致

输出
- 一个Excel：每张数据表两张sheet：
  错误-表名：错误明细
  标注-表名：原数据 + __校验错误__ 列
- 跨行/一致性检查：单独一个 sheet：跨行-中央企业职工收入情况表

用法示例
python soe_validator_v22_full_v2_fixed.py \
  --data "待校验数据.xlsx" \
  --rules-xlsx "数据标准2.0数据采集校验规则_V2.3.xlsx" \
  --sheet "央企端-表内校验" \
  --codes-sheet "码值" \
  --length-mode max \
  --pk "中央企业职工收入情况表:统一社会信用代码,证件号码,姓名" \
  --output "validation_errors_v23.xlsx"
"""
import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set

import pandas as pd
import numpy as np
from decimal import Decimal, InvalidOperation
import re
import sys
import warnings

# === 金额精度检查（≤2位小数） BEGIN ===
from decimal import Decimal

def _to_decimal_preserve(v):
    """尽量保留Excel浮点的真实精度以捕捉 66.3100000000001 这类情况。"""
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

def _money_columns_for_table(table_name: str, df_columns: list[str]) -> set:
    """已知的金额列全集 + 名称启发式（不改变原逻辑，仅做额外精度校验）。"""
    # --- 明确列清单（根据规则2.x与双方沟通补齐） ---
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
        # 如后续还有：中长期激励、专项奖励等，可在此追加，不影响其它逻辑
    }
    cols = set()
    cols |= known.get(table_name, set())
    # --- 名称启发式（只挑“很像金额”的列，尽量避免误伤计数列）---
    tokens = ("金额","收入","工资","薪酬","费用","补贴","缴纳","合计","实发","应扣","成本","经费","支付","税")
    for c in df_columns:
        if any(tok in str(c) for tok in tokens):
            cols.add(c)
    # 只保留数据中实际存在的列
    cols = {c for c in cols if c in df_columns}
    return cols

def _check_money_precision(df, table: str, pk_cols_map: dict):
    """对金额列做“小数位≤2”的精度校验。仅在非空且可解析为数值时检查。"""
    import pandas as _pd
    anno_col = "__校验错误__"
    if anno_col not in df.columns:
        df[anno_col] = ""

    money_cols = _money_columns_for_table(table, list(df.columns))
    if not money_cols:
        return [], df

    def _pk_of(i: int) -> str:
        cols = pk_cols_map.get(table, [])
        if not cols:
            return ""
        row = df.iloc[i]
        vals = [str(row.get(c, "")).strip() for c in cols]
        return ("|" + "|".join(vals)) if any(vals) else ""

    errors = []
    for idx, row in df.iterrows():
        for col in money_cols:
            val = row[col]
            # 空值不判
            try:
                if val is None or _pd.isna(val):
                    continue
            except Exception:
                if val is None:
                    continue
            d = _to_decimal_preserve(val)
            if d is None:
                continue
            q = d.quantize(Decimal("0.01"))
            if (d - q).copy_abs() > Decimal("0"):
                msg = "小数位应≤2位（金额保留到分）"
                errors.append({
                    "表名": table,
                    "行号": idx + 2,
                    "主键": _pk_of(idx),
                    "字段": col,
                    "错误类型": "精度超限",
                    "错误信息": msg,
                    "建议修复": "四舍五入到分，例如=ROUND(单元格,2)",
                    "原始值": str(val),
                })
                prev = str(df.at[idx, anno_col]).strip()
                df.at[idx, anno_col] = (prev + ("；" if prev else "") + f"{col}:{msg}") if prev else f"{col}:{msg}"
    return errors, df
# === 金额精度检查（≤2位小数） END ===

# === alias + optional field patch (minimal intrusive) ===
# 将“是否为专职外部董事”与“是否专职外部董事”视为同一字段；
# 将“派驻或派驻出企业名称”统一成“派驻或派出企业名称”，并将该字段视作可留空（不判定缺失）。

ALIAS_PAIRS = [
    ("是否为专职外部董事", "是否专职外部董事"),
    ("派驻或派驻出企业名称", "派驻或派出企业名称"),
]

OPTIONAL_FIELDS = {
    # 表名 -> 可留空的字段集合
    "中央企业职工收入情况表": {"派驻或派出企业名称"},
}

def _apply_column_aliases(df: pd.DataFrame, table_rules: Dict[str, Any]) -> pd.DataFrame:
    """根据规则中出现的字段名，按最合适的别名把列统一重命名。"""
    cols = list(df.columns)
    # 选择每对别名的“目标名”——优先选在规则里出现的名字
    for a, b in ALIAS_PAIRS:
        if a in table_rules:
            target = a
        elif b in table_rules:
            target = b
        else:
            # 两个名字都不在规则里，则不处理
            continue
        # 如果另一名称在数据列中，重命名到 target
        if b in cols and target == a:
            df = df.rename(columns={b: a})
            cols = list(df.columns)
        elif a in cols and target == b:
            df = df.rename(columns={a: b})
            cols = list(df.columns)
    return df

def _filter_optional_errors(table: str, err_df: pd.DataFrame, annotated_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """对声明为可留空的字段，删除“缺失/不能为空”类错误，并在标注列中同步去除。"""
    opt_set = OPTIONAL_FIELDS.get(table, set())
    if not opt_set or err_df is None or err_df.empty:
        return err_df, annotated_df

    mask_drop = False
    for col in opt_set:
        cond = (err_df['字段'] == col) & (err_df['错误类型'].isin(['缺失', '为空', '不能为空']))
        if isinstance(mask_drop, bool):
            mask_drop = cond
        else:
            mask_drop = mask_drop | cond
    if isinstance(mask_drop, bool):
        # 没匹配上
        return err_df, annotated_df

    err_df = err_df.loc[~mask_drop].copy()

    # 清理标注列（如果存在）
    anno_cols = [c for c in annotated_df.columns if str(c).startswith("__校验错误")]
    if anno_cols:
        ac = anno_cols[0]
        def _strip_opt_msgs(s):
            if not isinstance(s, str) or not s:
                return s
            parts = [p for p in s.split("；") if p]  # 原脚本使用中文分号拼接
            kept = []
            for p in parts:
                drop = False
                for col in opt_set:
                    # 既考虑“缺失/不能为空”也考虑英文提示
                    if (("缺失" in p or "不能为空" in p or "为空" in p) and (col in p)):
                        drop = True
                        break
                if not drop:
                    kept.append(p)
            return "；".join(kept)
        annotated_df[ac] = annotated_df[ac].apply(_strip_opt_msgs)

    return err_df, annotated_df
# === end of patch ===


warnings.simplefilter("ignore", FutureWarning)

# -------------- 工具函数 --------------

def read_data_any(path: str) -> Dict[str, pd.DataFrame]:
    """
    读取数据源：
    - 如果 path 是 Excel：读取所有 sheet，返回 {sheet名: df}
    - 如果 path 是目录：读取目录内所有 xls/xlsx，每个文件一个sheet名=文件名（不含扩展名）
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"未找到数据路径：{path}")
    if p.is_dir():
        dfs = {}
        for fp in sorted(p.glob("*.xls*")):
            try:
                df = pd.read_excel(fp)
                dfs[fp.stem] = df
            except Exception as e:
                print(f"[WARN] 读取失败：{fp.name} - {e}")
        return dfs
    else:
        try:
            xls = pd.ExcelFile(p)
            out = {}
            for name in xls.sheet_names:
                out[name] = pd.read_excel(xls, sheet_name=name)
            return out
        except Exception as e:
            raise RuntimeError(f"读取Excel失败：{path} - {e}")

def norm(s: Any) -> str:
    if s is None:
        return ""
    if isinstance(s, float) and np.isnan(s):
        return ""
    return str(s).strip()

def try_decimal(x: Any) -> Optional[Decimal]:
    s = norm(x)
    if s == "":
        return None
    try:
        return Decimal(s.replace(",", ""))
    except Exception:
        return None

def is_number_like(x: Any) -> bool:
    return try_decimal(x) is not None

def chinese_or_pipe_strip(s: str) -> str:
    # 统一 '|' 与 '｜'，去掉多余空格
    s = s.replace("｜", "|")
    parts = [p.strip() for p in s.split("|") if p.strip() != ""]
    return "|".join(parts)

def split_enum_string(s: str) -> List[str]:
    s = chinese_or_pipe_strip(s)
    if s == "":
        return []
    return [p.strip() for p in s.split("|")]

def normalize_table_name(name: str) -> str:
    return norm(name).replace("\n", "").replace("\r", "").strip()

# -------------- 读取规则 --------------

def compile_rules_from_excel(xlsx_path: Path, sheet_name: str = "央企端-表内校验", codes_sheet: str = "码值") -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    返回结构：{ 表名: { 字段名: {required:bool, type:str, len_eq:int|None, len_max:int|None, enum:Set[str], tips:str } } }
    优先从“码值”sheet 读取枚举；若同字段在 表内校验 也给了枚举，则合并（码值优先）
    """
    xls = pd.ExcelFile(xlsx_path)

    # 读取表内校验
    try:
        df_rules = pd.read_excel(xls, sheet_name=sheet_name)
    except Exception:
        raise RuntimeError(f"读取规则Sheet失败：{sheet_name}")

    # 尝试找常见列名
    # 表名/对象、字段/列名、规则/说明、允许值/枚举
    def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = {c.strip(): c for c in df.columns}
        for cand in candidates:
            if cand in cols:
                return cols[cand]
        # 模糊匹配
        for c in df.columns:
            s = str(c).strip()
            if any(k in s for k in candidates):
                return c
        return None

    col_table = pick_col(df_rules, ["表名", "表", "对象", "表英文名", "表中文名", "对象名称"])
    col_field = pick_col(df_rules, ["字段", "字段名", "列名", "字段名称"])
    col_rule  = pick_col(df_rules, ["规则", "校验规则", "校验规则说明", "规则说明", "逻辑", "说明"])
    col_enum  = pick_col(df_rules, ["允许值", "枚举", "取值", "枚举值"])

    if not col_table or not col_field:
        raise RuntimeError("在规则表中未找到 表名/字段 列")

    # 汇总规则容器
    rules: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # 先扫一遍表内校验，收集长度/必填/特殊提示、以及可能的内嵌枚举
    for _, row in df_rules.iterrows():
        table = normalize_table_name(row[col_table])
        field = norm(row[col_field])
        if not table or not field:
            continue
        r = norm(row[col_rule]) if col_rule else ""
        enum_text = norm(row[col_enum]) if col_enum else ""

        table_rules = rules.setdefault(table, {})
        fr = table_rules.setdefault(field, {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(), "tips": ""})

        text = f"{r} {enum_text}".strip()

        # 解析是否必填
        if "不为空" in text or "必填" in text:
            fr["required"] = True

        # 解析类型/长度（尽量多抓一些）
        # “整数部分小于等于24位，小数部分小于等于2位”
        m_int = re.search(r"整数部分[^\d]*(\d+)\s*位", text)
        m_dec = re.search(r"小数部分[^\d]*(\d+)\s*位", text)
        if m_int:
            fr["int_digits"] = int(m_int.group(1))
        if m_dec:
            fr["dec_digits"] = int(m_dec.group(1))

        # 长度= / 长度≤ / 长度小于等于 / 长度不超过
        m_eq = re.search(r"长度\s*[=＝]\s*(\d+)", text)
        m_le = re.search(r"(长度\s*[≤<=]\s*|长度小于等于|长度不超过)(\d+)", text)
        if m_eq:
            fr["len_eq"] = int(m_eq.group(1))
        if m_le:
            fr["len_max"] = int(m_le.group(2))

        # 内嵌枚举字符串
        # 例如 “1-男 | 2-女 | 9-未知的性别”
        # 或 “a|b|c|...”
        maybe_enum = []
        # 先看“允许值/枚举列”
        if enum_text:
            maybe_enum += split_enum_string(enum_text)
        # 再尝试从规则说明里抽 “x | y | z” 结构（保守处理：若命中了大量 |）
        if ("|" in r) and (len(r.split("|")) >= 2):
            maybe_enum += split_enum_string(r)

        if maybe_enum:
            # 枚举项清洗
            cleaned = []
            for item in maybe_enum:
                item = item.strip()
                # 去掉中文顿号逗号影响
                item = item.replace("，", ",")
                cleaned.append(item)
            fr["enum"].update([x for x in cleaned if x])

        # 额外记录提示原文
        if text:
            fr["tips"] = text

    # 再读取 码值 表，优先覆盖/补充枚举
    codes_sheet_try = [codes_sheet]
    if codes_sheet != "码值表":
        codes_sheet_try.append("码值表")
    for cs in codes_sheet_try:
        if cs not in xls.sheet_names:
            continue
        df_codes = pd.read_excel(xls, sheet_name=cs)

        # 尝试识别列
        col_field2 = pick_col(df_codes, ["字段", "字段名", "字段名称", "列名"])
        col_enum2  = pick_col(df_codes, ["枚举", "允许值", "取值", "枚举值", "代码-名称", "代码名称"])
        # 也兼容 “代码” + “名称” 两列
        col_code   = pick_col(df_codes, ["代码", "码", "编码", "值"])
        col_name   = pick_col(df_codes, ["名称", "含义", "文本"])

        if not col_field2:
            continue

        # 有“枚举”列：一行一个字段，里面是“a | b | c”
        if col_enum2 and (col_enum2 in df_codes.columns):
            for _, row in df_codes.iterrows():
                f = norm(row[col_field2])
                if not f:
                    continue
                enum_text = norm(row[col_enum2])
                items = split_enum_string(enum_text)
                if not items:
                    continue
                # 码值优先：覆盖/合并
                # 注意：这里没有表名维度（同名字段不同表的情况，默认共享一套枚举）
                for table in rules:
                    if f in rules[table]:
                        rules[table][f]["enum"] = set(items)  # 优先覆盖
                # 若某表尚未声明该字段，但 codes 有枚举，也建立
                for table in rules:
                    if f not in rules[table]:
                        rules[table][f] = {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(items), "tips": ""}

        # “代码+名称” 纵向枚举：需要 groupby 字段
        elif col_code and col_name and (col_code in df_codes.columns) and (col_name in df_codes.columns):
            for f, grp in df_codes.groupby(col_field2):
                f = norm(f)
                items = []
                for _, r2 in grp.iterrows():
                    code = norm(r2[col_code])
                    name = norm(r2[col_name])
                    if code and name:
                        items.append(f"{code}-{name}")
                    elif code:
                        items.append(code)
                    elif name:
                        items.append(name)
                if not items:
                    continue
                # 覆盖或新建
                for table in rules:
                    if f in rules[table]:
                        rules[table][f]["enum"] = set(items)
                for table in rules:
                    if f not in rules[table]:
                        rules[table][f] = {"required": False, "type": "", "len_eq": None, "len_max": None, "enum": set(items), "tips": ""}

    return rules

# -------------- 业务内置规则 --------------

EXACT_LENGTH_FIELDS = {
    # 明确等长（即使 length-mode = max 也强制等长）
    "发薪时间": 19,
}

# 人工成本字段集合（不含“非货币性福利”）
HR_COST_SUM_FIELDS = [
    "职工工资总额","社会保险费用","住房公积金","住房补贴","企业年金和职业年金",
    "补充医疗保险","福利费用","劳动保护费","工会经费","教育培训经费",
    "技术奖酬金及业务设计奖","辞退福利","股份支付","其他人工成本","劳务派遣费"
]

def check_listing_type(value: str) -> Optional[str]:
    """
    上市类型：只能由 a~i 与竖线组成；不可重复；若包含 i（非上市），则必须单独出现。
    允许示例：a|c|f 、 i
    """
    s = norm(value)
    if s == "":
        return None
    s = s.replace("｜", "|")
    toks = [t.strip() for t in s.split("|") if t.strip()]
    if not toks:
        return "上市类型为空或格式不正确"
    # 字符校验
    for t in toks:
        if not re.fullmatch(r"[a-i]", t):
            return "上市类型仅允许 a~i，用 | 分隔"
    # 去重
    if len(set(toks)) != len(toks):
        return "上市类型不能出现重复值"
    # i 单独
    if "i" in toks and len(toks) > 1:
        return "上市类型中 i（非上市）不能与其他值同时出现"
    return None

# -------------- 校验主函数 --------------

def validate_dataframe(
    df: pd.DataFrame,
    table: str,
    rules: Dict[str, Dict[str, Any]],
    length_mode: str = "max",
    pk_map: Optional[Dict[str, List[str]]] = None
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回：错误DataFrame、带__校验错误__的标注DataFrame
    """

    # === 主键拼接：根据 --pk 映射或自动兜底 ===
    def pk_of(row_idx: int) -> str:
        cols: List[str] = []
        # 1) 优先使用命令行 --pk 传入的映射
        if pk_map:
            # 精确表名
            if table in pk_map:
                cols = pk_map[table]
            else:
                # 容错：包含匹配
                for k in pk_map:
                    if (k in table) or (table in k):
                        cols = pk_map[k]
                        break
        # 2) 兜底
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
                    cols = group
                    break

        parts = []
        if cols:
            for c in cols:
                v = df.at[row_idx, c] if c in df.columns else None
                if v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v.strip()==""):
                    parts.append("")
                else:
                    parts.append(str(v).strip())
            return "|" + "|".join(parts)
        else:
            return ""

    errors: List[Dict[str, Any]] = []
    annotated_msgs: Dict[int, List[str]] = {}

    # 行遍历（保持原顺序）
    n = len(df)
    for idx in range(n):
        row = df.iloc[idx]
        row_msgs: List[str] = []

        # 字段级检查
        for field, fr in rules.items():
            val = row.get(field, None)
            sval = norm(val)

            # 1) 必填
            if fr.get("required") and sval == "":
                errors.append({
                    "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                    "字段": field, "错误类型": "缺失", "错误信息": "不能为空", "原始值": sval, "允许值": ""
                })
                row_msgs.append(f"[{field}] 不能为空")
                continue

            if sval == "":
                # 允许为空时，不做后续类型/长度/枚举检查
                continue

            # 2) 长度
            # 2.1 exact length（强制）
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
                # 2.2 规则里描述的长度
                len_eq = fr.get("len_eq")
                len_max = fr.get("len_max")
                # --length-mode 影响 len_eq：max 模式下将 len_eq 当作 len_max
                if length_mode == "max" and len_eq is not None:
                    len_max = max(len_max or 0, len_eq)
                    len_eq = None
                if len_eq is not None and len(sval) != int(len_eq):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "长度不符",
                        "错误信息": f"长度应={len_eq}",
                        "原始值": sval, "允许值": f"长度={len_eq}"
                    })
                    row_msgs.append(f"[{field}] 长度应={len_eq}")
                if len_max is not None and len(sval) > int(len_max):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": field, "错误类型": "长度超限",
                        "错误信息": f"长度应≤{len_max}",
                        "原始值": sval, "允许值": f"长度≤{len_max}"
                    })
                    row_msgs.append(f"[{field}] 长度应≤{len_max}")

            # 3) 枚举
            enum_set: Set[str] = set(fr.get("enum") or [])
            if enum_set:
                # 特殊：上市类型单独函数检查
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
                    # 一般枚举：要求“值”必须完全命中允许集（严格：‘男’与‘1-男’不等同）
                    if sval not in enum_set:
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": field, "错误类型": "取值非法",
                            "错误信息": "取值不在枚举中", "原始值": sval,
                            "允许值": " | ".join(sorted(enum_set))
                        })
                        row_msgs.append(f"[{field}] 取值不在枚举中")

        # 4) 表内计算（按表名）
        # 企业人工成本情况表
        if table == "中央企业各级单位人工成本情况表":
            total = try_decimal(row.get("企业人工成本总额"))
            if total is not None:
                subs = [try_decimal(row.get(c)) or Decimal("0") for c in HR_COST_SUM_FIELDS]
                sumv = sum(subs, Decimal("0"))
                if total < sumv:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "企业人工成本总额", "错误类型": "不满足下限",
                        "错误信息": f"应≥ 明细之和（{HR_COST_SUM_FIELDS}）",
                        "原始值": str(total), "允许值": f"期望≥{sumv}"
                    })
                    row_msgs.append(f"[企业人工成本总额] 应≥明细之和（期望≥{sumv} 实际{total}）")

        # 职工收入情况表：实发数 = 总收入 + 工资总额外的福利费用 - 应扣合计
        if table == "中央企业职工收入情况表":
            total_income = try_decimal(row.get("总收入"))
            extra_welfare = try_decimal(row.get("工资总额外的福利费用"))
            deduction = try_decimal(row.get("应扣合计"))
            actual_pay = try_decimal(row.get("实发数"))
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

        # 农民工情况表：块内 A = A1 + A2
        if table == "中央企业农民工情况表":
            blocks = [
                ("直接签订用工合同农民工费用总额", "其中：工资总额.1", "其中：各类保险总额.1"),
                ("劳务派遣形式农民工费用总额", "其中：工资总额.2", "其中：各类保险总额.2"),
                ("劳务外包和业务外包农民工费用总额", "其中：工资总额.3", "其中：各类保险总额.3"),
                ("其他农民工费用总额", "其中：工资总额.4", "其中：各类保险总额.4"),
            ]
            tol = Decimal("0.01")
            for total_name, w_name, ins_name in blocks:
                total_v = try_decimal(row.get(total_name))
                w_v = try_decimal(row.get(w_name))
                ins_v = try_decimal(row.get(ins_name))
                if all(v is not None for v in [total_v, w_v, ins_v]):
                    expect = (w_v or Decimal("0")) + (ins_v or Decimal("0"))
                    if (total_v - expect).copy_abs() > tol:
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                            "字段": total_name, "错误类型": "等式不满足",
                            "错误信息": f"{total_name} 应= {w_name}+{ins_name}",
                            "原始值": str(total_v), "允许值": f"期望={expect}"
                        })
                        row_msgs.append(f"[{total_name}] 应={w_name}+{ins_name}（期望={expect} 实际{total_v}）")

            # 总费用 = (其中：工资总额 + 其中：各类保险总额) = 四块合计
            g_total = try_decimal(row.get("农民工总费用"))
            g_w = try_decimal(row.get("其中：工资总额"))
            g_i = try_decimal(row.get("其中：各类保险总额"))
            if all(v is not None for v in [g_total, g_w, g_i]):
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
                try_decimal(row.get("直接签订用工合同农民工费用总额")) or Decimal("0"),
                try_decimal(row.get("劳务派遣形式农民工费用总额")) or Decimal("0"),
                try_decimal(row.get("劳务外包和业务外包农民工费用总额")) or Decimal("0"),
                try_decimal(row.get("其他农民工费用总额")) or Decimal("0"),
            ], Decimal("0"))
            if g_total is not None and (g_total - parts_total).copy_abs() > Decimal("0.01"):
                errors.append({
                    "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                    "字段": "农民工总费用", "错误类型": "等式不满足",
                    "错误信息": "农民工总费用应=四类费用合计",
                    "原始值": str(g_total), "允许值": f"期望={parts_total}"
                })
                row_msgs.append(f"[农民工总费用] 应=四类费用合计（期望={parts_total} 实际{g_total}）")

        # 职工收入情况表：岗位层级=91-其他 ⇒ 是否在岗 必须为“否”类
        if table == "中央企业职工收入情况表":
            pos = norm(row.get("岗位层级"))
            if pos.startswith("91-") or pos == "91":
                on_job = norm(row.get("是否在岗"))
                # 允许：2-否*, 3-否*, 4-否*, 5-否*
                if not (on_job.startswith("2-") or on_job.startswith("3-") or on_job.startswith("4-") or on_job.startswith("5-")):
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk_of(idx),
                        "字段": "是否在岗", "错误类型": "取值非法",
                        "错误信息": "当岗位层级=91-其他时，“是否在岗”必须为否类（2/3/4/5开头）",
                        "原始值": on_job, "允许值": "2-否...,3-否...,4-否...,5-否..."
                    })
                    row_msgs.append("[是否在岗] 岗位层级=91-其他时必须选择否类")

        if row_msgs:
            annotated_msgs.setdefault(idx, []).extend(row_msgs)

    # === 生成标注sheet ===
    annotated_df = df.copy()
    anno_col = "__校验错误__"
    if anno_col not in annotated_df.columns:
        annotated_df[anno_col] = ""
    for idx, msgs in annotated_msgs.items():
        annotated_df.at[idx, anno_col] = "；".join(msgs)

    # === 错误明细 DataFrame ===
    err_df = pd.DataFrame(errors, columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值"])
    return err_df, annotated_df

# -------- 职工收入表 跨行一致性检查（操作类型一致） --------

def cross_check_employee_ops(df_emp: pd.DataFrame) -> pd.DataFrame:
    """
    同一（统计年月, 子企业统一社会信用代码）下，操作类型必须一致。
    返回错误明细 DataFrame（可为空）
    """
    errs: List[Dict[str, Any]] = []
    need_cols = ["统计年月", "子企业统一社会信用代码", "操作类型"]
    if not all(c in df_emp.columns for c in need_cols):
        return pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"])

    for (ym, code), grp in df_emp.groupby(["统计年月", "子企业统一社会信用代码"], dropna=False):
        uniq = grp["操作类型"].astype(str).str.strip().unique().tolist()
        uniq = [u for u in uniq if u != "" and u.lower() != "nan"]
        if len(uniq) > 1:
            rows = (grp.index + 2).tolist()
            errs.append({
                "统计年月": ym,
                "子企业统一社会信用代码": code,
                "问题描述": "同一子企业在同一统计年月的【操作类型】不一致",
                "涉及行号": ",".join(map(str, rows)),
                "涉及取值": " | ".join(uniq),
            })
    return pd.DataFrame(errs)

# -------------- CLI --------------

def parse_pk_map(pk_arg: Optional[str]) -> Dict[str, List[str]]:
    """
    将 --pk "表A:主键1,主键2; 表B:主键1,主键2" 解析成 dict
    """
    out: Dict[str, List[str]] = {}
    if not pk_arg:
        return out
    # 支持 ; 或 | 分隔多个表映射
    for seg in re.split(r"[;|]", pk_arg):
        seg = seg.strip()
        if not seg:
            continue
        if ":" not in seg:
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

    data_path = args.data
    out_path = args.output
    rules_xlsx = Path(args.rules_xlsx)

    # 读取数据
    dfs = read_data_any(data_path)

    # 编译规则
    rules_all = compile_rules_from_excel(rules_xlsx, sheet_name=args.sheet, codes_sheet=args.codes_sheet)

    # 解析主键映射
    pk_map = parse_pk_map(args.pk)

    # 写出
    # 选择引擎：xlsxwriter 优先（若安装）
    engine = "xlsxwriter"
    try:
        import xlsxwriter  # noqa
        engine = "xlsxwriter"
    except Exception:
        engine = None

    with pd.ExcelWriter(out_path, engine=engine) as xw:
        # 每张表单独处理
        for t, df in dfs.items():
            table = normalize_table_name(t)
            table_rules = rules_all.get(table, {})

            # 别名统一（最小侵入）
            try:
                df = _apply_column_aliases(df, table_rules)
            except Exception:
                pass

            err_df, annotated_df = validate_dataframe(df, table, table_rules, length_mode=args.length_mode, pk_map=pk_map)

            # 可空字段过滤（最小侵入）
            try:
                err_df, annotated_df = _filter_optional_errors(table, err_df, annotated_df)
            except Exception:
                pass


            # 错误明细
            err_sheet = f"错误-{table}"
            if err_df.empty:
                # 输出空表头
                empty_err = pd.DataFrame(columns=["表名","行号","主键","字段","错误类型","错误信息","原始值","允许值"])
                empty_err.to_excel(xw, index=False, sheet_name=err_sheet)
            else:
                err_df.to_excel(xw, index=False, sheet_name=err_sheet)

            # 标注
            anno_sheet = f"标注-{table}"
            annotated_df.to_excel(xw, index=False, sheet_name=anno_sheet)

        # 跨行一致性（仅对 职工收入情况表）
        if "中央企业职工收入情况表" in dfs:
            cross_df = cross_check_employee_ops(dfs["中央企业职工收入情况表"])
            cross_sheet = "跨行-中央企业职工收入情况表"
            if cross_df.empty:
                pd.DataFrame(columns=["统计年月","子企业统一社会信用代码","问题描述","涉及行号","涉及取值"]).to_excel(xw, index=False, sheet_name=cross_sheet)
            else:
                cross_df.to_excel(xw, index=False, sheet_name=cross_sheet)

    print(f"完成：输出 {out_path}")

if __name__ == "__main__":
    sys.exit(main())

# [WARN] 未自动找到 validate_dataframe 调用位置，未注入精度检查调用。
# 请在 validate_dataframe(...) 之后手动加入：
#    prec_errs, annotated_df = _check_money_precision(annotated_df, t, pk_map)
#    errs.extend(prec_errs)
