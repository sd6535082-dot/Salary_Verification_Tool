# -*- coding: utf-8 -*-
"""
SOE Validator (v2.2 full v2) — 表内 + 表间
新增：控制“主数据重复”提示的输出位置
    --master-dup-report {inline,summary,off}  默认为 summary

python soe_validator_v22_full_v2_fixed.py \
  --data "/Users/rainmor1990/Desktop/2.0基础校验/待校验数据.xlsx" \
  --rules-xlsx "/Users/rainmor1990/Desktop/2.0基础校验/数据标准2.0数据采集校验规则_V2.2.xlsx" \
  --sheet "央企端-表内校验"\
  --codes-sheet "码值表" \
  --tables "中央企业各级次单位信息情况表" "中央企业职工收入情况表" "中央企业各级单位人工成本情况表" "中央企业职工中长期激励情况表-奖励现金型" "中央企业职工中长期激励情况表-奖励股权型" "中央企业职工中长期激励情况表-出售股权型" "中央企业农民工情况表" "中央企业各级负责人年度薪酬情况表" \
  --pk "中央企业职工收入情况表:统一社会信用代码,证件号码,姓名" \
  --length-mode max \
  --master-dup-report off \
  --output "/Users/rainmor1990/Desktop/2.0基础校验/validation_errors_v22.xlsx"


"""

import argparse
import json
import re
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, DefaultDict, Set
from collections import defaultdict

import pandas as pd


# -----------------------------
# 规则编译（从 Excel -> JSON）
# -----------------------------

COL_ALIASES = {
    "table": ["表名", "所属表", "表中文名", "表"],
    "field": ["字段", "字段名", "列名", "字段中文名"],
    "required": ["是否必填", "必填", "必填项"],
    "type": ["类型", "数据类型", "字段类型"],
    "max_len": ["最大长度", "长度上限", "字段长度", "长度"],
    "exact_len": ["固定长度", "长度等于"],
    "int_len": ["整数位", "整数长度"],
    "dec_len": ["小数位", "小数长度", "精度"],
    "charset": ["字符集", "正则", "允许字符", "字符规则", "字符约束", "pattern"],
    "enum": ["枚举", "枚举值", "下拉选项", "取值范围", "码值", "允许值", "字典项", "下拉值"],
    "enum_ref": ["枚举编码", "字典编码", "码值编码", "数据字典编码"],
}

TRUE_SET = {"是", "Y", "y", "true", "True", "1", 1}
NUM_TYPE_HINTS = {"number", "numeric", "decimal", "金额", "数值", "小数", "浮点", "float"}
INT_TYPE_HINTS = {"int", "integer", "整数"}


def _find_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    for a in aliases:
        if a in df.columns:
            return a
    norm = {re.sub(r"\s+", "", c): c for c in df.columns}
    for a in aliases:
        key = re.sub(r"\s+", "", a)
        if key in norm:
            return norm[key]
    return None


def _coerce_bool(v) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip()
    return s in TRUE_SET


def _coerce_int(v) -> Optional[int]:
    if pd.isna(v) or str(v).strip() == "":
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def _coerce_type(v) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    s_low = s.lower()
    if s_low in INT_TYPE_HINTS:
        return "int"
    if s_low in NUM_TYPE_HINTS:
        return "number"
    return "string"


def _split_enum_list(v: Any) -> List[str]:
    if pd.isna(v):
        return []
    s = str(v).strip()
    if not s:
        return []
    parts = re.split(r"[|、，,;\n\r]+", s)
    return [p.strip() for p in parts if p.strip() != ""]


def compile_rules_from_excel(xlsx_path: Path, sheet_name: str = "央企端-表内校验",
                             codes_sheet: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    xls = pd.ExcelFile(xlsx_path)
    if sheet_name not in xls.sheet_names:
        raise ValueError(f"规则文件中未找到Sheet：{sheet_name}，可用：{xls.sheet_names}")

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)

    codebook: Dict[str, List[str]] = {}
    if codes_sheet and codes_sheet in xls.sheet_names:
        df_codes = pd.read_excel(xlsx_path, sheet_name=codes_sheet)
        code_col = None
        val_col = None
        for c in df_codes.columns:
            if any(k in str(c) for k in ["编码", "字典", "码值编码"]):
                code_col = c
            if any(k in str(c) for k in ["值", "名称", "取值", "内容"]):
                if val_col is None:
                    val_col = c
        if code_col and val_col:
            for k, grp in df_codes.groupby(code_col):
                vals = [str(v).strip() for v in grp[val_col].tolist() if str(v).strip() != ""]
                if vals:
                    codebook[str(k).strip()] = vals

    col_table   = _find_col(df, COL_ALIASES["table"])
    col_field   = _find_col(df, COL_ALIASES["field"])
    col_req     = _find_col(df, COL_ALIASES["required"])
    col_type    = _find_col(df, COL_ALIASES["type"])
    col_max_len = _find_col(df, COL_ALIASES["max_len"])
    col_exact   = _find_col(df, COL_ALIASES["exact_len"])
    col_int     = _find_col(df, COL_ALIASES["int_len"])
    col_dec     = _find_col(df, COL_ALIASES["dec_len"])
    col_charset = _find_col(df, COL_ALIASES["charset"])
    col_enum    = _find_col(df, COL_ALIASES["enum"])
    col_enumref = _find_col(df, COL_ALIASES["enum_ref"])

    if any(c is None for c in [col_table, col_field]):
        raise ValueError("规则表缺少必要列：表名/字段名")

    compiled: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        table = str(r[col_table]).strip()
        field = str(r[col_field]).strip()
        if not table or not field or table == "nan" or field == "nan":
            continue
        tr = compiled.setdefault(table, {}).setdefault(field, {})

        if col_req:     tr["required"] = _coerce_bool(r[col_req])
        if col_type:    tr["type"] = _coerce_type(r[col_type])
        if col_max_len:
            ml = _coerce_int(r[col_max_len]);    tr["max_len"] = ml if ml is not None else tr.get("max_len")
        if col_exact:
            el = _coerce_int(r[col_exact]);      tr["exact_len"] = el if el is not None else tr.get("exact_len")
        if col_int:
            il = _coerce_int(r[col_int]);        tr["int_len"] = il if il is not None else tr.get("int_len")
        if col_dec:
            dl = _coerce_int(r[col_dec]);        tr["dec_len"] = dl if dl is not None else tr.get("dec_len")
        if col_charset:
            cs = r[col_charset];                  tr["allowed_charset"] = cs.strip() if isinstance(cs, str) and cs.strip() else tr.get("allowed_charset")

        values: List[str] = []
        if col_enum and not pd.isna(r[col_enum]):
            values.extend(_split_enum_list(r[col_enum]))
        if col_enumref and isinstance(r[col_enumref], str) and r[col_enumref].strip():
            ref = r[col_enumref].strip()
            values.extend(codebook.get(ref, []))
        if values:
            tr["dropdown"] = {"values": sorted(list(dict.fromkeys(values)))}

    return compiled


# -----------------------------
# 通用工具
# -----------------------------

def excel_like_str(val) -> str:
    if pd.isna(val):
        return ""
    return str(val)

def to_decimal(val, places: int = 2) -> Optional[Decimal]:
    if pd.isna(val) or val == "":
        return None
    try:
        d = Decimal(str(val))
        q = Decimal(10) ** -places
        return d.quantize(q, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None

def count_decimal_places(d: Decimal) -> int:
    s = format(d, 'f')
    return len(s.split('.')[-1]) if '.' in s else 0

def sheet_loader(data_path: Path, table_names: List[str]) -> Dict[str, pd.DataFrame]:
    frames = {}
    if data_path.is_dir():
        for t in table_names:
            candidates = [
                data_path / f"{t}.xlsx",
                data_path / f"{t}.xls",
                data_path / f"{t}.xlsm",
            ]
            normalized = re.sub(r'[^\w\u4e00-\u9fff]+', '', t)
            candidates += [
                data_path / f"{normalized}.xlsx",
                data_path / f"{normalized}.xls",
            ]
            file = None
            for c in candidates:
                if c.exists():
                    file = c
                    break
            if file is None:
                raise FileNotFoundError(f"目录 {data_path} 未找到与表名匹配的文件：{t}.xlsx")
            frames[t] = pd.read_excel(file)
    else:
        xls = pd.ExcelFile(data_path)
        for t in table_names:
            if t not in xls.sheet_names:
                raise ValueError(f"工作簿 {data_path} 未包含工作表：{t}")
            frames[t] = pd.read_excel(data_path, sheet_name=t)
    return frames


def validate_listing_type(value: str) -> Tuple[bool, str]:
    if value is None:
        return False, "不能为空"
    s = str(value).strip()
    if s == "":
        return False, "不能为空"
    if not re.fullmatch(r'^[a-i](\|[a-i])*$', s):
        return False, "仅允许字母a~i，使用“|”分隔（如 a|b|c 或 i）"
    parts = s.split('|')
    if len(parts) != len(set(parts)):
        return False, "不允许重复值（如 a|a）"
    if 'i' in parts and len(parts) > 1:
        return False, "当选择 i(非上市) 时不能与其他值并存"
    return True, ""


def coerce_required(val) -> bool:
    if pd.isna(val):
        return False
    if isinstance(val, str) and val.strip() == "":
        return False
    return True


def build_pk(row: pd.Series, df_cols: List[str], user_pk: List[str]) -> str:
    parts = []
    for c in user_pk:
        if c in df_cols and not pd.isna(row.get(c, None)):
            parts.append(f"{c}={row.get(c)}")
    if parts:
        return " | ".join(parts)
    auto_cols = ["统一社会信用代码", "子企业统一社会信用代码", "证件号码", "姓名", "子企业单位名称", "所属上级企业名称", "单位名称"]
    for c in auto_cols:
        if c in df_cols and not pd.isna(row.get(c, None)):
            parts.append(f"{c}={row.get(c)}")
    return " | ".join(parts)


# -----------------------------
# 表内校验
# -----------------------------

def validate_dataframe(df: pd.DataFrame,
                       table: str,
                       rules: Dict[str, Any],
                       length_mode: str,
                       pk_map: Dict[str, List[str]]) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    errors: List[Dict[str, Any]] = []
    annotated_msgs: DefaultDict[int, List[str]] = defaultdict(list)

    rule_fields = list(rules.keys())
    missing_cols = [c for c in rule_fields if c not in df.columns]
    extra_cols   = [c for c in df.columns if c not in rule_fields]

    if missing_cols or extra_cols:
        errors.append({
            "表名": table, "行号": "", "主键": "",
            "字段": ",".join(missing_cols) if missing_cols else "",
            "错误类型": "字段不匹配",
            "错误信息": f"缺少字段：{missing_cols}; 多余字段：{extra_cols}",
            "建议修复": "请使用最新模板/确保列名与规则完全一致",
            "原始值": ""
        })

    for idx, row in df.iterrows():
        pk = build_pk(row, df.columns.tolist(), pk_map.get(table, []))

        for field, rule in rules.items():
            if field not in df.columns:
                continue

            val = row[field]
            vstr = excel_like_str(val)

            # 必填
            if rule.get("required", False):
                if not coerce_required(val):
                    msg = "必填字段为空"
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk,
                        "字段": field, "错误类型": "必填缺失", "错误信息": msg,
                        "建议修复": "按模板填写，若无请填0或按说明填'无'",
                        "原始值": vstr
                    })
                    annotated_msgs[idx].append(f"[{field}] {msg}")
                    continue

            # 枚举
            dropdown = rule.get("dropdown")
            if dropdown and vstr != "":
                allowed = set(dropdown.get("values", []))
                if vstr not in allowed:
                    msg = f"值 '{vstr}' 不在允许集合（示例：{sorted(list(allowed))[:5]}...）"
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk,
                        "字段": field, "错误类型": "取值不在给定选项", "错误信息": msg,
                        "建议修复": "使用下拉选项，不要手填或复制旧版码值",
                        "原始值": vstr
                    })
                    annotated_msgs[idx].append(f"[{field}] 取值非法")

            # 上市类型
            if table == "中央企业各级次单位信息情况表" and field == "上市类型" and vstr != "":
                ok, msg = validate_listing_type(vstr)
                if not ok:
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk,
                        "字段": field, "错误类型": "上市类型格式错误", "错误信息": msg,
                        "建议修复": "示例：a|b|c 或 i",
                        "原始值": vstr
                    })
                    annotated_msgs[idx].append(f"[上市类型] {msg}")

            # 字符集
            allowed_charset = rule.get("allowed_charset")
            if allowed_charset and vstr != "":
                if not re.fullmatch(allowed_charset, vstr):
                    msg = f"不满足字符集限制：{allowed_charset}"
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk,
                        "字段": field, "错误类型": "非法字符", "错误信息": msg,
                        "建议修复": "去除非法字符或更换输入方式",
                        "原始值": vstr
                    })
                    annotated_msgs[idx].append(f"[{field}] 非法字符")

            # 长度
            max_len = rule.get("max_len")
            exact_len = rule.get("exact_len")
            if vstr != "":
                if length_mode == "strict" and exact_len:
                    if len(vstr) != int(exact_len):
                        msg = f"长度应= {exact_len}，当前 {len(vstr)}"
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk,
                            "字段": field, "错误类型": "长度不等于要求", "错误信息": msg,
                            "建议修复": "按模板长度填写",
                            "原始值": vstr
                        })
                        annotated_msgs[idx].append(f"[{field}] 长度不等于{exact_len}")
                else:
                    limit = max_len or exact_len
                    if limit and len(vstr) > int(limit):
                        msg = f"长度应≤ {limit}，当前 {len(vstr)}"
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk,
                            "字段": field, "错误类型": "长度超限", "错误信息": msg,
                            "建议修复": "精简文本或去掉多余空格/符号",
                            "原始值": vstr
                        })
                        annotated_msgs[idx].append(f"[{field}] 长度超限")

            # 数值/小数位
            ftype = rule.get("type")
            if ftype in ("number", "int") and vstr != "":
                d = to_decimal(vstr, places=rule.get("dec_len", 2) or 2)
                if d is None:
                    msg = "数字格式错误（勿带千分位/单位/空格）"
                    errors.append({
                        "表名": table, "行号": idx + 2, "主键": pk,
                        "字段": field, "错误类型": "数字格式错误", "错误信息": msg,
                        "建议修复": "仅填写纯数字，单位见字段说明",
                        "原始值": vstr
                    })
                    annotated_msgs[idx].append(f"[{field}] 数字格式错")
                else:
                    int_len = rule.get("int_len")
                    if int_len:
                        digits_before = len(str(d).split('.')[0].replace('-', ''))
                        if digits_before > int(int_len):
                            msg = f"整数位应≤ {int_len}"
                            errors.append({
                                "表名": table, "行号": idx + 2, "主键": pk,
                                "字段": field, "错误类型": "整数位超限", "错误信息": msg,
                                "建议修复": "核对数量级是否正确；去除多余字符",
                                "原始值": vstr
                            })
                            annotated_msgs[idx].append(f"[{field}] 整数位超限")
                    dec_len = rule.get("dec_len")
                    if dec_len is not None:
                        if count_decimal_places(d) > int(dec_len):
                            msg = f"小数位应≤ {dec_len}"
                            errors.append({
                                "表名": table, "行号": idx + 2, "主键": pk,
                                "字段": field, "错误类型": "小数位超限", "错误信息": msg,
                                "建议修复": "四舍五入至两位小数",
                                "原始值": vstr
                            })
                            annotated_msgs[idx].append(f"[{field}] 小数位超限")

        # 公式：实发数
        if table == "中央企业职工收入情况表":
            needed = ["实发数", "总收入", "工资总额外的福利费用", "应扣合计"]
            if all(col in df.columns for col in needed):
                d_sf = to_decimal(row["实发数"], 2)
                d_zsr = to_decimal(row["总收入"], 2) or Decimal("0.00")
                d_fl = to_decimal(row["工资总额外的福利费用"], 2) or Decimal("0.00")
                d_yk = to_decimal(row["应扣合计"], 2) or Decimal("0.00")
                if d_sf is not None:
                    expect = (d_zsr + d_fl - d_yk).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if abs(expect - d_sf) > Decimal("0.01"):
                        msg = f"应= 总收入+工资总额外的福利费用-应扣合计（期望{expect} 实际{d_sf}）"
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk,
                            "字段": "实发数", "错误类型": "公式不匹配", "错误信息": msg,
                            "建议修复": "直接填计算后的数值，保留两位小数",
                            "原始值": str(row.get("实发数"))
                        })
                        annotated_msgs[idx].append(f"[实发数] 公式不匹配")

        # 公式：企业人工成本总额
        if table == "中央企业各级单位人工成本情况表":
            sum_items = ["职工工资总额", "社会保险费用", "住房公积金", "住房补贴", "非货币性福利", "股份支付", "其他人工成本", "劳务派遣费"]
            if "企业人工成本总额" in df.columns and all(col in df.columns for col in sum_items):
                d_total = to_decimal(row["企业人工成本总额"], 2)
                if d_total is not None:
                    parts = [to_decimal(row[c], 2) or Decimal("0.00") for c in sum_items]
                    expect = sum(parts, start=Decimal("0.00")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    if abs(expect - d_total) > Decimal("0.01"):
                        msg = f"应为 {sum_items} 之和（期望{expect} 实际{d_total}）"
                        errors.append({
                            "表名": table, "行号": idx + 2, "主键": pk,
                            "字段": "企业人工成本总额", "错误类型": "合计不匹配", "错误信息": msg,
                            "建议修复": "逐项核对，若无请填0，合计保留两位小数",
                            "原始值": str(row.get("企业人工成本总额"))
                        })
                        annotated_msgs[idx].append(f"[企业人工成本总额] 合计不匹配")

    annotated_df = df.copy()
    anno_col = "__校验错误__"
    annotated_df[anno_col] = ""
    for idx, msgs in annotated_msgs.items():
        annotated_df.at[idx, anno_col] = "；".join(msgs)

    return errors, annotated_df


# -----------------------------
# 表间校验（引用一致性 + 主数据重复）
# -----------------------------

REF_KEYS = [
    "统一社会信用代码",
    "子企业统一社会信用代码",
    "单位统一社会信用代码",
]

NAME_KEYS = [
    "子企业单位名称",
    "单位名称",
]

def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace('\u3000', ' ')
    return re.sub(r'\s+', '', s)

def detect_master_duplicates(df_master: pd.DataFrame) -> pd.DataFrame:
    """返回主数据重复的详细列表（按列逐个检测），不含空值"""
    records = []
    for col in REF_KEYS + NAME_KEYS:
        if col in df_master.columns:
            ser = df_master[col].astype(str).str.strip()
            ser = ser[ser != ""].dropna()
            dup_mask = ser.duplicated(keep=False)
            if dup_mask.any():
                dups = df_master.loc[dup_mask, [col]].copy()
                dups["重复列"] = col
                dups["重复值(规范化)"] = dups[col].astype(str).str.replace('\u3000',' ').str.replace(r'\s+','',regex=True)
                records.append(dups)
    if records:
        out = pd.concat(records, axis=0)
        out = out[["重复列", "重复值(规范化)", out.columns[0]]]
        out = out.rename(columns={out.columns[2]:"原始值"})
        out = out.drop_duplicates()
        return out
    return pd.DataFrame(columns=["重复列","重复值(规范化)","原始值"])


def cross_validate_against_master(df_master: pd.DataFrame,
                                  df_child: pd.DataFrame,
                                  child_table: str,
                                  pk_map: Dict[str, List[str]],
                                  inline_master_dup_msgs: bool,
                                  master_dup_msgs: List[str]) -> List[Dict[str, Any]]:
    errs: List[Dict[str, Any]] = []

    # 可选：把主数据重复的摘要信息打到子表
    if inline_master_dup_msgs:
        for m in master_dup_msgs:
            errs.append({
                "表名": child_table, "行号": "", "主键": "", "字段": "",
                "错误类型": "主数据重复", "错误信息": m,
                "建议修复": "请先去重表1关键字段（代码/名称），避免跨表匹配歧义",
                "原始值": ""
            })

    # Build index
    codes: Set[str] = set()
    names: Set[str] = set()
    for code_col in REF_KEYS:
        if code_col in df_master.columns:
            vals = [str(v).strip() for v in df_master[code_col].dropna().tolist() if str(v).strip() != ""]
            codes.update(vals)
    for name_col in NAME_KEYS:
        if name_col in df_master.columns:
            vals = [norm_text(v) for v in df_master[name_col].dropna().tolist() if str(v).strip() != ""]
            names.update(vals)

    # 子表逐行检查
    for idx, row in df_child.iterrows():
        pk = build_pk(row, df_child.columns.tolist(), pk_map.get(child_table, []))

        # code
        code = None
        for k in REF_KEYS:
            if k in df_child.columns and not pd.isna(row.get(k)) and str(row.get(k)).strip() != "":
                code = str(row.get(k)).strip()
                break

        # name
        name = None
        for k in NAME_KEYS:
            if k in df_child.columns and not pd.isna(row.get(k)) and str(row.get(k)).strip() != "":
                name = str(row.get(k)).strip()
                break

        # 1) 引用存在性
        if code and code not in codes:
            errs.append({
                "表名": child_table, "行号": idx + 2, "主键": pk,
                "字段": "统一社会信用代码/相关代码", "错误类型": "表间引用不存在",
                "错误信息": f"代码 {code} 未在表1中找到",
                "建议修复": "请先在表1新增/修正该单位，再在子表中引用",
                "原始值": code
            })

        if (not code) and name:
            n = norm_text(name)
            if n not in names:
                errs.append({
                    "表名": child_table, "行号": idx + 2, "主键": pk,
                    "字段": "单位名称", "错误类型": "表间引用不存在",
                    "错误信息": f"名称 {name} 未在表1中找到",
                    "建议修复": "请先在表1新增/修正该单位，再在子表中引用",
                    "原始值": name
                })

        # 2) 名称一致性（当 code + name 同时存在时）
        if code and name:
            master_names = set()
            for name_col in NAME_KEYS:
                if name_col in df_master.columns and "统一社会信用代码" in df_master.columns:
                    for _, r in df_master[df_master["统一社会信用代码"] == code].iterrows():
                        master_names.add(norm_text(r.get(name_col)))
                if name_col in df_master.columns and "子企业统一社会信用代码" in df_master.columns:
                    for _, r in df_master[df_master["子企业统一社会信用代码"] == code].iterrows():
                        master_names.add(norm_text(r.get(name_col)))
            if master_names:
                if norm_text(name) not in master_names:
                    errs.append({
                        "表名": child_table, "行号": idx + 2, "主键": pk,
                        "字段": "单位名称", "错误类型": "表间名称不一致",
                        "错误信息": f"代码 {code} 在表1对应名称为 {list(master_names)[:3]}，而本表填写为 {name}",
                        "建议修复": "以表1名称为准修正子表；若表1名称错误请先修正表1",
                        "原始值": name
                    })

    return errs


# -----------------------------
# 主流程
# -----------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="待校验数据：单个Excel工作簿（多Sheet）或目录（每表一个Excel）")
    parser.add_argument("--rules-xlsx", required=False, help="V2.2 规则Excel路径（推荐）")
    parser.add_argument("--sheet", required=False, default="央企端-表内校验", help="规则Sheet名（默认：央企端-表内校验）")
    parser.add_argument("--codes-sheet", required=False, help="码值/字典Sheet名（可选）")
    parser.add_argument("--rules-json", required=False, default="rules_intra_compiled.json", help="若无rules-xlsx则读取该JSON")
    parser.add_argument("--dump-rules-json", required=False, help="将编译后的规则另存为JSON（可选）")
    parser.add_argument("--tables", nargs="*", help="需要校验的表名（留空则对规则中全部表进行校验）")
    parser.add_argument("--length-mode", choices=["max", "strict"], default="max", help="长度模式：max=≤上限；strict=等于exact_len")
    parser.add_argument("--pk", action="append", help="主键映射：格式 表名:字段1,字段2 （可多次填写不同表）")
    parser.add_argument("--no-annotated", action="store_true", help="不输出“标注-表名”Sheet")
    parser.add_argument("--master-dup-report", choices=["inline","summary","off"], default="summary",
                        help="主数据重复提示输出位置：inline=写入各表间Sheet；summary=独立Sheet；off=不提示")
    parser.add_argument("--output", required=True, help="输出Excel文件路径（.xlsx）")
    args = parser.parse_args()

    # 规则
    if args.rules_xlsx:
        rules_compiled = compile_rules_from_excel(Path(args.rules_xlsx), sheet_name=args.sheet, codes_sheet=args.codes_sheet)
        if args.dump_rules_json:
            Path(args.dump_rules_json).write_text(json.dumps(rules_compiled, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        rp = Path(args.rules_json)
        if not rp.exists():
            raise FileNotFoundError(f"未找到规则：{rp}，请提供 --rules-xlsx 或 --rules-json")
        rules_compiled = json.loads(Path(args.rules_json).read_text(encoding="utf-8"))

    # 目标表
    target_tables = args.tables if args.tables else list(rules_compiled.keys())

    # 主键映射
    pk_map: Dict[str, List[str]] = {}
    if args.pk:
        for item in args.pk:
            if ":" in item:
                t, cols = item.split(":", 1)
                pk_map[t.strip()] = [c.strip() for c in cols.split(",") if c.strip()]

    # 加载数据
    data_path = Path(args.data)
    frames = sheet_loader(data_path, target_tables)

    # 写入引擎自动选择
    try:
        import xlsxwriter  # noqa
        _engine = "xlsxwriter"
    except Exception:
        _engine = "openpyxl"

    out_path = Path(args.output)
    if out_path.suffix.lower() != ".xlsx":
        raise ValueError("输出文件必须是 .xlsx")

    with pd.ExcelWriter(out_path, engine=_engine) as xw:
        overview_rows: List[Dict[str, Any]] = []

        # 表内校验 + 标注
        for t in target_tables:
            if t not in frames:
                overview_rows.append({"表名": t, "问题数": 1, "备注": "未找到该表数据"})
                pd.DataFrame([{"提示": f"未在数据源中找到表/Sheet：{t}"}]).to_excel(xw, index=False, sheet_name=f"错误-{t[:25]}")
                continue
            if t not in rules_compiled:
                overview_rows.append({"表名": t, "问题数": 1, "备注": "未找到该表规则"})
                pd.DataFrame([{"提示": f"规则未包含该表：{t}"}]).to_excel(xw, index=False, sheet_name=f"错误-{t[:25]}")
                continue

            df = frames[t]
            table_rules = rules_compiled[t]

            errs, annotated_df = validate_dataframe(df, t, table_rules, length_mode=args.length_mode, pk_map=pk_map)
            err_df = pd.DataFrame(errs) if errs else pd.DataFrame(columns=["表名","行号","主键","字段","错误类型","错误信息","建议修复","原始值"])
            err_df.to_excel(xw, index=False, sheet_name=f"错误-{t[:25]}")
            if not args.no_annotated:
                annotated_df.to_excel(xw, index=False, sheet_name=f"标注-{t[:24]}")
            overview_rows.append({"表名": t, "问题数": len(errs), "备注": ""})

        # 表间校验（以表1为主数据表）
        MASTER = "中央企业各级次单位信息情况表"
        if MASTER in frames:
            df_master = frames[MASTER]

            # 主数据重复检查：summary / inline / off
            master_dup_df = detect_master_duplicates(df_master)
            master_dup_msgs = []
            if not master_dup_df.empty:
                for col in (master_dup_df["重复列"].unique().tolist()):
                    master_dup_msgs.append(f"{col} 存在重复")
                if args.master_dup_report in ("summary", "inline"):
                    if args.master_dup_report == "summary":
                        master_dup_df.to_excel(xw, index=False, sheet_name="主数据-重复检查")
                        overview_rows.append({"表名": "主数据-重复检查", "问题数": len(master_dup_df), "备注": "表间"})
                # else off -> 不输出

            # 对照的子表
            child_pairs = [
                ("中央企业职工收入情况表", "表间-职工收入vs单位信息"),
                ("中央企业各级单位人工成本情况表", "表间-人工成本vs单位信息"),
            ]

            inline_flag = (args.master_dup_report == "inline")
            for child_name, sheet_alias in child_pairs:
                if child_name in frames:
                    cross_errs = cross_validate_against_master(df_master, frames[child_name], child_name,
                                                               pk_map, inline_flag, master_dup_msgs)
                    pd.DataFrame(cross_errs).to_excel(xw, index=False, sheet_name=sheet_alias[:31])
                    overview_rows.append({"表名": sheet_alias, "问题数": len(cross_errs), "备注": "表间"})
        else:
            pd.DataFrame([{"提示": "未找到主数据表（中央企业各级次单位信息情况表），无法做表间校验"}]).to_excel(xw, index=False, sheet_name="表间-提示")
            overview_rows.append({"表名": "表间-提示", "问题数": 1, "备注": "缺少主数据表"})

        pd.DataFrame(overview_rows).to_excel(xw, index=False, sheet_name="总览")

    print(f"完成：输出 {out_path}")

if __name__ == "__main__":
    main()
