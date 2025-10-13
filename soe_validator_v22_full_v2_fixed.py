# -*- coding: utf-8 -*-
"""
soe_validator_v22_full_v2_fixed.py
V2.2 最终整合版（修复人工成本合计漏项：支持“非货币性福利”与“技术奖酬金及业*设计奖”别名；
表内+表间；空表容错；字段级“长度=”；Sheet 名去重；忽略字段名不一致）
"""

import argparse
from decimal import Decimal, ROUND_HALF_UP
import json
import math
from pathlib import Path
import re
import sys
import warnings

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)

EXPECTED_TABLES = [
    "中央企业各级次单位信息情况表",
    "中央企业职工收入情况表",
    "中央企业职工中长期激励情况表-奖励现金型",
    "中央企业职工中长期激励情况表-奖励股权型",
    "中央企业职工中长期激励情况表-出售股权型",
    "中央企业各级单位人工成本情况表",
    "中央企业农民工情况表",
    "中央企业各级负责人年度薪酬情况表",
]

STRICT_HEADER_MATCH = False  # 按用户要求忽略“字段名与规则不一致”

def to_decimal(x, places=2):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return Decimal("0.00")
        d = Decimal(str(x))
        q = "0." + "0"*places
        return d.quantize(Decimal(q), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")

def read_data_any(path: Path) -> dict:
    data = {}
    if path.is_file():
        xls = pd.ExcelFile(path)
        for sn in xls.sheet_names:
            data[sn] = xls.parse(sn)
    elif path.is_dir():
        for t in EXPECTED_TABLES:
            candidates = sorted(list(path.glob(f"{t}.xlsx"))) or sorted(list(path.glob(f"{t}*.xlsx")))
            if candidates:
                df = pd.read_excel(candidates[0])
                data[t] = df
            else:
                data[t] = pd.DataFrame()
    else:
        raise FileNotFoundError(f"未找到数据路径：{path}")
    for t in EXPECTED_TABLES:
        data.setdefault(t, pd.DataFrame())
    return data

def compile_rules_from_excel(xlsx_path: Path, sheet_name="央企端-表内校验", codes_sheet="码值表") -> dict:
    rules = {t: [] for t in EXPECTED_TABLES}
    if not xlsx_path.exists():
        return rules
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    except Exception:
        return rules

    def find_col(*keys):
        for k in keys:
            for c in df.columns:
                if re.search(k, str(c), re.I):
                    return c
        return None

    col_tab = find_col("表名|表格|表中文名")
    col_field = find_col("字段|列名|字段名称")
    col_req = find_col("必填|是否必填")
    col_len = find_col("长度|字长|最大长度")
    col_dec = find_col("小数|精度")
    col_enum = find_col("枚举|码值|取值|选项")
    col_rule = find_col("规则|说明|校验")
    if col_tab is None or col_field is None:
        return rules

    for _, r in df.iterrows():
        t = str(r.get(col_tab, "")).strip()
        f = str(r.get(col_field, "")).strip()
        if not t or not f or t not in rules:
            continue
        it = {"field": f}
        req = str(r.get(col_req, "")).strip()
        it["required"] = True if req in ("是", "必填", "Y", "1", "yes", "Yes") else False
        ml = r.get(col_len, None)
        try:
            it["max_len"] = int(ml) if pd.notnull(ml) else None
        except Exception:
            it["max_len"] = None
        dc = r.get(col_dec, None)
        try:
            it["decimals"] = int(dc) if pd.notnull(dc) else None
        except Exception:
            it["decimals"] = None
        ev = r.get(col_enum, None)
        if pd.notnull(ev):
            s = str(ev).strip().replace("；",";").replace("，",",").replace("/", "|").replace("\\","|")
            parts = re.split(r"[|;,，；\s]+", s)
            it["enum"] = set([p for p in parts if p])
        else:
            it["enum"] = None

        it["raw_rule"] = str(r.get(col_rule, "")).strip() if pd.notnull(r.get(col_rule, "")) else ""

        # 从规则文本自动识别“长度等于”的语义（避免把“最长不超过”误判为等于）
        it["len_equals"] = False
        rr = str(it.get("raw_rule","") or "")
        rr_norm = rr.replace(" ", "")
        negative_tokens = ("不超过","最多","至多","不大于","≤","小于等于","不高于","以内","以下","不多于","不超")
        positive_tokens = ("长度等于","必须等于","固定长度","长度应等于","严格等于")
        if it.get("max_len"):
            if not any(tok in rr_norm for tok in negative_tokens):
                if any(tok in rr_norm for tok in positive_tokens) or re.search(r"(长度|字长|字符数)\s*[:=：]\s*\d+\s*(位|字符|字)?(?!以内|以下|不超过)", rr):
                    it["len_equals"] = True

        # 字段级覆盖：这些名称按“≤最大长度”处理
        MAX_ONLY_FIELDS = {"集团注册名称","子企业单位名称","企业简称","所属上级企业名称"}
        if it.get("field") in MAX_ONLY_FIELDS:
            it["len_equals"] = False

        # 特例：发薪时间固定19位
        if it["field"] == "发薪时间":
            if not it.get("max_len"):
                it["max_len"] = 19
            it["len_equals"] = True

        rules[t].append(it)

    return rules

def build_pk_value(row: pd.Series, pk_fields: list) -> str:
    if not pk_fields:
        return ""
    vals = []
    for c in pk_fields:
        vals.append(str(row.get(c, "")).strip())
    return "|".join(vals)

def validate_dataframe(df: pd.DataFrame, table: str, rules: dict, length_mode="max", pk_map=None):
    errors = []
    adf = (df.copy() if df is not None else pd.DataFrame())
    if "__校验错误__" not in adf.columns:
        adf["__校验错误__"] = ""
    if df is None or df.empty:
        return errors, adf

    table_rules = rules.get(table, []) if isinstance(rules, dict) else (rules or [])
    pk_fields = []
    if pk_map and table in pk_map:
        pk_fields = [x.strip() for x in pk_map[table] if x.strip()]

    # 通用校验
    for it in table_rules:
        field = it.get("field")
        if not field or field not in df.columns:
            continue  # 忽略字段名不一致
        req = it.get("required", False)
        max_len = it.get("max_len", None)
        decimals = it.get("decimals", None)
        enum = it.get("enum", None)

        for idx, row in df.iterrows():
            val = row.get(field, None)
            pk = build_pk_value(row, pk_fields)

            # 必填
            if req and (pd.isna(val) or str(val).strip()==""):
                errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": field,
                               "错误类型": "必填缺失", "错误信息": "必填字段为空", "建议修复": "若无请填0/按规则给定默认值", "原始值": ""})
                adf.at[idx, "__校验错误__"] += f"[{field} 必填] "

            # 长度（字段级“等于”覆盖全局策略）
            if max_len is not None and isinstance(val, str):
                use_strict = bool(it.get("len_equals", False)) or (length_mode == "strict")
                if use_strict and len(val) != max_len:
                    errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": field,
                                   "错误类型": "长度不符", "错误信息": f"长度应等于{max_len}", "建议修复": "修正字符长度", "原始值": val})
                    adf.at[idx, "__校验错误__"] += f"[{field} 长度= {max_len}] "
                elif (not use_strict) and len(val) > max_len:
                    errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": field,
                                   "错误类型": "长度超限", "错误信息": f"长度应≤{max_len}", "建议修复": "截断或精简内容", "原始值": val})
                    adf.at[idx, "__校验错误__"] += f"[{field} 长度≤ {max_len}] "

            # 小数位
            if decimals is not None and not pd.isna(val) and str(val).strip()!="":
                try:
                    d = Decimal(str(val))
                    frac = str(d.normalize()).split(".")[1] if "." in str(d.normalize()) else ""
                    if len(frac) > decimals:
                        errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": field,
                                       "错误类型": "小数位超限", "错误信息": f"小数位应≤{decimals}", "建议修复": f"保留{decimals}位小数", "原始值": str(val)})
                        adf.at[idx, "__校验错误__"] += f"[{field} 小数位≤{decimals}] "
                except Exception:
                    pass

            # 枚举
            if enum and not pd.isna(val) and str(val).strip()!="":
                sval = str(val).strip()
                if sval not in enum:
                    errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": field,
                                   "错误类型": "取值非法", "错误信息": f"应在{sorted(list(enum))}内", "建议修复": "从下拉/码值中选择合法项", "原始值": sval})
                    adf.at[idx, "__校验错误__"] += f"[{field} 取值非法] "

    # 特例：上市类型、实发数、人工成本总额
    if table == "中央企业各级次单位信息情况表" and "上市类型" in df.columns:
        for idx, row in df.iterrows():
            v = str(row.get("上市类型", "")).strip()
            if v == "":
                continue
            parts = v.split("|")
            ok = True
            if any(not re.fullmatch(r"[a-i]", p) for p in parts):
                ok = False; reason = "仅允许 a~i 以及分隔符 '|'"
            elif len(parts) != len(set(parts)):
                ok = False; reason = "不允许重复值（如 a|a）"
            elif "i" in parts and len(parts) > 1:
                ok = False; reason = "i 不能与其他值同时出现"
            if not ok:
                pk = build_pk_value(row, pk_fields)
                errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": "上市类型",
                               "错误类型": "取值非法", "错误信息": reason, "建议修复": "参照规则填写如 a|b 或 i", "原始值": v})
                adf.at[idx, "__校验错误__"] += "[上市类型 取值非法] "

    if table == "中央企业职工收入情况表":
        need = ["实发数","总收入","工资总额外的福利费用","应扣合计"]
        present = [c for c in need if c in df.columns]
        if len(present) == 4:
            for idx, row in df.iterrows():
                pk = build_pk_value(row, pk_fields)
                d_actual = to_decimal(row.get("实发数"), 2)
                d_expect = (to_decimal(row.get("总收入"),2) +
                            to_decimal(row.get("工资总额外的福利费用"),2) -
                            to_decimal(row.get("应扣合计"),2)).quantize(Decimal("0.01"))
                if (d_actual - d_expect).copy_abs() > Decimal("0.01"):
                    errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": "实发数",
                                   "错误类型": "公式不匹配", "错误信息": f"应= 总收入+工资总额外的福利费用-应扣合计（期望{d_expect} 实际{d_actual}）",
                                   "建议修复": "检查三项金额及小数位；若无请填0，保留两位小数", "原始值": str(row.get("实发数"))})
                    adf.at[idx, "__校验错误__"] += "[实发数 公式不匹配] "

    if table == "中央企业各级单位人工成本情况表":
        # 基础明细列（常见15项，不含“技术奖酬金”与“非货币性福利”，这两项单独处理以兼容别名/新增）
        base_leaf = [
            "职工工资总额","社会保险费用","住房公积金","住房补贴",
            "企业年金和职业年金","补充医疗保险",
            "福利费用","劳动保护费","工会经费","教育培训经费",
            "辞退福利",
            "股份支付","其他人工成本","劳务派遣费"
        ]
        tech_aliases = [
            "技术奖酬金及业务设计奖",
            "技术奖酬金及业余设计奖",
            "技术奖酬金及业务(设计)奖",
            "技术奖酬金及业务／设计奖",
            "技术奖酬金及业务-设计奖",
            "技术奖酬金及业务、设计奖"
        ]
        top_cols = [
            "职工工资总额","社会保险费用","住房公积金","住房补贴",
            "非货币性福利","股份支付","其他人工成本","劳务派遣费"
        ]
        def pick_leaf_cols(df_):
            cols = [c for c in base_leaf if c in df_.columns]
            # 技术奖酬金… 选中第一个匹配的别名
            for alias in tech_aliases:
                if alias in df_.columns:
                    cols.append(alias)
                    break
            # 非货币性福利（若有则加入明细）
            if "非货币性福利" in df_.columns:
                cols.append("非货币性福利")
            # 去重保持顺序
            seen = set(); uniq = []
            for c in cols:
                if c not in seen:
                    seen.add(c); uniq.append(c)
            return uniq

        if "企业人工成本总额" in df.columns:
            present_leaf = pick_leaf_cols(df)
            use_leaf = len(present_leaf) >= 8
            cols = present_leaf if use_leaf else [c for c in top_cols if c in df.columns]
            for idx, row in df.iterrows():
                pk = build_pk_value(row, pk_fields)
                d_total = to_decimal(row.get("企业人工成本总额"), 2)
                parts_sum = sum((to_decimal(row.get(c),2) for c in cols), Decimal("0.00")).quantize(Decimal("0.01"))
                if use_leaf:
                    if d_total + Decimal("0.01") < parts_sum:
                        errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": "企业人工成本总额",
                                       "错误类型": "合计不足", "错误信息": f"应≥ {cols} 之和（期望≥{parts_sum} 实际{d_total}）",
                                       "建议修复": "补齐分项或核对总额；若无请填0", "原始值": str(row.get("企业人工成本总额"))})
                        adf.at[idx, "__校验错误__"] += "[企业人工成本总额 合计不足] "
                else:
                    if (d_total - parts_sum).copy_abs() > Decimal("0.01"):
                        errors.append({"表名": table, "行号": idx+2, "主键": pk, "字段": "企业人工成本总额",
                                       "错误类型": "合计不匹配", "错误信息": f"应= {cols} 之和（期望{parts_sum} 实际{d_total}）",
                                       "建议修复": "逐项核对；若无请填0，合计保留两位小数", "原始值": str(row.get("企业人工成本总额"))})
                        adf.at[idx, "__校验错误__"] += "[企业人工成本总额 合计不匹配] "

    return errors, adf

def validate_cross_tables(dfs: dict, master_dup_mode="summary"):
    cross_errors = []
    cross_sheets = {}

    df1 = dfs.get("中央企业各级次单位信息情况表", pd.DataFrame())
    df2 = dfs.get("中央企业职工收入情况表", pd.DataFrame())
    df4 = dfs.get("中央企业各级单位人工成本情况表", pd.DataFrame())

    key_code = "统一社会信用代码"
    key_name = "单位名称"

    key_col = key_code if key_code in df1.columns else (key_name if key_name in df1.columns else None)
    set1 = set()
    if key_col:
        set1 = set([str(v).strip() for v in df1[key_col].dropna().astype(str)])

    if not df2.empty:
        if key_code in df2.columns or key_name in df2.columns:
            df2_chk = df2.copy()
            use_col = key_code if key_code in df2.columns else key_name
            df2_chk["__存在于表1__"] = df2_chk[use_col].astype(str).str.strip().isin(set1) if key_col else True
            cross_sheets["表间-职工收入vs单位信息"] = df2_chk[[c for c in df2_chk.columns if c != "__校验错误__"]]
            if key_col:
                for idx, row in df2.iterrows():
                    v = str(row.get(use_col, "")).strip()
                    if v and v not in set1:
                        cross_errors.append({"表名": "中央企业职工收入情况表", "行号": idx+2, "主键": "",
                                             "字段": use_col, "错误类型": "表间未匹配", "错误信息": f"{use_col} 在表1不存在",
                                             "建议修复": "先在表1维护主数据，再填表2", "原始值": v})

    if not df4.empty:
        if key_code in df4.columns or key_name in df4.columns:
            df4_chk = df4.copy()
            use_col = key_code if key_code in df4.columns else key_name
            df4_chk["__存在于表1__"] = df4_chk[use_col].astype(str).str.strip().isin(set1) if key_col else True
            cross_sheets["表间-人工成本vs单位信息"] = df4_chk[[c for c in df4_chk.columns if c != "__校验错误__"]]
            if key_col:
                for idx, row in df4.iterrows():
                    v = str(row.get(use_col, "")).strip()
                    if v and v not in set1:
                        cross_errors.append({"表名": "中央企业各级单位人工成本情况表", "行号": idx+2, "主键": "",
                                             "字段": use_col, "错误类型": "表间未匹配", "错误信息": f"{use_col} 在表1不存在",
                                             "建议修复": "先在表1维护主数据，再填表4", "原始值": v})

    dup_sheet = None
    if not df1.empty and (("统一社会信用代码" in df1.columns) or ("单位名称" in df1.columns)):
        cols = [c for c in ["统一社会信用代码","单位名称"] if c in df1.columns]
        if cols:
            dup = df1[df1.duplicated(subset=cols, keep=False)].sort_values(by=cols)
            if not dup.empty:
                dup_sheet = dup
                if master_dup_mode in ("inline",):
                    for idx, row in dup.iterrows():
                        cross_errors.append({"表名": "中央企业各级次单位信息情况表", "行号": idx+2, "主键": "",
                                             "字段": ",".join(cols), "错误类型": "主数据重复",
                                             "错误信息": f"主数据重复：{cols}", "建议修复": "去重后再进行表间匹配", "原始值": ""})
    if dup_sheet is not None and master_dup_mode in ("summary","inline"):
        cross_sheets["主数据-重复检查"] = dup_sheet

    return cross_sheets, cross_errors

def parse_pk_map(pk_arg: str):
    if not pk_arg:
        return {}
    pk_map = {}
    parts = re.split(r"[;；]\s*", pk_arg.strip())
    for p in parts:
        if not p: continue
        if ":" not in p: continue
        t, cols = p.split(":", 1)
        t = t.strip()
        cols = [c.strip() for c in re.split(r"[,，]\s*", cols) if c.strip()]
        if t and cols:
            pk_map[t] = cols
    return pk_map

def unique_sheet_name(base: str, used: set) -> str:
    name = base[:31]
    if name not in used:
        used.add(name); return name
    i = 2
    while True:
        suffix = f"~{i}"
        name2 = (base[:31-len(suffix)] + suffix)
        if name2 not in used:
            used.add(name2); return name2
        i += 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="待校验数据：Excel文件(多Sheet) 或 目录（每表一个Excel）")
    ap.add_argument("--rules-xlsx", required=True, help="V2.2 规则Excel")
    ap.add_argument("--sheet", default="央企端-表内校验", help="规则所在Sheet名，默认：央企端-表内校验")
    ap.add_argument("--codes-sheet", default="码值表", help="码值Sheet名（可空）")
    ap.add_argument("--rules-json", help="（可选）已编译规则JSON")
    ap.add_argument("--dump-rules-json", help="（可选）把编译好的规则导出到JSON文件")
    ap.add_argument("--tables", nargs="*", help="只校验指定表（留空=按规则里所有表）")
    ap.add_argument("--assume-single-sheet", action="store_true", help="兼容模式：目录中Excel只读第一个Sheet")
    ap.add_argument("--length-mode", choices=["max","strict"], default="max", help="长度策略：max=≤上限，strict=等于上限")
    ap.add_argument("--pk", help='主键映射，如："中央企业职工收入情况表:统一社会信用代码,证件号码,姓名"；可用分号拼多表')
    ap.add_argument("--no-annotated", dest="no_annotated", action="store_true", help="不输出“标注-表名”Sheet")
    ap.add_argument("--master-dup-report", choices=["inline","summary","off"], default="summary", help="主数据重复提示输出位置")
    ap.add_argument("--output", required=True, help="输出Excel路径")
    args = ap.parse_args()

    data_path = Path(args.data)
    rules_path = Path(args.rules_xlsx)
    out_path = Path(args.output)

    dfs = read_data_any(data_path)

    compiled = compile_rules_from_excel(rules_path, sheet_name=args.sheet, codes_sheet=args.codes_sheet)
    if args.rules_json and Path(args.rules_json).exists():
        try:
            compiled = json.loads(Path(args.rules_json).read_text("utf-8"))
        except Exception:
            pass
    if args.dump_rules_json:
        try:
            Path(args.dump_rules_json).write_text(json.dumps(compiled, ensure_ascii=False, indent=2), "utf-8")
        except Exception:
            pass

    tables = args.tables if args.tables else EXPECTED_TABLES
    tables = [t for t in tables if t in EXPECTED_TABLES]

    pk_map = parse_pk_map(args.pk or "")

    all_errors = []
    annotated = {}
    for t in tables:
        df = dfs.get(t, pd.DataFrame())
        errs, adf = validate_dataframe(df, t, compiled, length_mode=args.length_mode, pk_map=pk_map)
        all_errors.extend(errs)
        annotated[t] = adf

    cross_sheets, cross_errors = validate_cross_tables(dfs, master_dup_mode=args.master_dup_report)
    all_errors.extend(cross_errors)

    engine = "xlsxwriter"
    try:
        import xlsxwriter  # noqa: F401
    except Exception:
        engine = "openpyxl"

    with pd.ExcelWriter(out_path, engine=engine) as xw:
        used_names = set()

        for t in tables:
            adf = annotated.get(t, pd.DataFrame())
            err_rows = [e for e in all_errors if e["表名"] == t]
            err_df = pd.DataFrame(err_rows) if err_rows else pd.DataFrame(columns=["表名","行号","主键","字段","错误类型","错误信息","建议修复","原始值"])
            err_df.to_excel(xw, sheet_name=unique_sheet_name(f"错误-{t}", used_names), index=False)
            if not args.no_annotated:
                adf.to_excel(xw, sheet_name=unique_sheet_name(f"标注-{t}", used_names), index=False)

        for name, sdf in cross_sheets.items():
            sdf.to_excel(xw, sheet_name=unique_sheet_name(name, used_names), index=False)

        over = []
        for t in tables:
            cnt = sum(1 for e in all_errors if e["表名"] == t)
            over.append({"表名": t, "错误数量": cnt, "记录数": len(annotated.get(t, pd.DataFrame()))})
        pd.DataFrame(over).to_excel(xw, sheet_name=unique_sheet_name("总览", used_names), index=False)

    print(f"完成：输出 {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
