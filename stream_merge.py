#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stream_merge.py — 多路数据流合并工具（纯 Python 标准库，单文件）

功能
====
输入多路数据流（每流：名称、优先级、记录序列，每记录为 [键, 值]），
输出合并后的记录集、错误报告与可追溯的合并历史。

规则（自定，理由如下）
=====================
1. 合并顺序：默认按输入数组顺序（即“到达顺序”）逐流、逐记录合并；
   可用顶层 "merge_order" 显式指定到达顺序。
   理由：与真实数据流的到达语义一致，且完全确定、可重放。
2. 同键冲突（不同流、同键、不同值）：优先级数值大者胜；
   优先级相同则后到者胜（Last-Writer-Wins）。
   理由：优先级表达数据源可信度；平级时后到的数据通常更新。
3. 同流同键重复（去重）：保留该流内最后一条（跨记录状态延续，
   后到的记录视为对先前状态的覆盖），被丢弃的记录记入错误报告（warning 级）。
4. 流引用不存在：merge_order 中引用了不存在的流名 → 报告 error；
   流名重复 → 报告 error。
5. 记录格式非法：流（名称/优先级/记录序列非法）、键（非空字符串以外）、
   值（非 JSON 标量）均报告 error，非法记录跳过、不影响其余记录。
6. 引用同步更新：值为 "@键" 的字符串表示引用其他键。全部合并完成后
   统一解析：被引用键的最终合并值会同步反映到引用者（resolved_value）；
   引用不存在的键或出现循环引用 → 报告 error。
7. 可追溯：每次写入/冲突裁决/去重丢弃都追加到 history，
   每条最终记录携带来源（流、优先级、流内序号、版本号）。

输入格式（JSON）
================
{
  "merge_order": ["sensorA", "sensorB"],          // 可选；缺省按 streams 数组顺序
  "streams": [
    {"name": "sensorA", "priority": 10,
     "records": [["k1", "v1"], ["k2", "@k1"]]},   // 值为 "@键" 表示引用
    {"name": "sensorB", "priority": 20,
     "records": [["k1", "v1-new"], ["k3", 42]]}
  ]
}

输出格式（JSON）
================
{
  "merged":  {键: {value, resolved_value, source, priority, record_index, version}},
  "errors":  [{level, kind, stream, key, message}],
  "history": [{seq, event, detail}]               // 合并历史，可追溯
}

用法
====
  python3 stream_merge.py 输入.json [-o 输出.json]
  python3 stream_merge.py -            # 从标准输入读
  python3 stream_merge.py --demo       # 运行内置示例
"""

import json
import sys


# ---------------------------------------------------------------- 错误收集

class Report:
    """错误/警告与合并历史的收集器。"""

    def __init__(self):
        self.errors = []
        self.history = []
        self._seq = 0

    def error(self, kind, message, stream=None, key=None, level="error"):
        entry = {"level": level, "kind": kind, "message": message}
        if stream is not None:
            entry["stream"] = stream
        if key is not None:
            entry["key"] = key
        self.errors.append(entry)

    def log(self, event, detail):
        self._seq += 1
        self.history.append({"seq": self._seq, "event": event, "detail": detail})


# ---------------------------------------------------------------- 输入校验

SCALAR_TYPES = (str, int, float, bool, type(None))


def validate_streams(data, report):
    """校验顶层结构，返回 (streams, merge_order)。非法流被剔除并报告。"""
    if not isinstance(data, dict) or not isinstance(data.get("streams"), list):
        report.error("invalid_input", "顶层必须是含 \"streams\" 数组的对象")
        return [], None

    streams = []
    seen_names = set()
    for i, raw in enumerate(data["streams"]):
        label = "streams[%d]" % i
        if not isinstance(raw, dict):
            report.error("invalid_stream", "流定义必须是对象", stream=label)
            continue
        name = raw.get("name")
        priority = raw.get("priority")
        records = raw.get("records")
        ok = True
        if not isinstance(name, str) or not name:
            report.error("invalid_stream", "流名称必须是非空字符串", stream=label)
            ok = False
        elif name in seen_names:
            report.error("duplicate_stream", "流名称重复: %r" % name, stream=name)
            ok = False
        if not isinstance(priority, (int, float)) or isinstance(priority, bool):
            report.error("invalid_stream", "优先级必须是数值", stream=name or label)
            ok = False
        if not isinstance(records, list):
            report.error("invalid_stream", "records 必须是数组", stream=name or label)
            ok = False
        if ok:
            seen_names.add(name)
            streams.append({"name": name, "priority": priority, "records": records})

    merge_order = data.get("merge_order")
    if merge_order is not None:
        if not isinstance(merge_order, list) or not all(
            isinstance(n, str) for n in merge_order
        ):
            report.error("invalid_merge_order", "merge_order 必须是字符串数组")
            merge_order = None
        else:
            known = {s["name"] for s in streams}
            valid_order = []
            for n in merge_order:
                if n not in known:
                    report.error("unknown_stream",
                                 "merge_order 引用了不存在的流: %r" % n, stream=n)
                elif n in valid_order:
                    report.error("duplicate_stream",
                                 "merge_order 中流名重复: %r" % n, stream=n)
                else:
                    valid_order.append(n)
            # merge_order 未列出的流按输入顺序追加到末尾（不静默丢弃数据）
            valid_order.extend(s["name"] for s in streams
                               if s["name"] not in valid_order)
            merge_order = valid_order

    return streams, merge_order


def validate_record(rec, report, stream_name, index):
    """校验单条记录 [键, 值]，合法返回 (key, value)，否则返回 None。"""
    where = "记录#%d" % index
    if not isinstance(rec, (list, tuple)) or len(rec) != 2:
        report.error("invalid_record",
                     "%s 必须是 [键, 值] 二元组" % where, stream=stream_name)
        return None
    key, value = rec
    if not isinstance(key, str) or not key:
        report.error("invalid_key",
                     "%s 的键必须是非空字符串, 得到 %r" % (where, key),
                     stream=stream_name)
        return None
    if not isinstance(value, SCALAR_TYPES):
        report.error("invalid_value",
                     "%s 的值必须是 JSON 标量(字符串/数值/布尔/null), 得到 %r"
                     % (where, value), stream=stream_name, key=key)
        return None
    return key, value


# ---------------------------------------------------------------- 合并

def merge(streams, merge_order, report):
    """按到达顺序逐流合并，返回 {键: 记录状态}。"""
    by_name = {s["name"]: s for s in streams}
    order = merge_order if merge_order is not None else [s["name"] for s in streams]
    merged = {}   # key -> {value, source, priority, record_index, version}

    for stream_name in order:
        stream = by_name[stream_name]
        prio = stream["priority"]
        report.log("stream_start", "开始合并流 %r (优先级 %s)" % (stream_name, prio))

        seen_in_stream = {}  # 流内去重：key -> 已保留的 record_index
        for idx, raw_rec in enumerate(stream["records"]):
            parsed = validate_record(raw_rec, report, stream_name, idx)
            if parsed is None:
                continue
            key, value = parsed

            # --- 同流同键去重：保留最后一条（状态延续），丢弃的记报告 ---
            if key in seen_in_stream:
                prev_idx = seen_in_stream[key]
                prev_val = stream["records"][prev_idx][1]
                report.error(
                    "duplicate_key",
                    "流内重复键 %r: 丢弃记录#%d (值 %r)，保留记录#%d (值 %r)"
                    % (key, prev_idx, prev_val, idx, value),
                    stream=stream_name, key=key, level="warning")
                report.log("dedup", "流 %r 键 %r: 记录#%d 被记录#%d 覆盖"
                           % (stream_name, key, prev_idx, idx))
                # 若被丢弃的记录已写入全局，需要修正全局状态（见下方冲突逻辑，
                # 此处直接以新值继续走合并流程即可，因为同源同键后写覆盖先写）
            seen_in_stream[key] = idx

            # --- 全局合并 ---
            if key not in merged:
                merged[key] = {"value": value, "source": stream_name,
                               "priority": prio, "record_index": idx, "version": 1}
                report.log("insert", "键 %r 由流 %r 写入, 值 %r"
                           % (key, stream_name, value))
                continue

            cur = merged[key]
            if cur["source"] == stream_name:
                # 同流覆盖（去重后的状态延续），不构成跨流冲突
                cur["value"] = value
                cur["record_index"] = idx
                cur["version"] += 1
                report.log("update", "键 %r 被同流 %r 更新为 %r (版本 %d)"
                           % (key, stream_name, value, cur["version"]))
                continue

            if cur["value"] == value:
                report.log("redundant", "键 %r: 流 %r 的值与现有值相同, 保留来源 %r"
                           % (key, stream_name, cur["source"]))
                continue

            # --- 跨流同键不同值：冲突，按优先级裁决，平级则后到者胜 ---
            winner, loser = cur, {"value": value, "source": stream_name,
                                  "priority": prio, "record_index": idx}
            if (prio, 1) > (cur["priority"], 0):  # 平级时后到者胜
                winner, loser = loser, winner
            report.error(
                "conflict",
                "键 %r 冲突: 流 %r(优先级 %s, 值 %r) vs 流 %r(优先级 %s, 值 %r) "
                "-> 裁决 %r 胜出" % (key, loser["source"], loser["priority"],
                                     loser["value"], winner["source"],
                                     winner["priority"], winner["value"],
                                     winner["source"]),
                stream=stream_name, key=key, level="warning")
            report.log("conflict", "键 %r: %r 胜 %r (优先级 %s vs %s)"
                       % (key, winner["source"], loser["source"],
                          winner["priority"], loser["priority"]))
            if winner["source"] == stream_name:
                merged[key] = {"value": value, "source": stream_name,
                               "priority": prio, "record_index": idx,
                               "version": cur["version"] + 1}

    return merged


# ---------------------------------------------------------------- 引用解析

def resolve_references(merged, report):
    """解析 "@键" 引用，写入 resolved_value；缺失/循环引用报告错误。"""
    def resolve(key, chain):
        if key in chain:  # 循环引用
            report.error("reference_cycle",
                         "循环引用: %s" % " -> ".join(list(chain) + [key]),
                         key=key)
            return None
        if key not in merged:
            report.error("unresolved_reference",
                         "引用链 %s 指向不存在的键 %r"
                         % (" -> ".join(chain) or "(根)", key), key=key)
            return None
        value = merged[key]["value"]
        if isinstance(value, str) and value.startswith("@"):
            return resolve(value[1:], chain + (key,))
        return value

    for key, rec in merged.items():
        value = rec["value"]
        if isinstance(value, str) and value.startswith("@"):
            target = value[1:]
            resolved = resolve(target, (key,))
            rec["resolved_value"] = resolved
            if resolved is not None:
                report.log("reference", "键 %r 的引用 %r 解析为 %r"
                           % (key, value, resolved))
        else:
            rec["resolved_value"] = value


# ---------------------------------------------------------------- 主流程

def run(data):
    report = Report()
    streams, merge_order = validate_streams(data, report)
    merged = merge(streams, merge_order, report)
    resolve_references(merged, report)
    return {
        "merged": {k: merged[k] for k in sorted(merged)},
        "errors": report.errors,
        "history": report.history,
    }


DEMO_INPUT = {
    "merge_order": ["sensorA", "sensorB", "ghost", "sensorC"],
    "streams": [
        {"name": "sensorA", "priority": 10, "records": [
            ["temp", 36.5],
            ["humidity", 60],
            ["temp", 37.0],                # 同流重复键 -> 去重报告，保留后者
            ["alias_temp", "@temp"],       # 引用：合并后同步解析
            ["bad"],                       # 非法记录：不是二元组
            ["", "x"],                     # 非法键
            ["lst", [1, 2]],               # 非法值
        ]},
        {"name": "sensorB", "priority": 20, "records": [
            ["temp", 38.1],                # 与 sensorA 冲突，优先级高者胜
            ["humidity", 60],              # 同值，冗余不冲突
            ["note", "from-B"],
        ]},
        {"name": "sensorC", "priority": 20, "records": [
            ["note", "from-C"],            # 平级冲突，后到者胜
            ["dangling", "@no_such_key"],  # 引用不存在的键
            ["loop_a", "@loop_b"],
            ["loop_b", "@loop_a"],         # 循环引用
        ]},
    ],
}


def main(argv):
    demo = "--demo" in argv
    args = [a for a in argv[1:] if a not in ("-", "--demo")]
    out_path = None
    if "-o" in args:
        i = args.index("-o")
        out_path = args[i + 1]
        del args[i:i + 2]

    if demo or (not args and sys.stdin.isatty()):
        print("# 运行内置示例\n", file=sys.stderr)
        data = DEMO_INPUT
    elif args:
        with open(args[0], "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = json.load(sys.stdin)

    result = run(data)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        print("已写入 %s" % out_path, file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
