#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_streams.py — 多路数据流合并工具（纯 Python 标准库，单文件）

功能
----
输入多路数据流（每流：名称、优先级、记录序列，每记录：键、值），
按到达顺序合并，输出合并后的记录集、引用解析结果、错误报告与可追溯的合并历史。

规则（自定，理由见下）
----------------------
1. 合并顺序：按流在输入中出现的到达顺序依次合并。
   理由：确定性最强、结果可复现，且符合"先到的数据先落地"的直觉。
2. 同键冲突（多流同键不同值）：优先级数值大者胜；优先级相同则先到者胜
   （first-writer-wins）。理由：规则简单、与输入顺序唯一对应，可追溯。
3. 同流同键重复（去重）：
   - 同键同值：丢弃重复记录，报告 DEDUP 警告；
   - 同键不同值：后者覆盖前者（视为流内修正），报告 OVERWRITE 警告。
4. 引用：值中 @键名 表示引用其他记录的键；合并完成后统一解析为裁决后的
   最终值（同步更新）。引用不存在的键报告 DANGLING_REF 错误。
5. 流引用不存在（MERGE 引用了未定义的流）报告 UNKNOWN_STREAM 错误。
6. 记录格式非法（缺字段、优先级非整数、空键、空值、未知指令等）报告
   BAD_FORMAT 错误，该记录/流被跳过，不影响其余数据。
7. 跨记录状态延续：合并历史（每次写入/覆盖/丢弃）全部记录，可追溯。

输入格式（行文本，# 开头为注释）
--------------------------------
    STREAM <流名> <优先级整数>
    REC <键> <值>            # 值可含空格；值中 @键名 为引用
    REC ...
    END                      # 结束当前流
    MERGE <流名> [流名 ...]  # 可选；缺省则按全部流的出现顺序合并

用法
----
    python3 merge_streams.py 输入文件          # 从文件读
    cat 输入文件 | python3 merge_streams.py    # 从标准输入读
    python3 merge_streams.py --demo            # 运行内置示例

输出
----
    == MERGED RECORDS ==   合并后的记录集（键 -> 最终值，含来源与优先级）
    == RESOLVED REFERENCES ==  引用解析（@键 同步更新为最终值）
    == MERGE HISTORY ==    可追溯的合并历史
    == ERRORS ==           错误与警告清单
"""

import re
import sys
import shlex
from dataclasses import dataclass, field

REF_PATTERN = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


@dataclass
class Record:
    key: str
    value: str
    stream: str
    priority: int
    seq: int  # 全局到达序号


@dataclass
class Stream:
    name: str
    priority: int
    records: list = field(default_factory=list)  # list[Record]
    arrival: int = 0  # 到达顺序号


@dataclass
class Winner:
    record: Record
    reason: str


class Merger:
    def __init__(self):
        self.streams = {}          # name -> Stream
        self.arrival_order = []    # 流名，按出现顺序
        self.merge_list = None     # MERGE 指令指定的流名列表
        self.errors = []           # (级别, 类别, 位置, 描述)
        self.history = []          # 合并历史（可追溯）
        self.winners = {}          # key -> Winner
        self._seq = 0

    # ---------- 错误与历史 ----------
    def error(self, level, category, where, msg):
        self.errors.append((level, category, where, msg))

    def log(self, msg):
        self.history.append(msg)

    # ---------- 解析输入 ----------
    def parse(self, text):
        current = None
        for lineno, raw in enumerate(text.splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                parts = shlex.split(line, comments=True)
            except ValueError as exc:
                self.error("ERROR", "BAD_FORMAT", f"line {lineno}",
                           f"行解析失败: {exc}: {raw!r}")
                continue
            if not parts:
                continue
            cmd = parts[0].upper()
            where = f"line {lineno}"

            if cmd == "STREAM":
                if current is not None:
                    self.error("ERROR", "BAD_FORMAT", where,
                               f"流 {current.name!r} 未以 END 结束")
                if len(parts) != 3:
                    self.error("ERROR", "BAD_FORMAT", where,
                               f"STREAM 需要 名称 优先级 两个参数: {raw!r}")
                    current = None
                    continue
                name, prio_s = parts[1], parts[2]
                if not name:
                    self.error("ERROR", "BAD_FORMAT", where, "流名为空")
                    current = None
                    continue
                try:
                    prio = int(prio_s)
                except ValueError:
                    self.error("ERROR", "BAD_FORMAT", where,
                               f"优先级非整数: {prio_s!r}")
                    current = None
                    continue
                if name in self.streams:
                    self.error("WARN", "DUP_STREAM", where,
                               f"流 {name!r} 重复定义，后者覆盖前者")
                current = Stream(name=name, priority=prio,
                                 arrival=len(self.arrival_order))
                self.streams[name] = current
                self.arrival_order.append(name)

            elif cmd == "REC":
                if current is None:
                    self.error("ERROR", "BAD_FORMAT", where,
                               "REC 出现在任何 STREAM 之外，已跳过")
                    continue
                if len(parts) < 3:
                    self.error("ERROR", "BAD_FORMAT", where,
                               f"REC 需要 键 值 两个参数: {raw!r}")
                    continue
                key = parts[1]
                value = " ".join(parts[2:])
                if not key:
                    self.error("ERROR", "BAD_FORMAT", where, "记录键为空")
                    continue
                if value == "":
                    self.error("ERROR", "BAD_FORMAT", where,
                               f"记录 {key!r} 的值为空")
                    continue
                self._seq += 1
                current.records.append(Record(key=key, value=value,
                                              stream=current.name,
                                              priority=current.priority,
                                              seq=self._seq))
            elif cmd == "END":
                if current is None:
                    self.error("WARN", "BAD_FORMAT", where,
                               "END 没有匹配的 STREAM")
                current = None
            elif cmd == "MERGE":
                if len(parts) < 2:
                    self.error("ERROR", "BAD_FORMAT", where,
                               "MERGE 需要至少一个流名")
                else:
                    self.merge_list = parts[1:]
            else:
                self.error("ERROR", "BAD_FORMAT", where,
                           f"未知指令 {parts[0]!r}，已跳过")
        if current is not None:
            self.error("ERROR", "BAD_FORMAT", "EOF",
                       f"流 {current.name!r} 未以 END 结束")

    # ---------- 合并 ----------
    def merge(self):
        order = self.merge_list if self.merge_list else self.arrival_order
        for name in order:
            if name not in self.streams:
                self.error("ERROR", "UNKNOWN_STREAM", "MERGE",
                           f"引用了不存在的流 {name!r}，已跳过")
                continue
            stream = self.streams[name]
            self.log(f"[流 {stream.name}] 到达顺序#{stream.arrival} "
                     f"优先级={stream.priority} 记录数={len(stream.records)}")
            seen = {}  # 流内去重：key -> Record
            for rec in stream.records:
                # 流内去重
                if rec.key in seen:
                    prev = seen[rec.key]
                    if prev.value == rec.value:
                        self.error("WARN", "DEDUP", f"流 {name} 记录#{rec.seq}",
                                   f"同流同键同值重复 {rec.key!r}={rec.value!r}，"
                                   f"丢弃记录#{rec.seq}（保留记录#{prev.seq}）")
                        self.log(f"  丢弃重复 {rec.key!r} (记录#{rec.seq})")
                        continue
                    self.error("WARN", "OVERWRITE", f"流 {name} 记录#{rec.seq}",
                               f"同流同键不同值 {rec.key!r}: "
                               f"{prev.value!r} -> {rec.value!r}，后者覆盖前者")
                    self.log(f"  流内覆盖 {rec.key!r}: {prev.value!r} -> "
                             f"{rec.value!r} (记录#{rec.seq})")
                seen[rec.key] = rec
                # 跨流冲突裁决
                cur = self.winners.get(rec.key)
                if cur is None:
                    self.winners[rec.key] = Winner(rec, "首次写入")
                    self.log(f"  写入 {rec.key!r}={rec.value!r} "
                             f"(流 {name}, 记录#{rec.seq})")
                    continue
                old = cur.record
                if old.stream == rec.stream:
                    # 同流修正：直接替换，不参与跨流裁决
                    self.winners[rec.key] = Winner(rec, "流内覆盖")
                    self.log(f"  应用流内修正 {rec.key!r}={rec.value!r} "
                             f"(流 {name}, 记录#{rec.seq})")
                    continue
                if old.value == rec.value:
                    self.log(f"  一致 {rec.key!r}：流 {name} 与流 "
                             f"{old.stream} 值相同，无需裁决")
                    continue
                if (rec.priority, -stream.arrival) > \
                   (old.priority, -self.streams[old.stream].arrival):
                    self.winners[rec.key] = Winner(
                        rec, f"优先级 {rec.priority} > {old.priority}"
                        if rec.priority != old.priority else "同优先级，先到者胜")
                    self.error("WARN", "CONFLICT", f"键 {rec.key!r}",
                               f"冲突：流 {old.stream}(优先级{old.priority})="
                               f"{old.value!r} vs 流 {rec.stream}"
                               f"(优先级{rec.priority})={rec.value!r}，"
                               f"裁决采用 {rec.value!r}")
                    self.log(f"  裁决 {rec.key!r}: {old.value!r}(流 {old.stream})"
                             f" -> {rec.value!r}(流 {rec.stream})")
                else:
                    self.error("WARN", "CONFLICT", f"键 {rec.key!r}",
                               f"冲突：流 {old.stream}(优先级{old.priority})="
                               f"{old.value!r} vs 流 {rec.stream}"
                               f"(优先级{rec.priority})={rec.value!r}，"
                               f"裁决保留 {old.value!r}")
                    self.log(f"  保留 {rec.key!r}={old.value!r} "
                             f"(流 {old.stream})，丢弃流 {rec.stream} 的值 "
                             f"{rec.value!r}")

    # ---------- 引用解析（同步更新） ----------
    def resolve_references(self):
        self.log("[引用解析阶段] 合并完成后统一同步更新")
        resolved = {}
        for key, win in self.winners.items():
            rec = win.record
            refs = REF_PATTERN.findall(rec.value)
            if not refs:
                continue
            new_value = rec.value
            for ref in refs:
                if ref == key:
                    self.error("ERROR", "DANGLING_REF", f"键 {key!r}",
                               f"记录自引用 @{ref}，无法解析")
                    continue
                target = self.winners.get(ref)
                if target is None:
                    self.error("ERROR", "DANGLING_REF", f"键 {key!r}",
                               f"引用了不存在的键 @{ref}，保留原样")
                    continue
                new_value = new_value.replace("@" + ref, target.record.value)
                self.log(f"  引用更新 {key!r}: @{ref} -> "
                         f"{target.record.value!r}")
            resolved[key] = new_value
        return resolved

    # ---------- 输出 ----------
    def report(self, out=sys.stdout):
        resolved = self.resolve_references()
        print("== MERGED RECORDS ==", file=out)
        for key in sorted(self.winners):
            win = self.winners[key]
            rec = win.record
            final = resolved.get(key, rec.value)
            suffix = f"  [引用解析后: {final!r}]" if key in resolved else ""
            print(f"  {key} = {rec.value}  (来源流: {rec.stream}, "
                  f"优先级: {rec.priority}, 依据: {win.reason}){suffix}",
                  file=out)
        if resolved:
            print("\n== RESOLVED REFERENCES ==", file=out)
            for key in sorted(resolved):
                print(f"  {key} -> {resolved[key]}", file=out)
        print("\n== MERGE HISTORY ==", file=out)
        for entry in self.history:
            print(entry, file=out)
        print("\n== ERRORS ==", file=out)
        if not self.errors:
            print("  (无)", file=out)
        for level, cat, where, msg in self.errors:
            print(f"  [{level}][{cat}] {where}: {msg}", file=out)
        n_err = sum(1 for e in self.errors if e[0] == "ERROR")
        n_warn = len(self.errors) - n_err
        print(f"\n合计: {len(self.winners)} 条记录, "
              f"{n_err} 个错误, {n_warn} 个警告", file=out)


DEMO_INPUT = """\
# 多路数据流合并示例
STREAM sensor_a 10
REC temperature 22.5
REC humidity 60
REC humidity 60            # 同流同键同值 -> 去重报告
REC humidity 61            # 同流同键不同值 -> 覆盖并报告
REC status "正常, 参考 @temperature"
END

STREAM sensor_b 5
REC temperature 23.1       # 与 sensor_a 冲突，优先级低 -> 被裁决
REC pressure 101.3
REC note "温度见 @temperature, 湿度见 @humidity, 缺失见 @missing"
END

STREAM sensor_c 7
REC altitude 120
REC broken_rec             # 格式非法：缺值
END

MERGE sensor_a sensor_b sensor_c ghost_stream
"""


def main(argv):
    if "--demo" in argv:
        text = DEMO_INPUT
        print("== INPUT (内置示例) ==")
        print(text)
    else:
        if len(argv) > 1:
            with open(argv[1], encoding="utf-8") as fh:
                text = fh.read()
        else:
            text = sys.stdin.read()
    merger = Merger()
    merger.parse(text)
    merger.merge()
    merger.report()
    return 1 if any(e[0] == "ERROR" for e in merger.errors) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
