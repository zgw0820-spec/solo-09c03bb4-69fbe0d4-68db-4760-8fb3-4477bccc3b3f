#!/usr/bin/env python3
"""端到端自测：通过 HTTP API 投送样包，校验 UDP 出口实收序号与放行时序。

用法：python3 scripts/e2e_test.py [base_url]   默认 http://127.0.0.1:8000
覆盖：补包（任一路补齐缺号）、重复去重、65535 回绕、50ms 超时放行、
      128 缓存上限放行、缺号跳过后迟到包丢弃。
仅依赖 Python 标准库。
"""
import json
import sys
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
FAILED = []


def call(method, path, body=None):
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def inject(line, seq, count=1, delay_ms=0, interval_ms=0):
    return call("POST", "/inject", {
        "line": line, "seq": seq, "count": count,
        "delay_ms": delay_ms, "interval_ms": interval_ms,
    })


def received():
    return call("GET", "/received?limit=-1")


def stats():
    return call("GET", "/stats")


def reset():
    return call("POST", "/reset")


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"{mark} {name}" + (f"  [{detail}]" if detail else ""))
    if not cond:
        FAILED.append(name)


def t_of(rec, seq):
    for it in rec["items"]:
        if it["seq"] == seq:
            return it["t_ms"]
    return None


def main():
    # 0. Swagger / OpenAPI 可用
    with urllib.request.urlopen(BASE + "/docs", timeout=10) as r:
        check("Swagger UI 可访问", r.status == 200)
    spec = call("GET", "/openapi.json")
    check("OpenAPI 含 /inject 与 /received",
          "/inject" in spec["paths"] and "/received" in spec["paths"])

    # A. 补包：缺号由另一路补齐，出口按序
    reset()
    inject(1, 1000, count=2)            # 1000,1001
    inject(1, 1003, count=2)            # 1003,1004（缺 1002）
    time.sleep(0.03)
    inject(2, 1002)                     # 线路 2 补齐 1002
    time.sleep(0.2)
    rec = received()
    check("A 补包后按序输出", rec["seqs"] == [1000, 1001, 1002, 1003, 1004],
          f"实际 {rec['seqs']}")

    # B. 去重：同一原包两路都送，只发一次
    reset()
    inject(1, 1010)
    inject(2, 1010)
    time.sleep(0.12)
    rec = received()
    st = stats()["merger"]
    check("B 同一原包只发一次", rec["seqs"] == [1010], f"实际 {rec['seqs']}")
    check("B 重复包被丢弃计数", st["dup_dropped"] + st["late_dropped"] >= 1,
          f"dup={st['dup_dropped']} late={st['late_dropped']}")

    # C. 回绕：65534..1 按序；回绕前后重复包均不再发
    reset()
    inject(1, 65534, count=4)           # 65534,65535,0,1
    time.sleep(0.12)
    rec = received()
    check("C 跨 65535 回绕按序输出", rec["seqs"] == [65534, 65535, 0, 1],
          f"实际 {rec['seqs']}")
    inject(1, 65535)
    inject(2, 0)
    time.sleep(0.08)
    rec2 = received()
    st = stats()["merger"]
    check("C 回绕后重复包不再发", rec2["seqs"] == [65534, 65535, 0, 1]
          and st["late_dropped"] >= 2,
          f"实际 {rec2['seqs']} late={st['late_dropped']}")

    # D. 50ms 超时放行：缺 2001，2002 约 50ms 后放出；迟到的 2001 被丢弃
    reset()
    inject(1, 2000)
    inject(1, 2002)
    time.sleep(0.25)
    rec = received()
    st = stats()["merger"]
    dt = (t_of(rec, 2002) or 0) - (t_of(rec, 2000) or 0)
    check("D 超时后跳过缺号输出", rec["seqs"] == [2000, 2002],
          f"实际 {rec['seqs']}")
    check("D 放行延时约 50ms", 40 <= dt <= 150, f"实测 {dt}ms")
    check("D 计为超时跳转而非缓存跳转",
          st["timeout_jumps"] >= 1 and st["buffer_jumps"] == 0,
          f"timeout_jumps={st['timeout_jumps']} buffer_jumps={st['buffer_jumps']}")
    inject(1, 2001)                     # 缺号被跳过后才到
    time.sleep(0.08)
    rec2 = received()
    st2 = stats()["merger"]
    check("D 迟到包直接丢弃", rec2["seqs"] == [2000, 2002]
          and st2["late_dropped"] >= 1,
          f"实际 {rec2['seqs']} late={st2['late_dropped']}")

    # E. 128 缓存上限：缺 3001，待发包到 128 个即跳到最靠前的包
    reset()
    inject(1, 3000)
    inject(1, 3002, count=130)          # 3002..3131，第 128 个入缓存即触发
    time.sleep(0.25)
    rec = received()
    st = stats()["merger"]
    expect = [3000] + list(range(3002, 3132))
    dt = (t_of(rec, 3002) or 0) - (t_of(rec, 3000) or 0)
    check("E 缓存满 128 跳号后全部按序输出", rec["seqs"] == expect,
          f"实收 {len(rec['seqs'])} 个，期望 {len(expect)} 个")
    check("E 由缓存上限放行（远早于 50ms）", 0 <= dt < 40, f"实测 {dt}ms")
    check("E 计为缓存跳转而非超时跳转",
          st["buffer_jumps"] >= 1 and st["timeout_jumps"] == 0,
          f"buffer_jumps={st['buffer_jumps']} timeout_jumps={st['timeout_jumps']}")

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项失败: {FAILED}")
        return 1
    print("全部端到端用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
