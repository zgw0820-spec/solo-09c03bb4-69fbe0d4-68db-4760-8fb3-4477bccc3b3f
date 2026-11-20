"""RTP 双线路合流网关（FastAPI）。

- 两个 UDP 端口（线路 1 / 线路 2）接收同一 SSRC 的 RTP 包；
- 合流器按序去重后，把原始报文送到唯一 UDP 出口（解码器地址）；
- 内置出口监视器：监听出口端口，记录实收序号，供 /received 查询；
- Swagger UI（/docs）提供按线路、序号、延时投送样包的 /inject 接口。
"""
from __future__ import annotations

import asyncio
import os
import socket
import struct
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .merger import RtpMerger, seq_of

# ---- 配置（均可用环境变量覆盖） ----
BIND_HOST = os.getenv("BIND_HOST", "0.0.0.0")        # 两个线路端口与监视端口的绑定地址
LINE1_PORT = int(os.getenv("LINE1_PORT", "5004"))    # 线路 1 收包端口 (UDP)
LINE2_PORT = int(os.getenv("LINE2_PORT", "5006"))    # 线路 2 收包端口 (UDP)
OUTPUT_HOST = os.getenv("OUTPUT_HOST", "127.0.0.1")  # 出口：解码器地址
OUTPUT_PORT = int(os.getenv("OUTPUT_PORT", "6000"))  # 出口：解码器端口 (UDP)
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "6000"))  # 内置监视器监听端口 (UDP)
INJECT_TARGET = os.getenv("INJECT_TARGET", "127.0.0.1")  # 样包投送目标（本机回环）
TIMEOUT_MS = float(os.getenv("TIMEOUT_MS", "50"))    # 待发包最长等待（毫秒）
BUFFER_MAX = int(os.getenv("BUFFER_MAX", "128"))     # 待发包缓存上限
DEFAULT_SSRC = 0x11223344                            # 样包默认 SSRC

# ---- UDP 出口（发往解码器） ----
_out_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_out_sock.setblocking(False)
counters = {"send_errors": 0, "injected": 0, "monitor_received": 0, "monitor_malformed": 0}


def _send_out(data: bytes) -> None:
    try:
        _out_sock.sendto(data, (OUTPUT_HOST, OUTPUT_PORT))
    except OSError:
        counters["send_errors"] += 1


merger = RtpMerger(_send_out, timeout_s=TIMEOUT_MS / 1000.0, buffer_max=BUFFER_MAX)

# ---- 出口监视器：记录实收 (序号, 时刻) ----
monitor: deque[tuple[int, float]] = deque(maxlen=100_000)
line_stats = {1: {"received": 0}, 2: {"received": 0}}


class _LineProtocol(asyncio.DatagramProtocol):
    """线路 1 / 线路 2 的收包协议。"""

    def __init__(self, line: int):
        self.line = line

    def datagram_received(self, data: bytes, addr) -> None:
        line_stats[self.line]["received"] += 1
        merger.on_packet(data, time.monotonic())


class _MonitorProtocol(asyncio.DatagramProtocol):
    """出口监视器：谁在出口端口上收到了什么序号。"""

    def datagram_received(self, data: bytes, addr) -> None:
        now = time.monotonic()
        if len(data) < 12:
            counters["monitor_malformed"] += 1
            return
        monitor.append((seq_of(data), now))
        counters["monitor_received"] += 1


async def _tick_loop() -> None:
    """1ms 周期的超时巡检（50ms 放行上限的时钟来源）。"""
    while True:
        merger.tick(time.monotonic())
        await asyncio.sleep(0.001)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_running_loop()
    transports = []
    for line, port in ((1, LINE1_PORT), (2, LINE2_PORT)):
        transport, _ = await loop.create_datagram_endpoint(
            lambda line=line: _LineProtocol(line),
            local_addr=(BIND_HOST, port),
        )
        transports.append(transport)
    transport, _ = await loop.create_datagram_endpoint(
        _MonitorProtocol, local_addr=(BIND_HOST, MONITOR_PORT)
    )
    transports.append(transport)
    tick_task = asyncio.create_task(_tick_loop())
    yield
    tick_task.cancel()
    for t in transports:
        t.close()
    for t in _pending_injects:
        t.cancel()


app = FastAPI(
    title="RTP 双线路合流网关",
    version="1.0.0",
    description=(
        "两条 UDP 线路接收同一 SSRC 的 RTP 包，按序去重后送唯一 UDP 出口。"
        "待发包最多等 50ms；缓存满 128 个跳到最靠前的包；缺号跳过后迟到的包丢弃。"
    ),
    lifespan=lifespan,
)


# ---- 样包投送 ----
def build_rtp(seq: int, ssrc: int) -> bytes:
    """构造一个最小 RTP 包：V=2, PT=96, 时间戳随意, 固定负载。"""
    header = struct.pack("!BBHII", 0x80, 96, seq & 0xFFFF, (seq * 3000) & 0xFFFFFFFF, ssrc)
    return header + b"rtp-merge-sample"


_inject_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_inject_sock.setblocking(False)
_pending_injects: set[asyncio.Task] = set()


class InjectRequest(BaseModel):
    line: int = Field(1, ge=1, le=2, description="线路编号：1 或 2")
    seq: int = Field(..., ge=0, le=65535, description="起始 RTP 序号（0-65535）")
    count: int = Field(1, ge=1, le=2048, description="连续投送包数，序号递增并按 65536 回绕")
    delay_ms: float = Field(0, ge=0, le=60000, description="投送前延时（毫秒）")
    interval_ms: float = Field(0, ge=0, le=60000, description="多包之间的间隔（毫秒）")
    ssrc: int = Field(DEFAULT_SSRC, ge=0, le=0xFFFFFFFF, description="RTP SSRC")


async def _inject_flow(req: InjectRequest, port: int) -> None:
    if req.delay_ms:
        await asyncio.sleep(req.delay_ms / 1000)
    for i in range(req.count):
        pkt = build_rtp((req.seq + i) % 65536, req.ssrc)
        try:
            _inject_sock.sendto(pkt, (INJECT_TARGET, port))
            counters["injected"] += 1
        except OSError:
            counters["send_errors"] += 1
        if req.interval_ms and i + 1 < req.count:
            await asyncio.sleep(req.interval_ms / 1000)


@app.post("/inject", summary="按线路、序号和延时投送样包")
async def inject(req: InjectRequest):
    port = LINE1_PORT if req.line == 1 else LINE2_PORT
    task = asyncio.create_task(_inject_flow(req, port))
    _pending_injects.add(task)
    task.add_done_callback(_pending_injects.discard)
    return {
        "scheduled": True,
        "line": req.line,
        "target": f"udp://{INJECT_TARGET}:{port}",
        "seqs": [ (req.seq + i) % 65536 for i in range(min(req.count, 8)) ],
        "count": req.count,
        "delay_ms": req.delay_ms,
        "interval_ms": req.interval_ms,
    }


# ---- 查询与控制 ----
@app.get("/", summary="网关说明与入口")
def root():
    return {
        "name": "RTP 双线路合流网关",
        "docs": "/docs",
        "endpoints": {
            "POST /inject": "按线路、序号、延时投送样包",
            "GET /received": "UDP 出口实收序号（内置监视器）",
            "GET /stats": "合流器状态与计数",
            "POST /reset": "清空合流状态与监视记录",
        },
        "udp": {
            "line1": LINE1_PORT, "line2": LINE2_PORT,
            "output": f"{OUTPUT_HOST}:{OUTPUT_PORT}", "monitor": MONITOR_PORT,
        },
    }


@app.get("/received", summary="UDP 出口实收序号")
def received(limit: int = 1000):
    items = list(monitor) if limit < 0 else list(monitor)[-limit:]
    t0 = monitor[0][1] if monitor else None
    return {
        "total_received": counters["monitor_received"],
        "stored": len(monitor),
        "returned": len(items),
        "seqs": [s for s, _ in items],
        "items": [
            {"seq": s, "t_ms": round((t - t0) * 1000, 2)} for s, t in items
        ] if t0 is not None else [],
        "note": "t_ms 为相对本次 reset 后首个实收包的毫秒数",
    }


@app.get("/stats", summary="合流器状态与计数")
def stats():
    return {
        "config": {
            "line1_port": LINE1_PORT, "line2_port": LINE2_PORT,
            "output": f"{OUTPUT_HOST}:{OUTPUT_PORT}", "monitor_port": MONITOR_PORT,
            "timeout_ms": TIMEOUT_MS, "buffer_max": BUFFER_MAX,
        },
        "merger": merger.stats(),
        "lines": line_stats,
        "counters": counters,
    }


@app.post("/reset", summary="清空合流状态与监视记录")
def reset():
    for t in list(_pending_injects):
        t.cancel()
    _pending_injects.clear()
    merger.reset()
    monitor.clear()
    line_stats[1]["received"] = 0
    line_stats[2]["received"] = 0
    counters.update({"send_errors": 0, "injected": 0,
                     "monitor_received": 0, "monitor_malformed": 0})
    return {"reset": True}
