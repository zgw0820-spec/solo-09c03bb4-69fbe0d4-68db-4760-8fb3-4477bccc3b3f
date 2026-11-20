"""RTP 合流核心：把两条线路送来的同一 SSRC 的 RTP 包按序号合并成一路。

规则（与需求一一对应）：
1. 首包决定起始序号，从首包起按序、原样转发到唯一 UDP 出口；
2. 来包序号与待发位置相差不到半个序号周期（32768）才接受，
   否则视为迟到包或已发过的重复包，直接丢弃 —— 跨 65535 回绕同样成立，
   因此同一原包最多只发一次；
3. 缺号可由任一路补齐：乱序包进缓存按序号等待，谁先到用谁；
4. 每个待发包从首次收到起最多等 timeout_s（默认 50ms），
   超时则跳到该包按序发出，途中的缺号一并跳过；
5. 待发包缓存达到 buffer_max（默认 128）个时，跳到最靠前的包继续发；
6. 缺号被跳过后才到的包，因规则 2 落在待发位置“身后”而被丢弃。

本模块不做任何 I/O：发送通过构造函数注入的回调完成，时间由调用方传入，
方便用假时钟做单元测试。
"""
from __future__ import annotations

SEQ_MOD = 1 << 16           # RTP 序号空间：0..65535
HALF_CYCLE = SEQ_MOD // 2   # 半个序号周期：32768

DEFAULT_TIMEOUT_S = 0.050   # 待发包最长等待 50ms
DEFAULT_BUFFER_MAX = 128    # 待发包缓存上限


def seq_of(packet: bytes) -> int:
    """RTP 头部第 3-4 字节为序号（大端）。"""
    return (packet[2] << 8) | packet[3]


def ssrc_of(packet: bytes) -> int:
    """RTP 头部第 9-12 字节为 SSRC（大端）。"""
    return int.from_bytes(packet[8:12], "big")


class RtpMerger:
    """同 SSRC 双线路 RTP 包的去重保序合流器。"""

    def __init__(self, send, timeout_s: float = DEFAULT_TIMEOUT_S,
                 buffer_max: int = DEFAULT_BUFFER_MAX):
        # send: 回调函数(bytes)，把待发的原始 RTP 数据报送向出口
        self._send = send
        self.timeout_s = timeout_s
        self.buffer_max = buffer_max
        self.reset()

    def reset(self) -> None:
        self.started = False
        self.next_seq = 0                       # 待发位置：下一个应发出的序号
        self.ssrc: int | None = None            # 首包学习到的 SSRC
        self.buffer: dict[int, tuple[bytes, float]] = {}  # seq -> (原始包, 首次收到时刻)
        # 统计计数
        self.sent = 0               # 已发出包数
        self.dup_dropped = 0        # 缓存中已存在的重复包
        self.late_dropped = 0       # 落在待发位置身后（迟到/已发过）的包
        self.malformed_dropped = 0  # 长度不足 RTP 头的包
        self.ssrc_dropped = 0       # SSRC 与首包不一致的包
        self.timeout_jumps = 0      # 因 50ms 超时触发的跳号次数
        self.buffer_jumps = 0       # 因缓存满 128 触发的跳号次数

    # ---- 输入：任一线路收到一个 RTP 包 ----
    def on_packet(self, data: bytes, now: float) -> None:
        if len(data) < 12:
            self.malformed_dropped += 1
            return
        seq = seq_of(data)
        ssrc = ssrc_of(data)
        if not self.started:
            # 首包：建立起始序号与 SSRC
            self.started = True
            self.next_seq = seq
            self.ssrc = ssrc
        elif ssrc != self.ssrc:
            self.ssrc_dropped += 1
            return
        diff = (seq - self.next_seq) % SEQ_MOD
        if diff >= HALF_CYCLE:
            # 与待发位置相差达到/超过半个周期：已发过或已跳过的迟到包
            self.late_dropped += 1
            return
        if seq in self.buffer:
            # 另一路送来的同一原包：只留一份
            self.dup_dropped += 1
            return
        self.buffer[seq] = (data, now)
        self._pump(now)

    # ---- 定时驱动：检查 50ms 超时 ----
    def tick(self, now: float) -> None:
        self._pump(now)

    # ---- 内部 ----
    def _drain(self) -> None:
        """把缓存中与待发位置连续的包按序发出。"""
        while self.next_seq in self.buffer:
            data, _ = self.buffer.pop(self.next_seq)
            self._send(data)
            self.sent += 1
            self.next_seq = (self.next_seq + 1) % SEQ_MOD

    def _front_seq(self) -> int:
        """缓存中离待发位置最近（最靠前）的序号。"""
        best, best_d = -1, SEQ_MOD
        for s in self.buffer:
            d = (s - self.next_seq) % SEQ_MOD
            if d < best_d:
                best, best_d = s, d
        return best

    def _jump_to(self, seq: int) -> None:
        """跳过途中缺号，直接发到 seq，然后继续按序连发。"""
        data, _ = self.buffer.pop(seq)
        self._send(data)
        self.sent += 1
        self.next_seq = (seq + 1) % SEQ_MOD
        self._drain()

    def _pump(self, now: float) -> None:
        if not self.started:
            return
        self._drain()
        while self.buffer:
            front = self._front_seq()
            _, first_seen = self.buffer[front]
            timed_out = (now - first_seen) >= self.timeout_s
            overfull = len(self.buffer) >= self.buffer_max
            if timed_out:
                self.timeout_jumps += 1
                self._jump_to(front)
            elif overfull:
                self.buffer_jumps += 1
                self._jump_to(front)
            else:
                break

    def stats(self) -> dict:
        pending = sorted(self.buffer, key=lambda s: (s - self.next_seq) % SEQ_MOD)
        return {
            "started": self.started,
            "ssrc": self.ssrc,
            "next_seq": self.next_seq,
            "buffer_len": len(self.buffer),
            "buffer_seqs_head": pending[:32],
            "sent": self.sent,
            "dup_dropped": self.dup_dropped,
            "late_dropped": self.late_dropped,
            "malformed_dropped": self.malformed_dropped,
            "ssrc_dropped": self.ssrc_dropped,
            "timeout_jumps": self.timeout_jumps,
            "buffer_jumps": self.buffer_jumps,
        }
