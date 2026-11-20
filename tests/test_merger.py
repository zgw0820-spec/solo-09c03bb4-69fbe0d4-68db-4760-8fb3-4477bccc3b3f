"""RtpMerger 单元测试：假时钟 + 记录式发送回调。

可直接运行：python3 tests/test_merger.py
也可用 pytest：pytest tests/
"""
import os
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.merger import HALF_CYCLE, RtpMerger, seq_of  # noqa: E402

SSRC = 0x11223344


def pkt(seq: int, ssrc: int = SSRC) -> bytes:
    return struct.pack("!BBHII", 0x80, 96, seq & 0xFFFF, 0, ssrc) + b"x"


def make(timeout_s=0.050, buffer_max=128):
    sent = []
    m = RtpMerger(sent.append, timeout_s=timeout_s, buffer_max=buffer_max)
    return m, sent


def sent_seqs(sent):
    return [seq_of(b) for b in sent]


def test_first_packet_starts_stream():
    m, sent = make()
    m.on_packet(pkt(500), now=0.0)
    assert sent_seqs(sent) == [500]
    assert m.started and m.next_seq == 501 and m.ssrc == SSRC


def test_in_order_forwarded_immediately():
    m, sent = make()
    for i, s in enumerate([500, 501, 502]):
        m.on_packet(pkt(s), now=i * 0.001)
    assert sent_seqs(sent) == [500, 501, 502]
    assert len(m.buffer) == 0


def test_gap_filled_by_either_line():
    """缺号由后到的包补齐，补齐后按序连发。"""
    m, sent = make()
    m.on_packet(pkt(500), now=0.0)          # 立即发
    m.on_packet(pkt(502), now=0.005)        # 缺 501，缓存
    m.on_packet(pkt(503), now=0.006)        # 缓存
    assert sent_seqs(sent) == [500]
    m.on_packet(pkt(501), now=0.010)        # 另一路补齐
    assert sent_seqs(sent) == [500, 501, 502, 503]


def test_duplicate_dropped():
    m, sent = make()
    m.on_packet(pkt(500), now=0.0)
    m.on_packet(pkt(500), now=0.001)        # 已发过 -> 迟到丢弃
    m.on_packet(pkt(502), now=0.002)        # 缓存
    m.on_packet(pkt(502), now=0.003)        # 缓存里已有 -> 重复丢弃
    assert sent_seqs(sent) == [500]
    assert m.late_dropped == 1 and m.dup_dropped == 1


def test_timeout_jump_then_late_packet_dropped():
    """待发包等满 50ms 后跳号发出；缺号被跳过后才到的包丢弃。"""
    m, sent = make()
    m.on_packet(pkt(500), now=0.0)
    m.on_packet(pkt(502), now=0.010)        # 缺 501
    m.tick(now=0.030)                       # 才等 20ms，不发
    assert sent_seqs(sent) == [500]
    m.tick(now=0.061)                       # 等了 51ms >= 50ms，跳到 502
    assert sent_seqs(sent) == [500, 502]
    assert m.timeout_jumps == 1 and m.buffer_jumps == 0
    m.on_packet(pkt(501), now=0.062)        # 501 已被跳过 -> 丢弃
    assert m.late_dropped == 1
    assert sent_seqs(sent) == [500, 502]


def test_wraparound_order_and_dup():
    """跨 65535 回绕按序转发；回绕后同一原包最多发一次。"""
    m, sent = make()
    for i, s in enumerate([65534, 65535, 0, 1]):
        m.on_packet(pkt(s), now=i * 0.001)
    assert sent_seqs(sent) == [65534, 65535, 0, 1]
    assert m.next_seq == 2
    m.on_packet(pkt(65535), now=0.010)      # 回绕前的重复包
    m.on_packet(pkt(0), now=0.011)          # 回绕后的重复包
    assert m.late_dropped == 2
    assert sent_seqs(sent) == [65534, 65535, 0, 1]


def test_gap_across_wrap_released_by_timeout():
    """跨回绕的缺号同样按 50ms 超时跳过。"""
    m, sent = make()
    m.on_packet(pkt(65530), now=0.0)        # 首包
    m.on_packet(pkt(65532), now=0.010)      # 缺 65531
    m.on_packet(pkt(1), now=0.010)          # 还缺 65533..65535、0
    m.tick(now=0.061)
    assert sent_seqs(sent) == [65530, 65532, 1]
    assert m.timeout_jumps == 2


def test_buffer_limit_jump():
    """缓存达到 128 个待发包时跳到最靠前的包继续发。"""
    m, sent = make()
    m.on_packet(pkt(100), now=0.0)          # 首包即发，next=101
    for i, s in enumerate(range(102, 230)):  # 102..229 共 128 个，缺 101
        m.on_packet(pkt(s), now=0.001 + i * 0.0001)
    # 第 128 个待发包入缓存时即触发跳转：发 102 并连发 103..229
    assert sent_seqs(sent) == [100] + list(range(102, 230))
    assert m.buffer_jumps == 1 and m.timeout_jumps == 0
    m.on_packet(pkt(230), now=0.02)         # 后续包恢复正常直发
    assert sent_seqs(sent)[-1] == 230


def test_buffer_limit_not_triggered_below_128():
    m, sent = make()
    m.on_packet(pkt(100), now=0.0)
    for i, s in enumerate(range(102, 229)):  # 102..228 共 127 个
        m.on_packet(pkt(s), now=0.001 + i * 0.0001)
    assert sent_seqs(sent) == [100]         # 未达 128，且未超时
    assert m.buffer_jumps == 0 and len(m.buffer) == 127


def test_half_cycle_boundary():
    """与待发位置相差不到半个序号周期才接受；恰好半个周期按迟到处理。"""
    m, sent = make()
    m.on_packet(pkt(1000), now=0.0)
    base = m.next_seq                                    # 首包发出后待发位置为 1001
    ahead_ok = (base + HALF_CYCLE - 1) % 65536           # 相差 32767
    ahead_bad = (base + HALF_CYCLE) % 65536              # 相差 32768
    m.on_packet(pkt(ahead_ok), now=0.001)
    assert ahead_ok in m.buffer
    m.on_packet(pkt(ahead_bad), now=0.002)
    assert ahead_bad not in m.buffer and m.late_dropped == 1


def test_ssrc_filter():
    """首包之后的异 SSRC 包被丢弃。"""
    m, sent = make()
    m.on_packet(pkt(700, ssrc=0xAAAA), now=0.0)
    m.on_packet(pkt(701, ssrc=0xBBBB), now=0.001)
    assert m.ssrc_dropped == 1 and sent_seqs(sent) == [700]
    m.on_packet(pkt(701, ssrc=0xAAAA), now=0.002)
    assert sent_seqs(sent) == [700, 701]


def test_malformed_dropped():
    m, sent = make()
    m.on_packet(b"\x00\x01", now=0.0)
    assert m.malformed_dropped == 1 and not m.started and sent == []


def test_tick_before_first_packet_is_noop():
    m, sent = make()
    m.tick(now=1.0)
    assert sent == [] and not m.started


def _run_all() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {name}: {e!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run_all())
