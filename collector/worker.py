"""采集工作进程：领取 → 取原件 → 持久保存 → 确认/拒收。

原则：
- 确认之前原件必须已经持久保存；批次是否拼完不影响单条投递的确认，
  避免迟到分片把租约拖过期。
- 租约是否有效以来源响应为准（200/409），本地不按经过秒数推断。
- 坏格式或运输摘要不符：先保留原件与处置依据，再按契约拒收；
  临时读取失败（5xx/连接错误）不拒收，等租约失效后来源会重发。
- 单进程一次只处理一条投递，积压留在来源端，内存占用有界；
  需要并发时增加进程数，多进程共用同一存储。
"""
import hashlib
import json
import os
import signal
import socket
import time

from .client import PayloadMissing, PayloadTooLarge, TemporarySourceError


class EnvelopeError(Exception):
    """载荷格式不符合契约。"""


def _require_str(doc, field):
    value = doc.get(field)
    if not isinstance(value, str) or not value:
        raise EnvelopeError('字段 %s 缺失或不是非空字符串' % field)
    return value


def _require_int(doc, field):
    value = doc.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise EnvelopeError('字段 %s 缺失或不是整数' % field)
    return value


def validate_envelope(doc):
    """校验并返回 (kind, 归一化信息)。坏格式抛 EnvelopeError。"""
    if not isinstance(doc, dict):
        raise EnvelopeError('载荷不是 JSON 对象')
    device = _require_str(doc, 'device')
    boot = _require_str(doc, 'boot')
    batch = _require_int(doc, 'batch')
    kind = doc.get('kind')
    if kind == 'manifest':
        parts = doc.get('parts')
        if not isinstance(parts, dict) or not parts:
            raise EnvelopeError('manifest 缺少非空 parts 表')
        normalized = {}
        for key, digest in parts.items():
            try:
                part_no = int(key)
            except (TypeError, ValueError):
                raise EnvelopeError('manifest 的分片号不是整数: %r' % (key,))
            if not (isinstance(digest, str) and len(digest) == 64
                    and all(c in '0123456789abcdef' for c in digest)):
                raise EnvelopeError('分片 %s 的内容摘要不是 sha256 十六进制' % key)
            normalized[part_no] = digest
        return 'manifest', {'device': device, 'boot': boot, 'batch': batch,
                            'parts': normalized}
    if kind == 'part':
        part = _require_int(doc, 'part')
        readings = doc.get('readings')
        if not isinstance(readings, list):
            raise EnvelopeError('part 缺少 readings 数组')
        for item in readings:
            if not isinstance(item, dict) or not isinstance(item.get('time'), str):
                raise EnvelopeError('readings 元素缺少 time 字符串')
            value = item.get('value')
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise EnvelopeError('readings 元素的 value 不是数值')
        return 'part', {'device': device, 'boot': boot, 'batch': batch,
                        'part': part, 'readings': readings}
    raise EnvelopeError('未知 kind: %r' % (kind,))


class Collector:
    def __init__(self, client, store, worker=None, *,
                 max_payload_bytes=32 * 1024 * 1024,
                 fetch_retries=2, retry_pause=0.3, idle_sleep=2.0):
        self.client = client
        self.store = store
        self.worker = worker or '%s:%d' % (socket.gethostname(), os.getpid())
        self.max_payload_bytes = max_payload_bytes
        self.fetch_retries = fetch_retries
        self.retry_pause = retry_pause
        self.idle_sleep = idle_sleep
        self.stop_requested = False
        self._temp_failures = 0

    # ---- 主循环 ----
    def run(self, *, once=False, max_claims=None):
        previous = self._install_signals()
        claims = 0
        try:
            while not self.stop_requested:
                try:
                    claim = self.client.claim(self.worker)
                except TemporarySourceError:
                    # 来源暂时不可达：退避后重试，不退出
                    time.sleep(self.idle_sleep)
                    continue
                if claim is None:
                    if once or (max_claims is not None and claims >= max_claims):
                        break
                    time.sleep(self.idle_sleep)
                    continue
                claims += 1
                self.process(claim)
                if max_claims is not None and claims >= max_claims:
                    break
        finally:
            self._restore_signals(previous)
        return claims

    def _install_signals(self):
        previous = {}

        def handler(signum, frame):
            self.stop_requested = True

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                previous[sig] = signal.signal(sig, handler)
            except (ValueError, OSError):
                pass  # 非主线程时不装信号处理
        return previous

    def _restore_signals(self, previous):
        for sig, old in previous.items():
            try:
                signal.signal(sig, old)
            except (ValueError, OSError):
                pass

    # ---- 单条投递 ----
    def process(self, claim):
        delivery = claim['delivery']
        lease = claim['lease']
        self.store.record_claim(delivery, claim.get('payload'),
                                claim.get('sha256'), self.worker, lease,
                                claim.get('until'))
        try:
            data = self._fetch(claim)
        except TemporarySourceError as exc:
            # 临时读取失败：不确认也不拒收，租约失效后来源会重发
            self.store.record_temp_failure(delivery, self.worker, str(exc))
            self._temp_failures += 1
            if self._temp_failures >= 3:
                time.sleep(min(self.idle_sleep, 5.0))  # 退避，避免空转抢占租约维护
            return 'temp_failed'
        except (PayloadMissing, PayloadTooLarge) as exc:
            return self._reject(claim, None, None, '来源载荷不可用: %s' % exc)
        self._temp_failures = 0

        actual = hashlib.sha256(data).hexdigest()
        if actual != claim.get('sha256'):
            return self._reject(claim, data, actual,
                                '运输摘要不符: 申报 %s 实际 %s'
                                % (claim.get('sha256'), actual))
        try:
            doc = json.loads(data.decode('utf-8'))
            kind, info = validate_envelope(doc)
        except (UnicodeDecodeError, ValueError, EnvelopeError) as exc:
            return self._reject(claim, data, actual, '格式不符: %s' % exc)

        # 先持久保存原件，再按业务身份登记观测（同事务去重），最后确认
        self.store.save_original(delivery, claim.get('payload'), data, actual,
                                 self.worker)
        if kind == 'manifest':
            self.store.record_manifest(info['device'], info['boot'], info['batch'],
                                       actual, info['parts'], delivery, self.worker)
        else:
            self.store.record_fragment(info['device'], info['boot'], info['batch'],
                                       info['part'], actual, doc, delivery,
                                       self.worker)
        return self._ack(claim)

    def _fetch(self, claim):
        """取载荷；临时失败时在租约仍有效果的前提下有界重试。"""
        attempts = 1 + self.fetch_retries
        last_error = None
        for attempt in range(attempts):
            try:
                return self.client.fetch_payload(claim['payload'],
                                                 self.max_payload_bytes)
            except TemporarySourceError as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    break
                renewed = self.client.renew(claim['delivery'], claim['lease'])
                if not renewed.get('ok'):
                    # 租约已失效：以来源响应为准，停止处理，等重发
                    self.store.record_lease_lost(claim['delivery'], self.worker,
                                                 'renew', renewed.get('done'),
                                                 claim['lease'])
                    raise TemporarySourceError('续约被来源拒绝，放弃本次领取') from exc
                self.store.record_renew(claim['delivery'], self.worker,
                                        claim['lease'], renewed)
                time.sleep(self.retry_pause)
        raise last_error

    def _ack(self, claim):
        try:
            result = self.client.ack(claim['delivery'], claim['lease'])
        except TemporarySourceError as exc:
            # 确认结果未知：以来源状态为准，留待重发时核对
            self.store.record_temp_failure(claim['delivery'], self.worker,
                                           '确认时来源不可达: %s' % exc)
            return 'temp_failed'
        if result.get('ok'):
            self.store.record_ack(claim['delivery'], self.worker,
                                  claim['lease'], result)
            return 'acked'
        # 租约已失效：可能已被他人领取甚至完成；已保存的原件保留，不删不重
        self.store.record_lease_lost(claim['delivery'], self.worker, 'ack',
                                     result.get('done'), claim['lease'])
        return 'lease_lost'

    def _reject(self, claim, data, content_sha256, reason):
        delivery = claim['delivery']
        if data is not None:
            # 先保留原件与处置依据，再走契约拒收
            self.store.save_original(delivery, claim.get('payload'), data,
                                     content_sha256, self.worker)
        try:
            result = self.client.reject(delivery, claim['lease'], reason)
        except TemporarySourceError as exc:
            self.store.record_temp_failure(delivery, self.worker,
                                           '拒收时来源不可达: %s' % exc)
            return 'temp_failed'
        if result.get('ok'):
            self.store.record_reject(delivery, self.worker, claim['lease'],
                                     reason, content_sha256, result)
            return 'rejected'
        self.store.record_lease_lost(delivery, self.worker, 'reject',
                                     result.get('done'), claim['lease'],
                                     reason=reason)
        return 'lease_lost'
