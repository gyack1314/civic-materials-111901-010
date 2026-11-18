"""来源 HTTP 客户端。

只搬运来源响应，不在本地推断租约是否有效：租约、续约、确认、拒收
的生效与否一律以来源的 200/409 等响应为准（见 docs/source-contract.md）。
"""
import http.client
import json
from urllib.parse import urlsplit


class SourceError(Exception):
    """来源交互出现非预期响应。"""


class TemporarySourceError(SourceError):
    """临时性失败（连接错误、5xx）：投递不是坏件，稍后可再取。"""


class PayloadMissing(SourceError):
    """来源侧载荷不存在（4xx）：按永久坏件处理。"""


class PayloadTooLarge(SourceError):
    """载荷超过本地接收上限：按永久坏件处理，避免积压占内存。"""


class SourceClient:
    def __init__(self, base, timeout=10.0):
        parts = urlsplit(base)
        if parts.scheme not in ('http', '') or not parts.hostname:
            raise SourceError('来源地址必须是 http://host:port 形式: %r' % base)
        self.host = parts.hostname
        self.port = parts.port or 80
        self.timeout = timeout

    def _request(self, method, path, payload=None):
        body = json.dumps(payload).encode('utf-8') if payload is not None else None
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            conn.request(method, path, body,
                         {'Content-Type': 'application/json'} if body is not None else {})
            resp = conn.getresponse()
            status, headers = resp.status, dict(resp.getheaders())
            limit = getattr(self, '_read_limit', None)
            if limit is not None:
                data = resp.read(limit + 1)
                if len(data) > limit:
                    raise PayloadTooLarge('载荷超过接收上限 %d 字节' % limit)
            else:
                data = resp.read()
            return status, headers, data
        except (PayloadTooLarge,):
            raise
        except (OSError, http.client.HTTPException) as exc:
            raise TemporarySourceError('来源连接失败: %s' % exc) from exc
        finally:
            conn.close()

    @staticmethod
    def _json(data):
        return json.loads(data.decode('utf-8')) if data else {}

    # ---- 产品接口 ----
    def claim(self, worker):
        status, _, data = self._request('POST', '/claim', {'worker': worker})
        if status == 204:
            return None
        if status != 200:
            raise SourceError('领取失败 HTTP %d: %r' % (status, data[:200]))
        return self._json(data)

    def fetch_payload(self, path, max_bytes):
        self._read_limit = max_bytes
        try:
            status, _, data = self._request('GET', path)
        finally:
            self._read_limit = None
        if status >= 500:
            raise TemporarySourceError('来源临时读取失败 HTTP %d' % status)
        if status != 200:
            raise PayloadMissing('来源无法提供载荷 HTTP %d: %s' % (status, path))
        return data

    def _lease_op(self, op, delivery, lease, reason=None):
        payload = {'delivery': delivery, 'lease': lease}
        if reason is not None:
            payload['reason'] = reason
        status, _, data = self._request('POST', '/' + op, payload)
        body = self._json(data)
        if status == 409:
            return {'ok': False, 'done': bool(body.get('done')), 'error': body.get('error')}
        if status != 200:
            raise SourceError('%s 失败 HTTP %d: %r' % (op, status, body))
        return {'ok': True, **body}

    def renew(self, delivery, lease):
        return self._lease_op('renew', delivery, lease)

    def ack(self, delivery, lease):
        return self._lease_op('ack', delivery, lease)

    def reject(self, delivery, lease, reason):
        return self._lease_op('reject', delivery, lease, reason=reason)

    def status(self):
        status, _, data = self._request('GET', '/status')
        if status != 200:
            raise SourceError('状态查询失败 HTTP %d' % status)
        return self._json(data)

    def health(self):
        status, _, data = self._request('GET', '/health')
        if status != 200:
            raise SourceError('健康查询失败 HTTP %d' % status)
        return self._json(data)

    # ---- 演练控制（仅供测试/操作员调试来源，产品流程不调用）----
    def admin_advance(self, seconds=0, phase=None):
        payload = {'seconds': seconds}
        if phase is not None:
            payload['phase'] = phase
        status, _, data = self._request('POST', '/__admin/advance', payload)
        if status != 200:
            raise SourceError('演练控制失败 HTTP %d' % status)
        return self._json(data)
