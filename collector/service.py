"""只读状态服务：让资料员查看批次状态、投递轨迹与保存的原件。

与采集进程并行运行，共用同一存储（WAL 允许并发读）。
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from . import assemble
from .client import SourceClient, SourceError


def make_handler(store, source_client=None):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def _send(self, status, body, content_type='application/json; charset=utf-8'):
            if isinstance(body, (dict, list)):
                data = json.dumps(body, ensure_ascii=False, indent=2).encode('utf-8')
            else:
                data = body
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            if data:
                self.wfile.write(data)

        def do_GET(self):
            try:
                self._route()
            except Exception as exc:  # 状态服务不应因单个请求崩溃
                self._send(500, {'error': '%s: %s' % (type(exc).__name__, exc)})

        def _route(self):
            parts = [unquote(p) for p in urlsplit(self.path).path.split('/') if p]
            if parts == ['health']:
                return self._send(200, {'ok': True})
            if parts == ['batches']:
                return self._send(200, assemble.batch_board(store))
            if len(parts) == 4 and parts[0] == 'batches':
                device, boot, batch = parts[1], parts[2], _int(parts[3])
                if batch is None:
                    return self._send(400, {'error': 'batch 必须是整数'})
                return self._send(200, assemble.batch_detail(store, device, boot, batch))
            if len(parts) == 5 and parts[0] == 'batches' and parts[4] == 'readings':
                device, boot, batch = parts[1], parts[2], _int(parts[3])
                if batch is None:
                    return self._send(400, {'error': 'batch 必须是整数'})
                result, detail = assemble.readings(store, device, boot, batch)
                if result is None:
                    return self._send(409, {'error': '批次尚不能提供读数',
                                            'reason': detail['summary'],
                                            'batch': detail})
                return self._send(200, result)
            if parts == ['deliveries']:
                return self._send(200, self._deliveries())
            if len(parts) == 2 and parts[0] == 'deliveries':
                return self._delivery(parts[1])
            if len(parts) == 2 and parts[0] == 'originals':
                row = store.original(parts[1])
                if row is None:
                    return self._send(404, {'error': '没有该内容摘要的原件'})
                return self._send(200, bytes(row['bytes']),
                                  'application/octet-stream')
            return self._send(404, {'error': '未知路径'})

        def _source_rows(self):
            if source_client is None:
                return None
            try:
                return {row['delivery']: row
                        for row in source_client.status().get('deliveries', [])}
            except SourceError:
                return None

        def _deliveries(self):
            source_rows = self._source_rows()
            out = []
            for row in store.deliveries():
                item = dict(row)
                if source_rows is not None:
                    item['source'] = source_rows.get(row['delivery'])
                out.append(item)
            return out

        def _delivery(self, delivery):
            source_rows = self._source_rows()
            source_row = source_rows.get(delivery) if source_rows else None
            info = assemble.explain_delivery(store, delivery, source_row)
            if info['local'] is None and source_row is None:
                return self._send(404, {'error': '没有该投递的记录'})
            return self._send(200, info)

    return Handler


def _int(text):
    try:
        return int(text)
    except ValueError:
        return None


def serve(store, port, source=None, host='127.0.0.1'):
    client = SourceClient(source) if source else None
    server = ThreadingHTTPServer((host, port), make_handler(store, client))
    return server
