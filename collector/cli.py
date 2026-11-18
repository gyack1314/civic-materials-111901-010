"""命令行入口：采集进程、只读状态服务与批次/投递查看。"""
import argparse
import json
import sys

from . import assemble
from .client import SourceClient
from .service import serve
from .store import Store
from .worker import Collector


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def cmd_collect(args):
    client = SourceClient(args.source)
    store = Store(args.store)
    try:
        collector = Collector(client, store, worker=args.worker,
                              idle_sleep=args.idle_sleep)
        claims = collector.run(once=args.once, max_claims=args.max_claims)
        print('worker %s 结束，处理领取 %d 条' % (collector.worker, claims))
    finally:
        store.close()
    return 0


def cmd_serve(args):
    store = Store(args.store)
    server = serve(store, args.port, source=args.source)
    print('状态服务已启动，端口 %d' % server.server_port, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()
    return 0


def cmd_batches(args):
    store = Store(args.store)
    try:
        _print(assemble.batch_board(store))
    finally:
        store.close()
    return 0


def _batch_args(parser):
    parser.add_argument('--device', required=True)
    parser.add_argument('--boot', required=True)
    parser.add_argument('--batch', required=True, type=int)


def cmd_batch(args):
    store = Store(args.store)
    try:
        _print(assemble.batch_detail(store, args.device, args.boot, args.batch))
    finally:
        store.close()
    return 0


def cmd_readings(args):
    store = Store(args.store)
    try:
        result, detail = assemble.readings(store, args.device, args.boot, args.batch)
        if result is None:
            _print({'error': '批次尚不能提供读数', 'reason': detail['summary'],
                    'batch': detail})
            return 3
        _print(result)
    finally:
        store.close()
    return 0


def _source_rows(args):
    if not getattr(args, 'source', None):
        return None
    try:
        return {row['delivery']: row
                for row in SourceClient(args.source).status().get('deliveries', [])}
    except Exception as exc:
        print('（来源 /status 暂不可用：%s，以下仅本地记录）' % exc,
              file=sys.stderr)
        return None


def cmd_deliveries(args):
    store = Store(args.store)
    try:
        source_rows = _source_rows(args)
        out = []
        for row in store.deliveries(worker=args.worker):
            item = dict(row)
            if source_rows is not None:
                item['source'] = source_rows.get(row['delivery'])
            out.append(item)
        _print(out)
    finally:
        store.close()
    return 0


def cmd_delivery(args):
    store = Store(args.store)
    try:
        source_rows = _source_rows(args)
        source_row = source_rows.get(args.delivery) if source_rows else None
        info = assemble.explain_delivery(store, args.delivery, source_row)
        if info['local'] is None and source_row is None:
            print('没有投递 %s 的记录' % args.delivery, file=sys.stderr)
            return 1
        _print(info)
        print(info['why_redelivered'])
    finally:
        store.close()
    return 0


def cmd_original(args):
    store = Store(args.store)
    try:
        row = store.original(args.sha256)
        if row is None:
            print('没有内容摘要 %s 的原件' % args.sha256, file=sys.stderr)
            return 1
        data = bytes(row['bytes'])
        if args.out:
            with open(args.out, 'wb') as fh:
                fh.write(data)
            print('已写出 %d 字节到 %s' % (len(data), args.out))
        else:
            _print({k: (v if k != 'bytes' else data.decode('utf-8', 'replace'))
                    for k, v in row.items()})
    finally:
        store.close()
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog='collector', description='社区节能观测采集端')
    sub = parser.add_subparsers(dest='command', required=True)

    collect = sub.add_parser('collect', help='运行采集进程（可多进程并行）')
    collect.add_argument('--source', required=True, help='来源地址，如 http://127.0.0.1:8765')
    collect.add_argument('--store', required=True, help='SQLite 存储文件路径')
    collect.add_argument('--worker', help='进程名（默认 主机:进程号）')
    collect.add_argument('--once', action='store_true', help='领完即止（用于演练）')
    collect.add_argument('--max-claims', type=int, help='最多处理多少条领取后停止')
    collect.add_argument('--idle-sleep', type=float, default=2.0,
                         help='无可领取投递时的等待秒数')
    collect.set_defaults(func=cmd_collect)

    status = sub.add_parser('serve', help='运行只读状态服务')
    status.add_argument('--store', required=True)
    status.add_argument('--source', help='来源地址（用于关联运输状态）')
    status.add_argument('--port', type=int, default=8766)
    status.set_defaults(func=cmd_serve)

    batches = sub.add_parser('batches', help='查看全部业务批次状态')
    batches.add_argument('--store', required=True)
    batches.set_defaults(func=cmd_batches)

    batch = sub.add_parser('batch', help='查看单个业务批次详情')
    batch.add_argument('--store', required=True)
    _batch_args(batch)
    batch.set_defaults(func=cmd_batch)

    readings = sub.add_parser('readings', help='取得批次读数（仅完整一致时）')
    readings.add_argument('--store', required=True)
    _batch_args(readings)
    readings.set_defaults(func=cmd_readings)

    deliveries = sub.add_parser('deliveries', help='查看运输投递处理状态')
    deliveries.add_argument('--store', required=True)
    deliveries.add_argument('--source', help='来源地址（关联实时运输状态）')
    deliveries.add_argument('--worker', help='只看某个进程领取的投递')
    deliveries.set_defaults(func=cmd_deliveries)

    delivery = sub.add_parser('delivery', help='解释单条投递（来源为何仍在重发）')
    delivery.add_argument('--store', required=True)
    delivery.add_argument('--delivery', required=True)
    delivery.add_argument('--source', help='来源地址（关联实时运输状态）')
    delivery.set_defaults(func=cmd_delivery)

    original = sub.add_parser('original', help='回查保存的原件')
    original.add_argument('--store', required=True)
    original.add_argument('--sha256', required=True, help='内容摘要')
    original.add_argument('--out', help='写出到文件')
    original.set_defaults(func=cmd_original)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
