"""采集端检查：多进程争用、租约竞争、临时失败、坏件处置、缺片与冲突。

所有断言都落在实际取得的内容与可用观测结果上：确认接口返回成功
本身不足以表示采集闭合。
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import source.server as source_server
from source.server import running_source

from collector import assemble
from collector.client import SourceClient
from collector.service import serve
from collector.store import Store
from collector.worker import Collector, validate_envelope, EnvelopeError

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def make_data_dir(tmp, deliveries):
    """在临时目录构造合成来源资料，返回数据目录路径。"""
    data = Path(tmp) / 'data'
    payloads = data / 'payloads'
    payloads.mkdir(parents=True)
    rows = []
    for name, body in deliveries.items():
        (payloads / name).write_bytes(body)
        rows.append({'delivery': name.split('.')[0].upper(),
                     'payload': '/payload/%s' % name, 'sha256': sha(body)})
    (data / 'deliveries.json').write_text(json.dumps(rows), encoding='utf-8')
    return data


def patched_data(path):
    return _PatchData(path)


class _PatchData:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.old = source_server.DATA
        source_server.DATA = Path(self.path)

    def __exit__(self, *exc):
        source_server.DATA = self.old


def drain(base, db, worker='w'):
    """单进程领完即止，返回 Collector。"""
    client = SourceClient(base)
    collector = Collector(client, Store(db), worker=worker, idle_sleep=0.05,
                          retry_pause=0.01)
    collector.run(once=True)
    return collector


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = str(Path(self.tmp.name) / 'collector.db')

    def store(self):
        return Store(self.db)


class EndToEnd(CollectorCase):
    """多进程并行领取同一样本来源，核对实际内容与观测结果。"""

    def test_multi_process_drain(self):
        with running_source() as (base, _):
            env = dict(os.environ)
            procs = [subprocess.Popen(
                [sys.executable, '-m', 'collector', 'collect',
                 '--source', base, '--store', self.db,
                 '--worker', 'w%d' % i, '--once', '--idle-sleep', '0.1'],
                cwd=str(ROOT), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for i in range(3)]
            for proc in procs:
                out, _ = proc.communicate(timeout=120)
                self.assertEqual(proc.returncode, 0, out.decode())

            # 来源侧：7 条投递全部完结，6 确认 1 拒收
            status = SourceClient(base).status()
            done = {row['delivery']: row for row in status['deliveries']}
            self.assertEqual(len(done), 7)
            self.assertTrue(all(row['done'] for row in done.values()))
            self.assertEqual(done['T-7']['disposition'], 'rejected')
            self.assertEqual(done['T-5']['disposition'], 'accepted')

            store = self.store()
            # 原件按内容去重：7 条投递、6 份不同字节
            self.assertEqual(store.stats()['originals'], 6)
            self.assertEqual(store.stats()['fragments'], 4)
            self.assertEqual(store.stats()['manifests'], 1)
            # 同一内容的重复投递（T-2/T-3）只产生一份观测
            p1_sha = sha((DATA / 'payloads/p1.json').read_bytes())
            self.assertEqual(sorted(store.deliveries_for_content(p1_sha)),
                             ['T-2', 'T-3'])
            # 坏件：原件与处置依据都保留，且来源确认拒收
            bad_sha = sha((DATA / 'payloads/bad.json').read_bytes())
            self.assertEqual(bytes(store.original(bad_sha)['bytes']),
                             (DATA / 'payloads/bad.json').read_bytes())
            rejections = store.rejections('T-7')
            self.assertEqual(len(rejections), 1)
            self.assertTrue(rejections[0]['confirmed'])
            self.assertIn('格式不符', rejections[0]['reason'])

            # 批次 boot-a：分片 1 冲突保留，两个候选都在，不提供读数
            detail = assemble.batch_detail(store, 'D01', 'boot-a', 1)
            self.assertEqual(detail['state'], 'conflicted')
            part1 = next(p for p in detail['parts'] if p['part'] == 1)
            self.assertEqual(part1['state'], 'conflict')
            self.assertEqual(len(part1['candidates']), 2)
            result, refused = assemble.readings(store, 'D01', 'boot-a', 1)
            self.assertIsNone(result)
            self.assertIn('冲突', refused['summary'])
            # 分片 2 的真实 0 值读数完整保存（缺读数不补零，0 也是数据）
            part2 = next(p for p in detail['parts'] if p['part'] == 2)
            self.assertEqual(part2['state'], 'ok')
            self.assertEqual(part2['candidates'][0]['readings'][0]['value'], 0)

            # 设备重启后的同序号批次是另一个业务批次：缺声明，不完整
            reboot = assemble.batch_detail(store, 'D01', 'boot-b', 1)
            self.assertEqual(reboot['state'], 'incomplete')
            self.assertEqual(reboot['progress']['unverifiable_parts'], 1)

            # 每条投递都有处理轨迹，可解释来源行为
            for delivery in done:
                info = assemble.explain_delivery(store, delivery, done[delivery])
                self.assertTrue(info['events'], delivery)
                self.assertIn('不再重发', info['why_redelivered'])
            store.close()


class LeaseRace(CollectorCase):
    """保存完原件后租约刚好过期：旧持有者确认被拒，新持有者完成接收。"""

    def test_lease_expires_between_save_and_ack(self):
        body = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                           'kind': 'part', 'part': 1,
                           'readings': [{'time': 't', 'value': 3}]}).encode()
        with patched_data(make_data_dir(self.tmp.name, {'p.json': body})), \
                running_source() as (base, _):
            client = SourceClient(base)
            # w1 领取后时钟推进，租约在处理中途过期
            claim = client.claim('w1')
            client.admin_advance(seconds=5)
            late = Collector(client, Store(self.db), worker='w1', retry_pause=0.01)
            outcome = late.process(claim)
            self.assertEqual(outcome, 'lease_lost')

            store = self.store()
            saved = store.delivery(claim['delivery'])
            self.assertEqual(saved['state'], 'lease_lost')
            self.assertEqual(saved['content_sha256'], sha(body))
            events = [e['event'] for e in store.delivery_events(claim['delivery'])]
            self.assertIn('ack_lease_lost', events)

            # w2 重新领取同一投递并完成确认：原件去重，不产生第二份观测
            again = client.claim('w2')
            self.assertEqual(again['delivery'], claim['delivery'])
            self.assertNotEqual(again['lease'], claim['lease'])
            fresh = Collector(client, Store(self.db), worker='w2', retry_pause=0.01)
            self.assertEqual(fresh.process(again), 'acked')
            self.assertEqual(store.stats()['originals'], 1)
            self.assertEqual(store.stats()['fragments'], 1)
            self.assertEqual(store.delivery(claim['delivery'])['state'], 'acked')
            # 来源停止重发
            self.assertIsNone(client.claim('w3'))
            store.close()

    def test_saved_but_unacked_work_is_finished_by_others(self):
        """已保存未确认的工作，由后来的进程继续核对直至来源确认。"""
        body = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                           'kind': 'part', 'part': 1,
                           'readings': [{'time': 't', 'value': 8}]}).encode()
        with patched_data(make_data_dir(self.tmp.name, {'p.json': body})), \
                running_source() as (base, _):
            client = SourceClient(base)
            claim = client.claim('w-gone')
            # 进程停止领取：只保存了原件，没来得及确认，租约失效
            data = client.fetch_payload(claim['payload'], 1 << 20)
            store = self.store()
            store.save_original(claim['delivery'], claim['payload'], data,
                                sha(data), 'w-gone')
            client.admin_advance(seconds=5)
            store.close()

            drain(base, self.db, worker='w-next')
            store = self.store()
            self.assertEqual(store.delivery(claim['delivery'])['state'], 'acked')
            self.assertEqual(store.stats()['originals'], 1)
            self.assertEqual(store.stats()['fragments'], 1)
            store.close()


class TemporaryFailure(CollectorCase):
    """phase=2 的临时 503：不拒收、不确认，恢复后正常完成。"""

    def test_phase2_payload_failure_is_not_rejected(self):
        body = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                           'kind': 'part', 'part': 1,
                           'readings': [{'time': 't', 'value': 1}]}).encode()
        with patched_data(make_data_dir(self.tmp.name, {'p.json': body})), \
                running_source() as (base, _):
            client = SourceClient(base)
            client.admin_advance(phase=2)
            claim = client.claim('w1')
            collector = Collector(client, Store(self.db), worker='w1',
                                  fetch_retries=2, retry_pause=0.01)
            self.assertEqual(collector.process(claim), 'temp_failed')

            store = self.store()
            # 没有拒收记录，没有原件，来源侧投递未完结
            self.assertEqual(store.rejections(), [])
            self.assertEqual(store.stats()['originals'], 0)
            row = client.status()['deliveries'][0]
            self.assertFalse(row['done'])
            self.assertEqual(store.delivery(claim['delivery'])['state'],
                             'temp_failed')
            store.close()

            # 来源恢复、旧租约过期后：重新领取并正常完成
            client.admin_advance(seconds=5, phase=0)
            drain(base, self.db, worker='w2')
            store = self.store()
            self.assertEqual(store.delivery(claim['delivery'])['state'], 'acked')
            self.assertEqual(store.stats()['fragments'], 1)
            store.close()


class BadDeliveries(CollectorCase):
    def test_transport_digest_mismatch_is_rejected_with_original_kept(self):
        body = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                           'kind': 'part', 'part': 1,
                           'readings': [{'time': 't', 'value': 2}]}).encode()
        data = make_data_dir(self.tmp.name, {'p.json': body})
        # 申报一个错误的运输摘要
        rows = json.loads((Path(data) / 'deliveries.json').read_text())
        rows[0]['sha256'] = '00' * 32
        (Path(data) / 'deliveries.json').write_text(json.dumps(rows))
        with patched_data(data), running_source() as (base, _):
            drain(base, self.db)
            store = self.store()
            delivery = store.delivery('P')
            self.assertEqual(delivery['state'], 'rejected')
            self.assertIn('运输摘要不符', delivery['reason'])
            # 原件保留可回查，处置依据保留
            self.assertEqual(bytes(store.original(sha(body))['bytes']), body)
            self.assertEqual(store.rejections('P')[0]['reason'], delivery['reason'])
            # 内容摘要不可信，不登记为观测
            self.assertEqual(store.stats()['fragments'], 0)
            store.close()

    def test_envelope_validation(self):
        good_part = {'device': 'D', 'boot': 'b', 'batch': 1, 'kind': 'part',
                     'part': 2, 'readings': [{'time': 't', 'value': 0}]}
        kind, info = validate_envelope(good_part)
        self.assertEqual((kind, info['part']), ('part', 2))
        good_manifest = {'device': 'D', 'boot': 'b', 'batch': 1,
                         'kind': 'manifest', 'parts': {'1': 'a' * 64}}
        kind, info = validate_envelope(good_manifest)
        self.assertEqual((kind, info['parts']), ('manifest', {1: 'a' * 64}))
        for bad in ({'device': 'D'},                                  # 缺字段
                    {'device': 'D', 'boot': 'b', 'batch': 1, 'kind': 'x'},
                    {'device': 'D', 'boot': 'b', 'batch': True, 'kind': 'part',
                     'part': 1, 'readings': []},                      # batch 不是整数
                    {'device': 'D', 'boot': 'b', 'batch': 1, 'kind': 'part',
                     'part': 1, 'readings': [{'time': 't', 'value': True}]},
                    {'device': 'D', 'boot': 'b', 'batch': 1, 'kind': 'manifest',
                     'parts': {'1': 'not-a-digest'}}):
            with self.assertRaises(EnvelopeError, msg=repr(bad)):
                validate_envelope(bad)


class MissingFragments(CollectorCase):
    """缺片批次只报进度不提供读数；完整批次的 0 值读数照常给出。"""

    def test_missing_part_and_zero_value(self):
        def part(value):
            return json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                               'kind': 'part', 'part': value,
                               'readings': [{'time': 't', 'value': value}]}).encode()
        p1, p2 = part(1), part(2)
        ghost = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                            'kind': 'part', 'part': 3,
                            'readings': [{'time': 't', 'value': 3}]}).encode()
        manifest = json.dumps({'device': 'D1', 'boot': 'b1', 'batch': 1,
                               'kind': 'manifest',
                               'parts': {'1': sha(p1), '2': sha(p2),
                                         '3': sha(ghost)}}).encode()
        zero = json.dumps({'device': 'D2', 'boot': 'b1', 'batch': 1,
                           'kind': 'part', 'part': 1,
                           'readings': [{'time': 't', 'value': 0}]}).encode()
        zero_manifest = json.dumps({'device': 'D2', 'boot': 'b1', 'batch': 1,
                                    'kind': 'manifest',
                                    'parts': {'1': sha(zero)}}).encode()
        data = make_data_dir(self.tmp.name, {
            'm.json': manifest, 'p1.json': p1, 'p2.json': p2,
            'zm.json': zero_manifest, 'z.json': zero})
        with patched_data(data), running_source() as (base, _):
            drain(base, self.db)
            store = self.store()
            # 缺第 3 片：进度 2/3，拒绝提供读数，缺失就是缺失
            detail = assemble.batch_detail(store, 'D1', 'b1', 1)
            self.assertEqual(detail['state'], 'incomplete')
            self.assertEqual(detail['progress']['obtained_parts'], 2)
            self.assertEqual(detail['progress']['missing_parts'], [3])
            result, refused = assemble.readings(store, 'D1', 'b1', 1)
            self.assertIsNone(result)
            self.assertIn('缺少分片 [3]', refused['summary'])
            # 完整批次：0 值读数是真实数据，原样给出
            result, _ = assemble.readings(store, 'D2', 'b1', 1)
            self.assertIsNotNone(result)
            self.assertEqual([r['value'] for r in result['readings']], [0])
            self.assertEqual(result['provenance']['1']['deliveries'], ['Z'])
            store.close()


class ReadApi(CollectorCase):
    def _get(self, port, path):
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
        conn.request('GET', path)
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        return resp.status, body

    def test_status_service(self):
        with running_source() as (base, _):
            drain(base, self.db)
            store = self.store()
            server = serve(store, 0, source=base)
            import threading
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                port = server.server_port
                status, body = self._get(port, '/batches')
                self.assertEqual(status, 200)
                board = json.loads(body)
                self.assertEqual(len(board), 2)
                # 冲突批次拒绝读数并说明原因
                status, body = self._get(
                    port, '/batches/D01/boot-a/1/readings')
                self.assertEqual(status, 409)
                self.assertIn('冲突', json.loads(body)['reason'])
                # 投递解释：T-7 的拒收依据
                status, body = self._get(port, '/deliveries/T-7')
                self.assertEqual(status, 200)
                info = json.loads(body)
                self.assertIn('拒收', info['why_redelivered'])
                self.assertEqual(info['source']['disposition'], 'rejected')
                # 原件回查
                bad_sha = sha((DATA / 'payloads/bad.json').read_bytes())
                status, body = self._get(port, '/originals/%s' % bad_sha)
                self.assertEqual(status, 200)
                self.assertEqual(body, (DATA / 'payloads/bad.json').read_bytes())
            finally:
                server.shutdown()
                server.server_close()
                thread.join()
                store.close()


if __name__ == '__main__':
    unittest.main()
