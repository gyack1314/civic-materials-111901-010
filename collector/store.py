"""采集端持久存储。

多进程共用一个 SQLite 文件（WAL 模式），每条投递的保存是单事务，
内容哈希去重：同一字节内容无论被多少投递、多少进程保存，只有一份
原件、一条观测。事件表只追加，是解释来源行为的依据。
"""
import json
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS originals (
  content_sha256 TEXT PRIMARY KEY,
  payload_path   TEXT NOT NULL,
  bytes          BLOB NOT NULL,
  size           INTEGER NOT NULL,
  first_delivery TEXT NOT NULL,
  saved_by       TEXT NOT NULL,
  saved_at       REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS fragments (
  device         TEXT NOT NULL,
  boot           TEXT NOT NULL,
  batch          INTEGER NOT NULL,
  part           INTEGER NOT NULL,
  content_sha256 TEXT NOT NULL REFERENCES originals(content_sha256),
  payload_json   TEXT NOT NULL,
  first_delivery TEXT NOT NULL,
  seen_by        TEXT NOT NULL,
  seen_at        REAL NOT NULL,
  PRIMARY KEY (device, boot, batch, part, content_sha256)
);
CREATE TABLE IF NOT EXISTS manifests (
  device         TEXT NOT NULL,
  boot           TEXT NOT NULL,
  batch          INTEGER NOT NULL,
  content_sha256 TEXT NOT NULL REFERENCES originals(content_sha256),
  parts_json     TEXT NOT NULL,
  first_delivery TEXT NOT NULL,
  seen_by        TEXT NOT NULL,
  seen_at        REAL NOT NULL,
  PRIMARY KEY (device, boot, batch, content_sha256)
);
CREATE TABLE IF NOT EXISTS deliveries (
  delivery       TEXT PRIMARY KEY,
  payload_path   TEXT,
  claim_sha256   TEXT,
  state          TEXT NOT NULL,
  content_sha256 TEXT,
  last_worker    TEXT,
  last_lease     TEXT,
  reason         TEXT,
  updated_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS rejections (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery       TEXT NOT NULL,
  content_sha256 TEXT,
  reason         TEXT NOT NULL,
  worker         TEXT NOT NULL,
  confirmed      INTEGER NOT NULL,
  created_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery    TEXT,
  worker      TEXT,
  event       TEXT NOT NULL,
  detail      TEXT,
  local_time  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_delivery ON events(delivery);
CREATE INDEX IF NOT EXISTS idx_deliveries_content ON deliveries(content_sha256);
"""


class Store:
    """线程安全、跨进程安全的采集存储。"""

    def __init__(self, path):
        self.path = str(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=FULL')
            self.conn.execute('PRAGMA busy_timeout=30000')
            self.conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self.conn.close()

    # ---- 写入 ----
    def _event(self, cur, delivery, worker, event, detail=None):
        cur.execute(
            'INSERT INTO events(delivery,worker,event,detail,local_time) VALUES(?,?,?,?,?)',
            (delivery, worker, event,
             json.dumps(detail, ensure_ascii=False) if detail is not None else None,
             time.time()))

    def _touch(self, cur, delivery, **fields):
        fields['updated_at'] = time.time()
        cols = ','.join('%s=?' % k for k in fields)
        cur.execute('UPDATE deliveries SET %s WHERE delivery=?' % cols,
                    (*fields.values(), delivery))

    def record_claim(self, delivery, payload_path, claim_sha256, worker, lease, until):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            cur.execute(
                'INSERT INTO deliveries(delivery,payload_path,claim_sha256,state,last_worker,last_lease,updated_at)'
                ' VALUES(?,?,?,?,?,?,?)'
                ' ON CONFLICT(delivery) DO UPDATE SET payload_path=excluded.payload_path,'
                ' claim_sha256=excluded.claim_sha256,state=excluded.state,'
                ' last_worker=excluded.last_worker,last_lease=excluded.last_lease,'
                ' updated_at=excluded.updated_at',
                (delivery, payload_path, claim_sha256, 'claimed', worker, lease, time.time()))
            self._event(cur, delivery, worker, 'claimed',
                        {'lease': lease, 'until': until, 'payload': payload_path,
                         'sha256': claim_sha256})

    def save_original(self, delivery, payload_path, data, content_sha256, worker):
        """保存原件字节；返回 True 表示新保存，False 表示内容已存在（去重）。"""
        with self._lock, self.conn:
            cur = self.conn.cursor()
            cur.execute(
                'INSERT OR IGNORE INTO originals'
                '(content_sha256,payload_path,bytes,size,first_delivery,saved_by,saved_at)'
                ' VALUES(?,?,?,?,?,?,?)',
                (content_sha256, payload_path, data, len(data), delivery, worker, time.time()))
            inserted = cur.rowcount > 0
            self._touch(cur, delivery, state='saved', content_sha256=content_sha256)
            self._event(cur, delivery, worker, 'saved' if inserted else 'saved_deduped',
                        {'content_sha256': content_sha256, 'size': len(data)})
            return inserted

    def record_fragment(self, device, boot, batch, part, content_sha256, payload, delivery, worker):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            cur.execute(
                'INSERT OR IGNORE INTO fragments'
                '(device,boot,batch,part,content_sha256,payload_json,first_delivery,seen_by,seen_at)'
                ' VALUES(?,?,?,?,?,?,?,?,?)',
                (device, boot, batch, part, content_sha256,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True),
                 delivery, worker, time.time()))
            self._event(cur, delivery, worker,
                        'fragment_recorded' if cur.rowcount else 'fragment_deduped',
                        {'device': device, 'boot': boot, 'batch': batch, 'part': part,
                         'content_sha256': content_sha256})

    def record_manifest(self, device, boot, batch, content_sha256, parts, delivery, worker):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            cur.execute(
                'INSERT OR IGNORE INTO manifests'
                '(device,boot,batch,content_sha256,parts_json,first_delivery,seen_by,seen_at)'
                ' VALUES(?,?,?,?,?,?,?,?)',
                (device, boot, batch, content_sha256,
                 json.dumps(parts, ensure_ascii=False, sort_keys=True),
                 delivery, worker, time.time()))
            self._event(cur, delivery, worker,
                        'manifest_recorded' if cur.rowcount else 'manifest_deduped',
                        {'device': device, 'boot': boot, 'batch': batch,
                         'content_sha256': content_sha256, 'parts': parts})

    def record_ack(self, delivery, worker, lease, source_reply):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            self._touch(cur, delivery, state='acked')
            self._event(cur, delivery, worker, 'ack_ok',
                        {'lease': lease, 'source': source_reply})

    def record_reject(self, delivery, worker, lease, reason, content_sha256, source_reply):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            cur.execute(
                'INSERT INTO rejections(delivery,content_sha256,reason,worker,confirmed,created_at)'
                ' VALUES(?,?,?,?,1,?)', (delivery, content_sha256, reason, worker, time.time()))
            self._touch(cur, delivery, state='rejected', reason=reason)
            self._event(cur, delivery, worker, 'reject_ok',
                        {'lease': lease, 'reason': reason, 'source': source_reply})

    def record_lease_lost(self, delivery, worker, action, done, lease, reason=None):
        """ack/reject/renew 被来源以 409 拒绝：租约已失效，以来源响应为准。"""
        with self._lock, self.conn:
            cur = self.conn.cursor()
            state = 'closed_elsewhere' if done else 'lease_lost'
            if reason is not None:
                cur.execute(
                    'INSERT INTO rejections(delivery,content_sha256,reason,worker,confirmed,created_at)'
                    ' VALUES(?,(SELECT content_sha256 FROM deliveries WHERE delivery=?),?,?,0,?)',
                    (delivery, delivery, reason, worker, time.time()))
                self._touch(cur, delivery, state=state, reason=reason)
            else:
                self._touch(cur, delivery, state=state)
            self._event(cur, delivery, worker, '%s_lease_lost' % action,
                        {'lease': lease, 'done': done, 'reason': reason})

    def record_temp_failure(self, delivery, worker, detail):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            self._touch(cur, delivery, state='temp_failed')
            self._event(cur, delivery, worker, 'temp_failure', {'error': detail})

    def record_renew(self, delivery, worker, lease, source_reply):
        with self._lock, self.conn:
            cur = self.conn.cursor()
            self._event(cur, delivery, worker, 'renew_ok',
                        {'lease': lease, 'source': source_reply})

    # ---- 查询 ----
    def _rows(self, sql, args=()):
        with self._lock:
            return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def _row(self, sql, args=()):
        rows = self._rows(sql, args)
        return rows[0] if rows else None

    def manifests_of(self, device, boot, batch):
        return self._rows(
            'SELECT * FROM manifests WHERE device=? AND boot=? AND batch=?',
            (device, boot, batch))

    def fragments_of(self, device, boot, batch):
        return self._rows(
            'SELECT * FROM fragments WHERE device=? AND boot=? AND batch=? ORDER BY part',
            (device, boot, batch))

    def batch_keys(self):
        keys = set()
        for row in self._rows('SELECT device,boot,batch FROM manifests'):
            keys.add((row['device'], row['boot'], row['batch']))
        for row in self._rows('SELECT device,boot,batch FROM fragments'):
            keys.add((row['device'], row['boot'], row['batch']))
        return sorted(keys)

    def deliveries_for_content(self, content_sha256):
        return [r['delivery'] for r in self._rows(
            'SELECT delivery FROM deliveries WHERE content_sha256=? ORDER BY delivery',
            (content_sha256,))]

    def delivery(self, delivery):
        return self._row('SELECT * FROM deliveries WHERE delivery=?', (delivery,))

    def deliveries(self, worker=None):
        if worker:
            return self._rows(
                'SELECT * FROM deliveries WHERE last_worker=? ORDER BY delivery', (worker,))
        return self._rows('SELECT * FROM deliveries ORDER BY delivery')

    def delivery_events(self, delivery):
        return self._rows('SELECT * FROM events WHERE delivery=? ORDER BY id', (delivery,))

    def rejections(self, delivery=None):
        if delivery:
            return self._rows(
                'SELECT * FROM rejections WHERE delivery=? ORDER BY id', (delivery,))
        return self._rows('SELECT * FROM rejections ORDER BY id')

    def original(self, content_sha256):
        return self._row(
            'SELECT content_sha256,payload_path,bytes,size,first_delivery,saved_by,saved_at'
            ' FROM originals WHERE content_sha256=?', (content_sha256,))

    def originals(self):
        return self._rows(
            'SELECT content_sha256,payload_path,size,first_delivery,saved_by,saved_at'
            ' FROM originals ORDER BY saved_at')

    def stats(self):
        with self._lock:
            count = lambda t: self.conn.execute('SELECT COUNT(*) FROM %s' % t).fetchone()[0]
            return {'originals': count('originals'), 'fragments': count('fragments'),
                    'manifests': count('manifests'), 'deliveries': count('deliveries'),
                    'rejections': count('rejections'), 'events': count('events')}
