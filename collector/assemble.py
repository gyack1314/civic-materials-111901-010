"""批次观测读模型：把已保存的原件组装成业务批次视图。

业务身份是 device/boot/batch/part（设备重启后批次序号会复用，靠启动
代际 boot 区分）；投递编号只是运输。只有声明与全部分片一致才提供读数；
同一分片号的不同字节保留为冲突，不按到达先后裁决；缺片只报进度，
不补零。
"""
import json

# 分片状态
PART_OK = 'ok'                      # 唯一候选且与声明摘要一致
PART_MISSING = 'missing'            # 声明了但尚未取得
PART_CONFLICT = 'conflict'          # 同一分片号出现不同字节
PART_DIGEST_MISMATCH = 'digest_mismatch'  # 有候选但都与声明摘要不符
PART_UNDECLARED = 'undeclared'      # 声明之外收到的分片
PART_UNVERIFIABLE = 'unverifiable'  # 已取得但声明缺失，暂无法核对

# 批次状态
BATCH_SERVABLE = 'servable'         # 声明与全部分片一致，可提供读数
BATCH_INCOMPLETE = 'incomplete'     # 缺声明或缺分片
BATCH_CONFLICTED = 'conflicted'     # 存在冲突或不一致


def _candidate_view(store, frag, declared_digest):
    payload = json.loads(frag['payload_json'])
    return {
        'content_sha256': frag['content_sha256'],
        'matches_manifest': (declared_digest is not None
                             and frag['content_sha256'] == declared_digest),
        'deliveries': store.deliveries_for_content(frag['content_sha256']),
        'first_delivery': frag['first_delivery'],
        'readings': payload.get('readings', []),
    }


def _part_view(store, part_no, declared_digest, candidates, has_manifest):
    view = {'part': part_no, 'declared_digest': declared_digest,
            'candidates': [_candidate_view(store, f, declared_digest) for f in candidates]}
    if not candidates:
        view['state'] = PART_MISSING
    elif not has_manifest:
        # 声明本身缺失：分片已保存但无法核对，批次属于不完整而非冲突
        view['state'] = PART_UNVERIFIABLE
    elif declared_digest is None:
        # 有唯一声明但该分片不在其中：声明与分片不一致
        view['state'] = PART_UNDECLARED
    elif len(candidates) > 1:
        # 同一业务身份出现不同字节：保留冲突，不按到达先后裁决
        view['state'] = PART_CONFLICT
    elif candidates[0]['content_sha256'] == declared_digest:
        view['state'] = PART_OK
    else:
        view['state'] = PART_DIGEST_MISMATCH
    return view


def batch_detail(store, device, boot, batch):
    """单个业务批次的完整视图：声明、各分片状态、冲突与进度。"""
    manifests = store.manifests_of(device, boot, batch)
    fragments = store.fragments_of(device, boot, batch)
    by_part = {}
    for frag in fragments:
        by_part.setdefault(frag['part'], []).append(frag)

    manifest_conflict = len(manifests) > 1
    declared = {}
    if len(manifests) == 1:
        declared = {int(k): v for k, v in
                    json.loads(manifests[0]['parts_json']).items()}

    parts = []
    for part_no in sorted(set(declared) | set(by_part)):
        declared_digest = declared.get(part_no) if declared else None
        parts.append(_part_view(store, part_no, declared_digest,
                                by_part.get(part_no, []),
                                has_manifest=bool(declared)))

    states = {p['state'] for p in parts}
    if manifest_conflict or states & {PART_CONFLICT, PART_DIGEST_MISMATCH, PART_UNDECLARED}:
        state = BATCH_CONFLICTED
    elif not manifests or PART_MISSING in states:
        state = BATCH_INCOMPLETE
    else:
        state = BATCH_SERVABLE

    declared_count = len(declared)
    obtained = sum(1 for p in parts
                   if p['part'] in declared and p['candidates'])
    detail = {
        'device': device, 'boot': boot, 'batch': batch, 'state': state,
        'progress': {'declared_parts': declared_count, 'obtained_parts': obtained,
                     'unverifiable_parts': sum(
                         1 for p in parts if p['state'] == PART_UNVERIFIABLE),
                     'missing_parts': [p['part'] for p in parts
                                       if p['state'] == PART_MISSING]},
        'manifests': [
            {'content_sha256': m['content_sha256'],
             'declared_parts': {int(k): v for k, v in
                                json.loads(m['parts_json']).items()},
             'deliveries': store.deliveries_for_content(m['content_sha256'])}
            for m in manifests],
        'manifest_conflict': manifest_conflict,
        'parts': parts,
    }
    detail['summary'] = _summary(detail)
    return detail


def _summary(detail):
    progress = detail['progress']
    base = '分片进度 %d/%d' % (progress['obtained_parts'], progress['declared_parts'])
    if detail['state'] == BATCH_SERVABLE:
        return '批次完整一致，可提供读数（%s）' % base
    if detail['state'] == BATCH_INCOMPLETE:
        if not detail['manifests']:
            return '批次不完整：尚未取得批次声明（已取得 %d 个分片，待声明核对）' % (
                progress['unverifiable_parts'],)
        return '批次不完整：缺少分片 %s（%s）' % (progress['missing_parts'], base)
    reasons = []
    if detail['manifest_conflict']:
        reasons.append('批次声明存在多个版本')
    for part in detail['parts']:
        if part['state'] == PART_CONFLICT:
            reasons.append('分片 %d 存在冲突字节（%d 个候选）'
                           % (part['part'], len(part['candidates'])))
        elif part['state'] == PART_DIGEST_MISMATCH:
            reasons.append('分片 %d 的内容摘要与声明不符' % part['part'])
        elif part['state'] == PART_UNDECLARED:
            reasons.append('分片 %d 不在批次声明中' % part['part'])
    return '批次存在不一致：' + '；'.join(reasons)


def batch_board(store):
    """所有已知业务批次的概览。"""
    return [batch_detail(store, device, boot, batch)
            for device, boot, batch in store.batch_keys()]


def readings(store, device, boot, batch):
    """仅在批次完整一致时返回读数；否则返回 (None, 说明)。

    缺片就是缺片，不会补零；声明中的 0 值读数照常出现在结果里。
    """
    detail = batch_detail(store, device, boot, batch)
    if detail['state'] != BATCH_SERVABLE:
        return None, detail
    ordered = []
    provenance = {}
    for part in detail['parts']:
        candidate = part['candidates'][0]
        ordered.extend(candidate['readings'])
        provenance[str(part['part'])] = {
            'content_sha256': candidate['content_sha256'],
            'deliveries': candidate['deliveries']}
    return {'device': device, 'boot': boot, 'batch': batch,
            'readings': ordered, 'provenance': provenance}, detail


def explain_delivery(store, delivery, source_row=None):
    """解释一条投递的本地处理轨迹，以及来源为什么还在重发它。

    source_row 是来源 /status 中该投递的当前行（可选）。
    """
    local = store.delivery(delivery)
    events = store.delivery_events(delivery)
    facts = []
    for ev in events:
        detail = json.loads(ev['detail']) if ev['detail'] else None
        facts.append({'event': ev['event'], 'worker': ev['worker'],
                      'time': ev['local_time'], 'detail': detail})

    if source_row is not None:
        source_done = source_row.get('done')
        disposition = source_row.get('disposition')
    else:
        source_done = disposition = None

    if source_done and disposition == 'accepted':
        why = '来源已收到有效确认，不再重发。'
    elif source_done and disposition == 'rejected':
        why = '来源已收到拒收（坏件处置），不再重发。'
    elif local is None:
        why = '本端没有该投递的处理记录；来源未收到确认，会继续重发。'
    elif local['state'] in ('saved', 'lease_lost'):
        why = ('原件已保存（内容摘要 %s），但确认未生效：租约已失效或被其他进程持有。'
               '来源未收到有效确认，会继续重发；再次领取后可直接确认，'
               '已保存的原件会去重复用，不会删除，也不会产生第二份观测。'
               % (local['content_sha256'] or '?'))
    elif local['state'] == 'temp_failed':
        why = ('上次读取载荷时来源临时失败，未确认也未拒收；'
               '来源会继续重发，稍后重新领取即可。')
    elif local['state'] == 'claimed':
        why = ('已领取但尚未保存原件（进程可能已停止领取）；'
               '租约到期后来源会重新投递，未保存的工作可再次取得。')
    elif local['state'] == 'closed_elsewhere':
        why = '确认由其他持有者完成；本地保存的原件保留用于核对。'
    elif local['state'] in ('acked', 'rejected'):
        why = ('本端记录已%s；若来源仍显示未完结，请以来源 /status 为准核对。'
               % ('确认' if local['state'] == 'acked' else '拒收'))
    else:
        why = '状态 %s，详见事件。' % local['state']

    return {'delivery': delivery, 'local': local, 'events': facts,
            'source': source_row, 'why_redelivered': why}
