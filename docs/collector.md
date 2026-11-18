# 采集端设计与运行

采集端把运输接收与设备批次的可用观测连起来：每条投递在原件持久保存后
即被确认，业务批次的组装是独立的读模型，不阻塞确认。只依赖 Python
标准库，可多进程并行运行，共用同一 SQLite 存储（WAL）。

## 概念对应

| 契约概念 | 采集端处理 |
| --- | --- |
| 投递编号 delivery | 仅运输身份；重发换号不影响业务观测 |
| 业务身份 | device/boot/batch/part；设备重启复用批次号，靠启动代际 boot 区分 |
| 租约 lease/until | 是否有效以来源响应（200/409）为准，本地不按经过秒数推断 |
| 运输摘要 sha256 | 收到字节先核对，不符按坏件处置 |
| 内容摘要（manifest） | 与分片原件字节摘要比对，决定批次是否可提供读数 |

## 单条投递的处理顺序

1. `POST /claim` 领取，记录事件（lease、until、申报摘要）。
2. `GET payload` 取原件。临时失败（5xx/连接错误）不确认也不拒收，
   在租约仍有效时有界续约重试；仍失败则等租约失效后来源重发。
3. 核对运输摘要；不符 → 保存原件与处置依据后 `POST /reject`。
4. 解析并校验格式（device/boot/batch/kind 及 manifest/part 结构）；
   坏格式同样先留原件与依据再拒收。
5. 保存原件（按内容摘要去重）并登记 manifest/fragment，单事务完成。
6. `POST /ack` 确认。若 409：租约已失效，可能已被他人领取甚至完成；
   已保存的原件保留，不删除，也不会产生第二份观测。

确认发生在单条投递保存之后，不等整个批次拼完，迟到分片不会把租约
拖过期。每个进程一次只处理一条投递，积压留在来源端，内存占用有界；
需要吞吐就增加进程数。进程被减少或停止领取时：未保存的工作随租约
失效由来源重发，已保存未确认的工作由后来的进程在重发时继续核对。

## 批次观测（读模型）

- 只有同一业务批次的声明与全部分片一致（每片唯一候选且内容摘要与
  声明相符）才提供读数；缺片就是缺片，不补零，声明中的 0 值读数
  照常是数据。
- 同一分片号的不同字节保留为冲突，不按到达先后裁决成更正版；
  候选与声明摘要不符、声明之外的分片、多版本声明都使批次不可读。
- 不完整批次展示取得进度（已得/应得、缺哪些片、待声明核对几片）。

## 运行

```sh
python -m source.server --port 8765 &                 # 本地来源
python -m collector collect --source http://127.0.0.1:8765 \
    --store collector.db --worker w1                  # 可起多个进程
python -m collector serve --store collector.db \
    --source http://127.0.0.1:8765 --port 8766 &      # 只读状态服务
```

状态服务接口：`GET /batches`、`GET /batches/{device}/{boot}/{batch}`、
`GET .../readings`（不可提供时 409 并说明）、`GET /deliveries`、
`GET /deliveries/{id}`（解释来源为何仍在重发）、`GET /originals/{sha256}`。
同样的信息也可用 CLI 查看：`batches` / `batch` / `readings` /
`deliveries` / `delivery` / `original`。

采集进程参数：`--once` 领完即止；`--max-claims N` 限量后停止；
`--idle-sleep` 控制空闲等待。SIGINT/SIGTERM 在当前投递处理完后退出。

## 核对采集是否闭合

确认接口返回成功不等于采集闭合。核对方式：

- `python -m collector deliveries --store collector.db --source ...`
  并列本地状态与来源 /status：存在本地已保存而来源未完结的投递时，
  说明确认尚未生效，等重发后由任一进程完成。
- `python -m collector batches --store collector.db` 查看每个业务批次
  是完整、缺片还是冲突，以及冲突分片的候选内容摘要与来源投递。
- `python -m collector delivery --store collector.db --delivery T-5
  --source ...` 解释单条投递的处理轨迹与来源重发原因。

## 演练

`tests/test_collector.py` 覆盖：多进程争用同一来源、保存与确认之间
租约过期、phase=2 临时 503、坏格式与运输摘要不符的拒收、缺片与
0 值读数、设备重启复用批次号、只读状态服务。运行：

```sh
python -m unittest discover -s tests -v
```
