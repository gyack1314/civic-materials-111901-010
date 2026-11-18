# 本地来源契约

POST /claim 发送 worker 名称，返回 delivery、payload、sha256、lease、until；无可领取投递时 204。租约在来源时钟达到 until 时失效，领取时给出新 lease，序号并非业务版本。GET payload 获取原始 UTF-8 JSON 字节，sha256 为运输摘要。POST /renew、/ack、/reject 都需 delivery 与当前 lease，旧租约返回 409；拒收还需 reason。成功 ack/reject 后不再投递。GET /status 供操作员查看来源运输状态，不代表设备批次已经完整。
载荷声明 device、boot、batch、kind。kind=manifest 指明各分片号与内容摘要；kind=part 含 part 和 readings。分片内容摘要计算自原始 part 载荷完整字节，设备业务身份是 device/boot/batch/part。不同 delivery 可运输相同分片，相同业务身份也可能出现不同字节冲突，来源不代为裁决。样本同时包括重发、设备重启、缺片与非法 JSON，均为本地合成。
演练时钟可推进使租约过期，phase=2 模拟载荷临时 503；单条投递拒收与业务批次完整性是两个不同来源事实。领取数据总量有限，支持多进程共用同一来源。

演练来源由 Python 标准库 HTTP 服务启动，默认端口 8765。GET /health 可查询来源时间与阶段；POST /__admin/advance 接收 seconds、phase 推进本地情形；GET /__admin/log 回查实际请求。这些控制只供操作员调试来源，不能替代产品从正常入口取得资料。来源进程重启会清空其演练状态；采集端自己的保存目录由产品独立管理。
