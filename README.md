# 社区节能观测投递箱的租约收集与接收确认

这是围绕城市低碳公开资料的本地研发项目。当前内容提供授权合成来源、资料、来源契约，以及可多进程并行的资料收集端。来源不会替收集端作身份、覆盖或交付判断。

## 运行来源

使用 Python 3.11 或 3.12，先执行 python -m pip install -r requirements.txt。

启动命令：python -m source.server --port 8765。浏览器或 HTTP 客户端访问 http://127.0.0.1:8765。接口和资料定义见 docs/source-contract.md。

## 运行采集端

python -m collector collect --source http://127.0.0.1:8765 --store collector.db --worker w1 启动一个采集进程，可并行起多个共用同一存储。python -m collector serve --store collector.db --source http://127.0.0.1:8765 --port 8766 启动只读状态服务，查看批次状态、投递轨迹与保存的原件。设计与命令详见 docs/collector.md。

## 检查

执行 python -m unittest discover -s tests -v。这些检查验证本地来源的真实响应、素材可读性，以及采集端在多进程争用、租约过期、临时失败、坏件、缺片与冲突情形下的行为。

## 容器

执行 docker build -t sensor-inbox-harvest .，然后 docker run --rm -p 8765:8765 sensor-inbox-harvest。测试可用 docker run --rm sensor-inbox-harvest python -m unittest discover -s tests -v。

source 提供来源服务，data 保存合成原始资料，collector 实现采集端，tests 核对来源行为与采集行为；docs 记录字段、交互契约与采集端设计。不要将运行日志、凭据或采集数据提交到版本库。
