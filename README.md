# 社区节能观测投递箱的租约收集与接收确认

这是围绕城市低碳公开资料的本地研发项目。当前内容提供授权合成来源、资料和来源契约；资料收集端尚未开发。来源不会替收集端作身份、覆盖或交付判断。

## 运行来源

使用 Python 3.11，先执行 python -m pip install -r requirements.txt。

启动命令：python -m source.server --port 8765。浏览器或 HTTP 客户端访问 http://127.0.0.1:8765。接口和资料定义见 docs/source-contract.md。

## 检查来源

执行 python -m unittest discover -s tests -v。这些检查验证本地来源的真实响应与素材可读性，不代表资料收集端已经实现。

## 容器

执行 docker build -t sensor-inbox-harvest .，然后 docker run --rm -p 8765:8765 sensor-inbox-harvest。测试可用 docker run --rm sensor-inbox-harvest python -m unittest discover -s tests -v。

source 提供来源服务，data 保存合成原始资料，tests 核对来源行为；docs 记录字段与交互契约。不要将运行日志、凭据或采集数据提交到版本库。
