# big-data-quantative-stock
## 项目架构

## 环境配置方法
1. 在`.env`中添加`mongodb`连接的字符串
    ```
    COSMOS_CONNECTION_STRING="mongodb+srv://fernando:Zz12345678@stockanalysis.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
    ```
2. 打开`kafka`和`zookeeper`的`docker`
我们只需要一个带有单个 Zookeeper 和单个 Broker 的 Kafka 集群
    ```bash
    # 以分离模式启动 Docker Compose YAML 文件中定义的 Docker 服务
    docker-compose -f zk-single-kafka-single.yml up -d
    # 检查一切是否正常工作
    docker-compose -f zk-single-kafka-single.yml ps
    ```

    进入容器并检查 kafka 版本
    ```bash
    docker exec -it kafka1 /bin/bash
    kafka-topics --version
    ```

    创建一个连接 Consumer 和 Producer 的名为 stock 的 Topic
    ```bash
    kafka-topics --create --topic stock --bootstrap-server localhost:9092
    kafka-topics --describe --topic stock --bootstrap-server localhost:9092
    ```

## 模块调用说明
### data_downloader
```python
downloader = Downloader("stock_data", "all_stocks_ticks")
downloader.download_data(["000001.SZ"], "tick", "20240414", "20240420")
data = downloader.get_local_data([], ["000001.SZ"], "tick", "20240418", "20240420", -1, "none", True, "")
downloader.store_data(data)
```

### data_retriever
```python
from data_retriever.retriever import Retriever

retriever = Retriever(db_name="stock_data", collection_name="600036.SH")
query = {'stock_code': '600036.SH'}
result = retriever.get_all_data(query=query)
df = pd.DataFrame(list(result))
```