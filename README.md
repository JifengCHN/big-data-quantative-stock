# big-data-quantative-stock
## 项目架构

## 环境配置方法
1. 在`.env`中添加`mongodb`连接的字符串
    ```
    COSMOS_CONNECTION_STRING="mongodb+srv://fernando:Zz12345678@stockanalysis.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
    OPENAI_API_KEY="sk-"
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
from data_downloader.downloader import Downloader

downloader = Downloader("stock_data", "all_stocks_ticks")
downloader.download_data(["000001.SZ"], "tick", "20240414", "20240420")
data = downloader.get_local_data([], ["000001.SZ"], "tick", "20240418", "20240420", -1, "none", True, "")
downloader.store_data(data)
```

### data_mocker
```python
from data_mocker.mocker import Mocker

```

### data_retriever
```python
from data_retriever.retriever import Retriever

retriever = Retriever(db_name="stock_data", collection_name="600036.SH")
query = {'stock_code': '600036.SH'}
result = retriever.get_all_data(query=query)
df = pd.DataFrame(list(result))
```

## Tweeter Data Mocker解析
![alt text](./imgs/image.png)
下面是通过 tweet API 获得的 json 格式的数据，data_mocker模块将模拟生成此格式数据
```json
[
    {
        "data": {
            "edit_history_tweet_ids": ["1653119752024236047"],
            "id": "1653119752024236047",
            "text": "@drkjeffery: ChatGPT just makes up garbage ... I'm slightly struggling to find a use for it ot ..."
        },
        "matching_rules": [
            {
                "id": "1653064881782894592",
                "tag": ""
            }
        ]
    },
    ...
]
```
其中，`edit_history_tweet_ids` 这个字段包含一个推特的历史编辑ID的列表。每当推特被编辑或修改时，系统会生成一个新的ID，并将其记录在这个列表中。这个列表可以帮助追踪某个推特的编辑历史；`matching_rules` 这个字段包含一个匹配规则的列表，这些规则被用来标识和分类推特。每条规则有一个唯一的ID和一个可选的标签（tag）。这些规则可能由用户或系统定义，用于过滤、分类或标记推特数据。