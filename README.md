# big-data-quantative-stock
## 项目架构
![alt text](imgs/stock_platform_components.png)

### 秒级行情多因子实时计算流程图
![alt text](imgs/stock_analysis_process.png)

### 推特实时情感分析架构图
![alt text](imgs/tick_architecture.png)

逻辑回归与`Spark Streaming`的交互
![alt text](imgs/lr_pipeline.png)

数据清洗流水线具体如下
![alt text](imgs/ml_pipeline.png)

## 使用说明
1. 创建一个`.env`文件并在其中添加`mongodb`连接的字符串
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

## 模块说明
### 数据下载器 `data_downloader`
```python
from data_downloader.downloader import Downloader

downloader = Downloader("stock_data", "all_stocks_ticks")
downloader.download_data(["000001.SZ"], "tick", "20240414", "20240420")
data = downloader.get_local_data([], ["000001.SZ"], "tick", "20240418", "20240420", -1, "none", True, "")
downloader.store_data(data)
```

### Kafka组件 `kafka_component`
#### 生产者`producer`
调用方法详见`produce.ipynb`

#### 消费者`consumer`
调用方法详见`consume.ipynb`

### 数据库连接器`mongodb_connector`
```python
from mongodb_connector.connector import MongoDBConnector

connector = MongoDBConnector(db_name="stock_data", collection_name="600036.SH")

# 查询股票行情
query = {'stock_code': '600036.SH'}
result = connector.get_all_data(query=query)
df = pd.DataFrame(list(result))
```

## Twitter Data Mocker解析
由于Twitter的API接口不是免费的，此项目使用GPT生成数据进行模拟，下面是通过 tweet API 获得的 json 格式的数据：
![alt text](imgs/tweet_structure.png)
`data_mocker`模块将模拟生成此格式数据
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

## `OBI`和`MicroPrice`因子实时计算
![alt text](imgs/factors.png)

计算完多个因子后，用户可以自定义每个因子的买卖阈值，进而产生买卖信号

![alt text](imgs/visualization.png)

## 推特实时消息分析结果
`train_data.csv`是一个用于训练逻辑回归的训练集，用户可以以同样的格式将其替换为更大的训练集

![alt text](imgs/twitter_analysis.png)