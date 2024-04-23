from xtquant import xtdata
import time
from pymongo import MongoClient
from tqdm import tqdm  # 引入tqdm

all_stock_list = xtdata.get_stock_list_in_sector('沪深A股')
period = "tick" # 示例周期
# 使用tqdm包装循环，显示进度条
for i in tqdm(all_stock_list, desc="下载行情数据"):
    xtdata.download_history_data(i, period=period, start_time='20240414', end_time='20240420', incrementally=True)

# 更新MongoDB连接字符串，连接到云数据库
mongo_uri = ""#输入mongodb的连接字符串
client = MongoClient(mongo_uri)
db = client['stock_data']
collection = db['all_stocks_ticks']

# 获取沪深A股的股票列表
all_stock_list = xtdata.get_stock_list_in_sector('沪深A股')

# 调用get_local_data获取所有股票的数据,data_dir为存储数据的文件夹路径，得看上面下载存到了哪里
data =  xtdata.get_local_data(field_list=[], stock_list=all_stock_list, period='tick', start_time='20240418', \
    end_time='20240420',count=-1,dividend_type='none', fill_data=True,data_dir = "" )

# 检查返回的数据结构是否是字典，并且键为股票代码
if isinstance(data, dict):
    for stock_code, df in tqdm(data.items(), desc="处理股票数据"):
        if not df.empty:
            df['stock_code'] = stock_code
            ticks = df.to_dict('records')
            collection.insert_many(ticks)

print("数据下载并存储完毕！")
