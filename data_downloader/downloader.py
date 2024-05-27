from xtquant import xtdata
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv
import os
from loguru import logger

load_dotenv()
CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")

class Downloader():
    def __init__(self, db_name, collection_name):
        self.client = MongoClient(CONNECTION_STRING)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        # 获取沪深A股的股票列表
        self.all_stock_list = xtdata.get_stock_list_in_sector('沪深A股')

    # 下载全部股票数据
    def download_all_data(self, period, start_time, end_time):
        self.download_data(self.all_stock_list, period, start_time, end_time)
        logger.info("全部数据下载完毕")

    # 下载列表形式数据
    def download_data(self, stock_list, period, start_time, end_time):
        for i in tqdm(stock_list, desc="下载行情数据"):
            xtdata.download_history_data(i, period=period, start_time=start_time, end_time=end_time, incrementally=True)
            logger.info("数据下载完毕: {}".format(i))

    # 获取数据
    def get_local_data(self, field_list, stock_list, period, start_time, end_time, count, dividend_type, fill_data, data_dir):
        data = xtdata.get_local_data(field_list=field_list, stock_list=stock_list, period=period, start_time=start_time, \
            end_time=end_time, count=count, dividend_type=dividend_type, fill_data=fill_data, data_dir=data_dir)
        return data

    # 存储数据
    def store_data(self, data):
        if isinstance(data, dict):
            for stock_code, df in tqdm(data.items(), desc="处理股票数据"):
                if not df.empty:
                    df['stock_code'] = stock_code
                    ticks = df.to_dict('records')
                    self.collection.insert_many(ticks)
        logger.info("数据存储完毕: {}".format(data.keys()))