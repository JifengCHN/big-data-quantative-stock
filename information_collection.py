from xtquant import xtdata
import time

from tqdm import tqdm  # 引入tqdm

all_stock_list = xtdata.get_stock_list_in_sector('沪深A股')
period = "tick" # 示例周期

# 使用tqdm包装循环，显示进度条
for i in tqdm(all_stock_list, desc="下载行情数据"):
    xtdata.download_history_data(i, period=period, start_time='20240414', end_time='20240420', incrementally=True)