import openai
import json
import uuid
import random
from datetime import datetime
from dotenv import load_dotenv
import os

class Mocker:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('OPENAI_API_KEY')
        openai.api_key = self.api_key

    def generate_tweets(self, search_term, n=5):
        prompt = (
            f"生成{n}条与'{search_term}'股票相关的推文，每条推文应具有对这只股票的评价性和情感偏向，"
            "推文内容应尽量像真实用户发布的，语言是中文或者英文，中文占多数。\n"
            "其中应包括正面和负面的评价。\n"
            "比如: 关键词是“平安银行”时：\n"
            "生成的正面推文可以是：‘平安银行太稳了，买了以后我基本不用盯盘！’\n"
            "生成的负面推文可以是：‘平安银行今天的表现不理想，我要割肉离场了呜呜呜！’\n"
            "生成的负面推文可以是：‘平安银行最近几天的股价一直在跌，我感觉很不安。’\n"
            "生成的负面推文可以是：‘平安银行的财报不如预期，我决定卖掉所有股票。’\n"
            "生成的正面推文可以是：‘平安银行的分红真不错，让我赚了不少。’\n"
            "请按上述格式生成推文。"
        )

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "你是一个生成推文的助手。"},
                {"role": "user", "content": prompt}
            ],
            max_tokens=200 * n  # Increase tokens to handle multiple tweets
        )

        tweets = response.choices[0].message['content'].strip().split('\n')
        tweet_json_list = []

        for i, tweet in enumerate(tweets):
            tweet_text = tweet.strip()
            if tweet_text and not tweet_text.isspace():  # Ensure tweet_text is not empty or whitespace only
                if '. ' in tweet_text:
                    tweet_text = tweet_text.split('. ', 1)[-1]

                tweet_id = str(uuid.uuid4())
                edit_history_tweet_ids = [tweet_id]
                matching_rule_id = str(random.randint(1000000000000000000, 9999999999999999999))
                timestamp = datetime.utcnow().isoformat() + "Z"

                tweet_json = {
                    "data": {
                        "edit_history_tweet_ids": edit_history_tweet_ids,
                        "id": tweet_id,
                        "text": tweet_text,
                        "created_at": timestamp
                    },
                    "matching_rules": [
                        {
                            "id": matching_rule_id,
                            "tag": search_term
                        }
                    ]
                }
                tweet_json_list.append(tweet_json)

        return tweet_json_list