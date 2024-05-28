import json
from bson import ObjectId

def json_serializer(data):
    """自定义JSON序列化函数，处理MongoDB特有的数据类型."""
    return json.dumps(data, default=json_util_default).encode('utf-8')

def json_util_default(obj):
    """将无法直接序列化的对象转换为可序列化的格式."""
    if isinstance(obj, ObjectId):
        return str(obj)  # 将ObjectId转换为字符串
    raise TypeError("不可序列化的对象类型：%s" % type(obj).__name__)