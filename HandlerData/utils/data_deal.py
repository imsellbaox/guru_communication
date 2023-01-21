import json
import threading
import traceback

from HandlerData.utils.data_clean import data_clearn


class data_deal:
    @staticmethod
    def get_near_indextime(dict0):
        newtime = dict0["data"][0]["timestamp"]
        newtime = int(newtime) + 1
        return newtime

    @staticmethod
    def totalnotzero(dict0):
        total = dict0["total"]
        data  = dict0["data"]
        if int(total) == 0:
            return False
        if len(data) == 0:
            return False
        return True

        # 参数名必须是data ， 其实是消费者的body
    @staticmethod
    def handle_data(data:str,symbol,table_name,thread:threading.Thread,**kwargs):
        try:
            dict0 = json.loads(data)
            if data_deal.totalnotzero(dict0):
                newtime = data_deal.get_near_indextime(dict0)
                thread.indextime = newtime
                # 数据清理
                df = data_clearn.resp_to_df(dict0)
                # 数据存入
                thread.dbtool.write_symbol_data(df, symbol=symbol, table_name=table_name)

        except Exception as e:
            traceback.print_exc()
