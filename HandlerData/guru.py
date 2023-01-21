import time
import threading
import requests
import json
import traceback
from HandlerData.utils.data_cache import dataCache, cacheFunc
from HandlerData.utils.data_deal import data_deal
from HandlerData.utils.ddb_tool import ddbtool


class guru:
    def __init__(self,url:str,api_address:str,api_key:str,chain_id:int=None,amm:str=None,token_address:str=None,wallet_address:str=None):
        self.url = url
        self.api_address = api_address
        self.api_key = api_key
        self.chain_id = chain_id
        self.amm = amm
        self.token_address = token_address
        self.wallet_address = wallet_address
        self.dbtool = ddbtool()
        self.api = self.url_spli()
        self.i = 1

    def deal_DataThread(self,que_name,func,symbol,table_name):
        self.c = cacheFunc()
        try:
            threading.Thread(target=self.c.channel_do_func,kwargs={"que_name":que_name, "func":func,"symbol":symbol,"table_name":table_name,"thread":self}).start()
        except Exception as e:
            traceback.print_exc()

    def url_spli(self):
        if self.chain_id is not None:
            self.api_address = self.api_address.replace("{chain_id}",str(self.chain_id))
        if self.amm is not None:
            self.api_address = self.api_address.replace("{amm}", self.amm)
        if self.token_address is not None:
            self.api_address = self.api_address.replace("{token_address}", self.token_address)
        if self.wallet_address is not None:
            self.api_address = self.api_address.replace("{wallet_address}", self.wallet_address)
        api = self.url + self.api_address
        return api

    def guru_request(self,**kwargs):
        payload = kwargs
        kwargs['api-key']= self.api_key
        req = requests.get(url=self.api, params=payload,timeout=5)
        return req.text

    def pull_start(self,token,symbol,net,token_address):
        self.deal_DataThread(str(int(time.time())-1), data_deal.handle_data, symbol, self.dbtool.guru_tikcer_data)
        try:
            t = univ3Thread_cache(self,token,symbol,net,token_address,self.dbtool)
            t.start()
        except Exception as e:
            print(e)
            traceback.print_exc()



class univ3Thread_cache(threading.Thread):
    def __init__(self, dex: guru, token: str, symbol: str,net:str,token_address:str,dbtool:ddbtool):
        super(univ3Thread_cache, self).__init__()
        self.guru = dex
        self.dbtool = dbtool
        self.token_address = token_address
        self.starttime = int(time.time()) - 1
        self.indextime = self.starttime
        self.net = net
        self.token = token
        self.symbol = symbol
        self.p = dataCache()
        self.stop_threads = False

    def run(self):
        while True:
            end = int(time.time())
            try:
                resp = self.guru.guru_request(limit=100, token_address=self.token_address,
                                              begin_timestamp=self.indextime, end_timestamp=end)
                print(resp)
                self.p.send_channel(que_name=str(self.starttime),routing_key=str(self.starttime),body=resp)
                print("%d————%d 正在获取币对数据：【%s】" % (self.indextime,end,self.symbol))
            except Exception as e:
                traceback.print_exc()
                print("触发重连")
            if self.stop_threads:
                break
            time.sleep(3)

    def colse_thread(self):
        self.stop_threads = True