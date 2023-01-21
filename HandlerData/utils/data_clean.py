import pandas as pd
import time
import numpy as np

class data_clearn:

    @staticmethod
    def resp_to_df(resp):
        content = resp
        df = data_clearn.get_a_model()
        json_list = []
        for data in content["data"]:
            index = {'block_timestamp': np.datetime64(data["timestamp"],"s"),
              'create_timestamp': int(time.time()),
              'symbol': data["tokens_in"][0]["symbol"] + "_" + data["tokens_out"][0]["symbol"],
              'net': data["tokens_in"][0]["network"],
              'amm':data["amm"],
              'chain_id':data["chain_id"],
              'transaction_address':data["transaction_address"],
              'block_number':data["block_number"],
              'token1':data["tokens_in"][0]["symbol"],
              'token1_price':data["tokens_in"][0]["price_usd"],
              'token1_amount':data["tokens_in"][0]["amount"],
              'token2':data["tokens_out"][0]["symbol"],
              'token2_price':data["tokens_out"][0]["price_usd"],
              'token2_amount':data["tokens_out"][0]["amount"],
              'wallet_address':data["wallet_address"],
              'wallet_category':data["wallet_category"],
              'pair_address':data["pair_address"]
              }
            json_list.append(index)
        if len(json_list) == 0:
            return df
        df = df.append(json_list)
        df["create_timestamp"] = pd.to_datetime(df["create_timestamp"],unit='s')
        df['chain_id'] = df['chain_id'].astype('int64')
        df['block_number'] = df['block_number'].astype('int64')
        df["symbol"] = df['symbol'].astype('string')
        return df



    @staticmethod
    def get_a_model():
        data = {'block_timestamp':[],
              'create_timestamp':[],
              'smybol': [],
              'net': [],
              'amm':[],
              'chain_id':[],
              'transaction_address':[],
              'block_number':[],
              'token1':[],
              'token1_price':[],
              'token1_amount':[],
              'token2':[],
              'token2_price':[],
              'token2_amount':[],
              'wallet_address':[],
              'wallet_category':[],
              'pair_address':[]
              }
        df = pd.DataFrame(data)
        return df