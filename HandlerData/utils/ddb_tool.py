import dolphindb as ddb
import pandas
import time
import configparser

def timestmp_to_TM(timestamp):
    t = time.strftime("%Y.%m.%dT%H:%M:%S.000", time.localtime(timestamp))
    return str(t)


class ddbtool:
    def __init__(self):
        cf = configparser.ConfigParser()
        cf.read("./config.ini")
        host = cf.get("dolphinDB", "host")
        port = int(cf.get("dolphinDB", "port"))
        user = cf.get("dolphinDB", "user")
        password = cf.get("dolphinDB", "password")
        self.dbpath = cf.get("dolphinDB", "db")
        self.guru_tikcer_data =  cf.get("dolphinDB", "guru_tikcer_data")
        self.session = ddb.session()
        self.con = self.session.connect(host, port, userid=user, password=password)



    def write_symbol_data(self,df: pandas.DataFrame, symbol:str, table_name: str):
        df = df.loc[df['symbol'] == symbol, :].sort_index(ascending=False)
        if df.empty:
            return
        print(df)
        self.insert_data(table_name,df)

    def insert_data(self,table_name,df):
        script = """
        def add_data(table_name,db_name,result) {
            d = database(db_name)
            t = d.loadTable(table_name)
            data = table(result["block_timestamp"] as "block_timestamp",result["create_timestamp"] as "create_timestamp",result["symbol"] as "symbol",result["net"] as "net",result["amm"] as "amm",result["chain_id"] as "chain_id",result["transaction_address"] as "transaction_address",result["block_number"] as "block_number",result["token1"] as "token1",result["token1_price"] as "token1_price",result["token1_amount"] as "token1_amount",result["token2"] as "token2",result["token2_price"] as "token2_price",result["token2_amount"] as "token2_amount",result["wallet_address"] as "wallet_address",result["wallet_category"] as "wallet_category",result["pair_address"] as "pair_address")
            t.tableInsert(data)
            }
            """
        self.session.run(script)
        self.session.run("add_data",table_name,self.dbpath,df)


    def query_data(self,table_name: str,begin_timestamp:int=None,end_timestamp:int=None,symbol=None,amm=None,net=None):
        if begin_timestamp == None:
            begin_timestamp=timestmp_to_TM(0)
        if end_timestamp == None:
            now = int(time.time())
            end_timestamp=timestmp_to_TM(now)
        sql = "select * from " + table_name + " where "+ str(begin_timestamp) +"<block_timestamp<="+str(end_timestamp)
        if  symbol != None:
            sql = sql + " and symbol=\'"+symbol+"\'"
        if amm !=None:
            sql = sql + " and amm=\'"+amm+"\'"
        if net != None:
            sql = sql + " and net=\'" + net + "\'"

        try:
            result = self.session.loadTableBySQL(tableName=table_name, dbPath=self.dbpath,
                                             sql=sql)
        except Exception as e:
            print(e)
            return ""
        return result.toDF().to_json(orient="records",force_ascii=False)