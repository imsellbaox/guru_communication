import json
import time
from HandlerData.guru import guru

class handler_center:
    def __init__(self):
        self.url = "https://api.dev.dex.guru"
        self.api_key = ""
        self.api_ddr = "/v1/chain/{chain_id}/amms/{amm}/swaps"
        self.chain_id = ""
        self.net = ""
        self.anchor = ""
        self.token_address = ""
        self.symbol = ""
        self.amm = ""

    def run(self):
        guru_inliz = guru(self.url, self.api_ddr, self.api_key, chain_id=self.chain_id, amm=self.amm)
        guru_inliz.pull_start(token=self.anchor,token_address = self.token_address, symbol=self.symbol,net=self.net)
