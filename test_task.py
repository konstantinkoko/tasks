import asyncio
import json
import websockets


class PriceMonitor:

    def __init__(self) -> None:

        self.time_period = 3600
        self.price_delta = 1
        self.correlation_coefficient = 0.9 # взят годовой отсюда: https://cryptowat.ch/ru-ru/correlations

        self.current_timestamp = 0

        self.price_dict = {
            "ETH" : [],
            "BTC" : []
        }

        self.coeff_dict = {
            "downgrade" : [-1, -1],
            "increase" : [0, 1]
        }


    def message_handler(self, message, currency_ticker):

        message_dict = json.loads(message)

        if "timestamp" in message_dict:
            price = float(message_dict["events"][0]["price"])
            timestamp = int(message_dict["timestamp"])
            self.current_timestamp = timestamp
            self._binary_insert(currency_ticker, (price, timestamp))

            if currency_ticker == "ETH":
                for mode in ["downgrade", "increase"]:
                    index, k = self.coeff_dict[mode]
                    delta_index = 0
                    while True:
                        if (timestamp - self.price_dict[currency_ticker][index + k * delta_index][1] > self.time_period):
                            delta_index += 1
                        elif -k * (self.price_dict[currency_ticker][index + k * delta_index][0] - price) < self.price_delta * self.correlation_coefficient * price / 100:
                            break
                        else:
                            self.check_self_price_change(mode, (price, timestamp), delta_index)
                            break
                
        if len(self.price_dict[currency_ticker]) > 2 * self.time_period:
            new_list = []
            for elem in self.price_dict[currency_ticker]:
                if self.current_timestamp - elem[1] < 3600:
                    new_list.append(elem)
            self.price_dict[currency_ticker] = new_list
        

    def check_self_price_change(self, mode, value, delta_index):
        index, k = self.coeff_dict[mode]
        eth_price_old, eth_timestamp_old = self.price_dict["ETH"][index + k * delta_index]
        btc_price_old = self.search_btc_price(eth_timestamp_old)
        btc_current_price = self.search_btc_price(value[1])
        delta_eth_self = value[0] - eth_price_old - self.correlation_coefficient * eth_price_old * (btc_current_price - btc_price_old) / btc_price_old
        if self.price_delta * value[0] / 100 < delta_eth_self:
            print('\n', mode, eth_price_old, '--->', value[0])
    

    def search_btc_price(self, timestamp):
        delta_timestamp = self.time_period
        index = 0
        for i, elem in enumerate(self.price_dict["BTC"]):
            if timestamp == elem[1]:
                return elem[0]
            else:
                if timestamp - elem[1] < delta_timestamp:
                    index = i
        return self.price_dict["BTC"][i][0]
    

    async def currency_ws(self, currency_ticker):
        async with websockets.connect(f'wss://api.gemini.com/v1/marketdata/{currency_ticker}USDT') as websocket:
            while True:
                message = await websocket.recv()
                self.message_handler(message, currency_ticker)
    

    def _binary_insert(self, currency_ticker, value):
        index = self._binary_index_search(currency_ticker, value)
        if index != -1:
            self.price_dict[currency_ticker].insert(index, value)
    

    def _binary_index_search(self, currency_ticker, value, lo=0, hi=None):
        lst = self.price_dict[currency_ticker]
        if hi is None:
            hi = len(lst)
        if hi == 0:
            return 0
        while lo < hi:
            mid = (lo+hi)//2
            midvalue = lst[mid]
            if midvalue == value:
                return -1
            elif midvalue[0] < value[0]:
                lo = mid+1
            elif midvalue[0] > value[0]: 
                hi = mid
            else:
                return mid
        return lo



if __name__ == "__main__":
    price_monitor = PriceMonitor()
    loop = asyncio.new_event_loop()
    loop.run_until_complete(asyncio.wait([   
    price_monitor.currency_ws("BTC"),
    price_monitor.currency_ws("ETH")
    ]))
