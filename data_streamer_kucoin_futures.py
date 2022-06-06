# Author - Karan Parmar

"""
Kucoin futures data streamer
"""

# Importing built-in libraries
import json
from datetime import datetime
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
import requests
from websocket import WebSocketApp

class KucoinDataStreamerFutures(Thread):
	
	ID = "VT_STREAMER_KUCOIN_FUTURES"
	EXCHANGE = "KUCOIN"
	MARKET = "FUTURES"
	NAME = "Kucoin data streamer"
	AUTHOR = "Variance Technologies"
	
	BASE_URL = "wss://push1-v2.kucoin.com/endpoint"

	rest_url = "https://api-futures.kucoin.com"

	_is_connected = False
	_subscriptions = []

	def __init__(self):
		Thread.__init__(self,daemon=False)

	@staticmethod
	def _filter_symbol(symbol:str) -> str:
		"""
		Filter dashed seperated symbol to appropriate ticker\n
		"""
		return symbol.replace('_','-').upper()
		
	def create_websocket_app(self) -> None:
		"""
		Creates websocket app\n
		"""
		self.WSAPP = WebSocketApp(
			self.BASE_URL,
			on_open=self.on_open,
			on_message=self.on_message,
			on_close=self.on_close,
			on_ping=self.on_ping,
			on_pong=self.on_pong
		)

	def on_open(self, wsapp) -> None:
		"""
		Call on open\n
		"""
		self.is_connected = True
		self._initialize_subscription()
		
	def on_message(self, wsapp, message) -> None:
		msg = json.loads(message)
		# print(msg)
		
		symbol = msg['data']['symbol']
		ltp = float(msg['data']['price'])
		qty = float(msg['data']['size'])
		# print(symbol,ltp, qty)
		self.save_data(symbol, ltp, qty)

	def on_close(self,wsapp,*args) -> None:
		"""
		"""
		pass
	
	def on_ping(self,*args) -> None:
		"""
		"""
		pass

	def on_pong(self,*args) -> None:
		"""
		"""
		pass

	# Public methods
	def _initialize_subscription(self) -> None:
		"""
		Initialize subscription to all required tickers\n
		"""
		for ticker in self._subscriptions:
			data = {
				"id": int(datetime.now().timestamp()),
				"type": "subscribe",
				"topic":f"/contractMarket/ticker:{ticker}",
				"privateChannel": False,
				"response": True
			}
			self.WSAPP.send(json.dumps(data))
	
	def _get_ws_url(self) -> None:
		"""
		Gets websocket active url from the server\n
		"""
		tokenUrl = self.rest_url + "/api/v1/bullet-public"
		data = requests.post(tokenUrl).json()['data']
		
		self.BASE_URL = data['instanceServers'][-1]['endpoint']
		self.TOKEN = data['token']
		ts = int(datetime.now().timestamp())
		self.BASE_URL += f"?token={self.TOKEN}&[connectId={ts}]"

	# Public methods
	def subscribe(self, symbol:str) -> None:
		"""
		Subscribe to FUTURES ticker\n
		"""
		ticker = self._filter_symbol(symbol)
		if ticker not in self._subscriptions:
			self._subscriptions.append(ticker)

	def unsubscribe(self, symbol:str) -> None:
		"""
		Unsubscribe to FUTURES ticker\n
		"""
		ticker = self._filter_symbol(symbol)
		if ticker in self._subscriptions:
			_ = {
				"id": int(datetime.now().timestamp()),
				"type": "unsubscribe",
				"topic":f"/contractMarket/ticker:{ticker}",
				"privateChannel": False,
				"response": True
			}
			self.WSAPP.send(json.dumps(_))
			self._subscriptions.remove(ticker)

	def get_connection_status(self) -> bool:
		"""
		Get socket connection status\n
		"""
		return self._is_connected
	
	def get_no_of_active_subscriptions(self) -> int:
		"""
		Returns no of active subscriptions by the socket\n
		"""
		return len(self._subscriptions)
		
	# Thread
	def run(self):
		
		while True:

			self._get_ws_url()

			self.create_websocket_app()

			self.WSAPP.run_forever()

			self._is_connected = False

if __name__ == '__main__':
	
	S1 = KucoinDataStreamerFutures()
	S1.start()

	S1.subscribe(symbol="XBTUSDTM")
	S1.subscribe(symbol="ETHUSDTM")

	import time

	time.sleep(5)
	S1.unsubscribe(symbol="XBTUSDTM")