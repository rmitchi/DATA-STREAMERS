# Author - Karan Parmar

"""
Binance futures data streamer
"""

# Importing built-in libraries
import json
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
from websocket import WebSocketApp

class BinanceDataStreamerFutures(Thread):

	ID = "VT_STREAMER_BINANCE_FUTURES"
	EXCHANGE = "BINANCE"
	MARKET = "FUTURES"
	NAME = "Binance futures data streamer"
	AUTHOR = "Variance Technologies"

	BASE_URL = "wss://fstream.binance.com/ws/"

	_is_connected = False
	_subscriptions = []

	def __init__(self):
		Thread.__init__(self, daemon=False)

	@staticmethod
	def _filter_symbol(symbol:str) -> str:
		"""
		Filter dashed seperated symbol to appropriate ticker\n
		"""
		return symbol.replace('_','').lower()
		
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
		Call on websocket open\n
		"""
		self._is_connected = True

		self._initialize_subscription()

	def on_message(self, wsapp, message) -> None:
		msg = json.loads(message)
		# print(msg)

		symbol = msg['s']
		ltp = float(msg['p'])
		qty = float(msg['q'])

		self.save_data(symbol, ltp, qty)

	def on_close(self, wsapp, *args) -> None:
		"""
		"""
		pass
	
	def on_ping(self, *args) -> None:
		"""
		"""
		pass

	def on_pong(self, *args) -> None:
		"""
		"""
		pass

	def _initialize_subscription(self) -> None:
		"""
		Initialize subscription to all required tickers\n
		"""
		for ticker in self._subscriptions:
			_ = {
				"method": "SUBSCRIBE",
				"params":
				[f"{ticker}@trade"],
				"id": 1
			}
			self.WSAPP.send(json.dumps(_))

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
			"method": "UNSUBSCRIBE",
			"params":
			[f"{ticker}@trade"],
			"id": 1
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
			self.create_websocket_app()

			self.WSAPP.run_forever()

			self._is_connected = False

if __name__ == '__main__':

	S1 = BinanceDataStreamerFutures()
	S1.start()

	import time

	S1.subscribe(symbol="BTC_USDT")
	S1.subscribe(symbol="ETH_USDT")

	time.sleep(5)
	S1.unsubscribe(symbol="BTC_USDT")