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

class BinanceFuturesDataStreamer(Thread):

	ID = "VT_STREAMER_BINANCE_FUTURES"
	EXCHANGE = "BINANCE"
	MARKET = "FUTURES"
	NAME = "Binance futures data streamer"
	AUTHOR = "Variance Technologies"

	url = "wss://fstream.binance.com/ws/"

	def __init__(self):
		Thread.__init__(self,daemon=False)
		
	def create_websocket_app(self) -> None:
		"""
		Creates websocket app\n
		"""
		self.WSAPP = WebSocketApp(
			self.url,
			on_open=self.on_open,
			on_message=self.on_message,
			on_close=self.on_close,
			on_ping=self.on_ping,
			on_pong=self.on_pong
		)

	def on_open(self,wsapp) -> None:
		"""
		"""
		pass

	def on_message(self, wsapp, message) -> None:
		msg = json.loads(message)
		# print(msg)
		self.save_data(float(msg['p']),float(msg['q']))

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
	def set_symbol(self, symbol: str) -> None:
		self.SYMBOL = symbol
		self.url = self.url + symbol.replace('_','').lower() + '@aggTrade'
		
	# Thread
	def run(self):
		
		while True:
			self.create_websocket_app()

			self.WSAPP.run_forever()

if __name__ == '__main__':

	symbol = 'ETH_USDT'

	B1 = BinanceFuturesDataStreamer()
	B1.set_symbol(symbol)
	B1.start()