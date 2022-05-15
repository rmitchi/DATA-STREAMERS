# Author - Shantheri

"""
Kraken data streamer
"""

# Importing built-in libraries
import json
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
from websocket import WebSocketApp

class KrakenDataStreamer(Thread):
	ID = "VT_STREAMER_KRAKEN"
	NAME = "KARKEN data streamer"
	AUTHOR = "Variance Technologies"
 
    

	url = "wss://ws.kraken.com/"

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

	def on_open(self, wsapp) -> None:
		data ='{"event":"subscribe", "subscription":{"name":"%(feed)s", "depth":%(depth)s}, "pair":["%(symbol)s"]}' % {"feed":"book", "depth":10, "symbol":self.SYMBOL}
		self.WSAPP.send(data)

	def on_message(self, wsapp, message) -> None:
		msg = json.loads(message)
		print(msg)
		self.save_data(float(msg['data']['last']),0)

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
		
	# Thread
	def run(self):
		
		while True:
			self.create_websocket_app()

			self.WSAPP.run_forever()

if __name__ == '__main__':

	symbol = "XBT/USDT"

	K1 = KrakenDataStreamer()
	K1.set_symbol(symbol)
	K1.start()