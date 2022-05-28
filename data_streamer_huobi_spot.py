# Author - Karan Parmar

"""
Huobi spot data streamer
"""

# Importing built-in libraries
import json, gzip
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
from websocket import WebSocketApp

class HuobiDataStreamer(Thread):

	ID = "VT_STREAMER_HUOBI_SPOT"
	EXCHANGE = "HUOBI"
	MARKET = "SPOT"
	NAME = "Huobi data streamer"
	AUTHOR = "Variance Technologies"

	url = "wss://api.huobi.pro/ws/"

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
			on_pong=self.on_pong,
			on_error=self.on_error
		)

	def on_open(self, wsapp) -> None:
		data = {"sub": f"market.{self.SYMBOL.lower().replace('_','')}.ticker"}
		self.WSAPP.send(json.dumps(data))

	def on_message(self, wsapp, message) -> None:
		msg = json.loads(gzip.decompress(message))
		# print(msg)
		
		# Sending pong heartbeat
		if msg.get('ping'): 
			pong = {"op":"pong","ts":msg['ping']}
			# print(pong)
			self.WSAPP.send(json.dumps(pong))

		self.save_data(float(msg['tick']['lastPrice']),float(msg['tick']['lastSize']))

	def on_close(self,wsapp,*args) -> None:
		"""
		"""
		pass
	
	def on_error(self, *args) -> None:
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

	symbol = 'ETH_USDT'

	B1 = HuobiDataStreamer()
	B1.set_symbol(symbol)
	B1.start()