# Author - Karan Parmar

"""
Huobi futures data streamer
"""

# Importing built-in libraries
import json, gzip
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
from websocket import WebSocketApp

class HuobiFuturesDataStreamer(Thread):

	ID = "VT_STREAMER_HUOBI_FUTURES"
	EXCHANGE = "HUOBI"
	MARKET = "FUTURES"
	NAME = "Huobi futures data streamer"
	AUTHOR = "Variance Technologies"

	url = "wss://api.hbdm.com/linear-swap-ws"

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
		data = {"sub": f"market.{self.SYMBOL.replace('_','-')}.trade.detail"}
		self.WSAPP.send(json.dumps(data))

	def on_message(self, wsapp, message) -> None:
		msg = json.loads(gzip.decompress(message))
		# print(msg)
		
		# Sending pong heartbeat
		if msg.get('ping'): 
			pong = {"op":"pong","ts":msg['ping']}
			# print(pong)
			self.WSAPP.send(json.dumps(pong))

		self.save_data(float(msg['tick']['data'][-1]['price']),float(msg['tick']['data'][-1]['quantity']))

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

	symbol = 'BTC_USDT'

	B1 = HuobiFuturesDataStreamer()
	B1.set_symbol(symbol)
	B1.start()