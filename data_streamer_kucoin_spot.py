# Author - Karan Parmar

"""
Kucoin spot data streamer
"""

# Importing built-in libraries
import json
from datetime import datetime
from threading import Thread

# Importing dependent libraries

# Importing third-party libraries
import requests
from websocket import WebSocketApp

class KucoinDataStreamer(Thread):
	
	ID = "VT_STREAMER_KUCOIN_SPOT"
	EXCHANGE = "KUCOIN"
	MARKET = "SPOT"
	NAME = "Kucoin data streamer"
	AUTHOR = "Variance Technologies"
	
	rest_url = "https://api.kucoin.com"
	url = "wss://push1-v2.kucoin.com/endpoint"

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
		data ={
			"id": int(datetime.now().timestamp()),
			"type": "subscribe",
			"topic":f"/market/ticker:{self.SYMBOL}",
			"privateChannel": False,
			"response": True
		}
		self.WSAPP.send(json.dumps(data))
		
	def on_message(self, wsapp, message) -> None:
		msg = json.loads(message)
		# print(msg)
		ltp = float(msg['data']['price'])
		qty = float(msg['data']['size'])
		print(ltp, qty)
		self.save_data(ltp, qty)

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
		self.SYMBOL = symbol.upper().replace('_','-')

		tokenUrl = self.rest_url + "/api/v1/bullet-public"
		data = requests.post(tokenUrl).json()['data']
		
		self.url = data['instanceServers'][-1]['endpoint']
		self.TOKEN = data['token']
		ts = int(datetime.now().timestamp())
		self.url += f"?token={self.TOKEN}&[connectId={ts}]"
		
	# Thread
	def run(self):
		
		while True:
			self.create_websocket_app()

			self.WSAPP.run_forever()

if __name__ == '__main__':

	symbol = "BTC_USDT"

	K1 = KucoinDataStreamer()
	K1.set_symbol(symbol)
	K1.start()