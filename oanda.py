import arrow
import os
import requests


class Oanda:
    BASE_URL = 'https://api-fxtrade.oanda.com:443/v3'

    def __init__(self):
        self.ACCOUNT_ID = os.environ.get('OANDA_ACCOUNT_ID')
        self.API_SECRET = os.environ.get('OANDA_SECRET')

    def _get_headers(self):
        return {
            'Authorization': 'Bearer %s' % self.API_SECRET,
            'Content-Type': 'application/json',
        }

    def get_candles_detailed(
      self, instrument='EUR_USD', granularity='1', count=60, start_time=None
    ):
        path = '%s/instruments/%s/candles?price=BA&granularity=M%s&count=%s' % (
			self.BASE_URL, instrument, granularity, count
		)
        if start_time:
            path += '&from=%s' % arrow.get(start_time).datetime
        response = requests.get(path, headers=self._get_headers())
        if response.status_code == 200:
            candles = response.json().get('candles')
            return [
                {
                    'time': arrow.get(candle['time']).timestamp,
                    'open': (
						float(candle['ask']['o']) + float(candle['bid']['o'])
					) / 2.0,
                    'high': (
						float(candle['ask']['h']) + float(candle['bid']['h'])
					) / 2.0,
                    'low': (
						float(candle['ask']['l']) + float(candle['bid']['l'])
					) / 2.0,
                    'close': (
						float(candle['ask']['c']) + float(candle['bid']['c'])
					) / 2.0,
                    'bid_open': float(candle['bid']['o']),
                    'bid_high': float(candle['bid']['h']),
                    'bid_low': float(candle['bid']['l']),
                    'bid_close': float(candle['bid']['c']),
                    'ask_open': float(candle['ask']['o']),
                    'ask_high': float(candle['ask']['h']),
                    'ask_low': float(candle['ask']['l']),
                    'ask_close': float(candle['ask']['c']),
                    'volume': float(candle['volume'])
                } for candle in candles
            ]
        return []
