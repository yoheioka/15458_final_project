import arrow
import sys
import os
_root = os.path.dirname(os.path.abspath(__file__)).split(os.path.sep)[:-1]
sys.path.extend([
    os.path.sep.join(_root),
])
from oanda import Oanda
from mysql_client import MysqlClient

START = arrow.get('2015-01-01').timestamp
END = arrow.get('2019-01-02').timestamp
if len(sys.argv) != 3:
    print(
      '    eg python scripts/0_build_dataset.py [1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
if int(sys.argv[1]) not in [1, 2, 3, 5, 10, 15, 30]:
    print(
      '    eg python scripts/0_build_dataset.py [1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
if sys.argv[2] not in ['EUR_USD', 'USD_JPY']:
    print(
      '    eg python scripts/0_build_dataset.py [1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
GRANULARITY = int(sys.argv[1])
INSTRUMENT = sys.argv[2]

def main():
    oanda = Oanda()
    start = START
    mysql_client = MysqlClient()
    while start < END:
        prev = start
        print(arrow.get(start))
        candles = oanda.get_candles_detailed(
          instrument=INSTRUMENT, count=5000,
          start_time=start, granularity=GRANULARITY
        )
        for candle in candles:
            mysql_client.save_data(
              candle, 'candles_%s_%s' % (GRANULARITY, INSTRUMENT)
            )
        start = candles[-1]['time']
        if start == prev:
            return

if __name__ == '__main__':
    main()
