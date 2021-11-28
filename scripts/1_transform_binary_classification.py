import arrow
import sys
import os
_root = os.path.dirname(os.path.abspath(__file__)).split(os.path.sep)[:-1]
sys.path.extend([
    os.path.sep.join(_root),
])
from mysql_client import MysqlClient

if len(sys.argv) != 3:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
if int(sys.argv[1]) not in [1, 2, 3, 5, 10, 15, 30]:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
if sys.argv[2] not in ['EUR_USD', 'USD_JPY']:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY]'
    )
    sys.exit(1)
GRANULARITY = int(sys.argv[1])
INSTRUMENT = sys.argv[2]

SUFFIX = '_movement'
METRICS = {
    'long1': (True, 0.0001, -0.0001),
    'long2': (True, 0.0002, -0.0002),
    'long3': (True, 0.0003, -0.0003),
    'long4': (True, 0.0004, -0.0004),
    'long5': (True, 0.0005, -0.0005),
    'long6': (True, 0.0006, -0.0006),
    'long7': (True, 0.0007, -0.0007),
    'long8': (True, 0.0008, -0.0008),
    'long9': (True, 0.0009, -0.0009),
    'long10': (True, 0.0010, -0.0010),
    'long11': (True, 0.0011, -0.0011),
    'long12': (True, 0.0012, -0.0012),
    'long13': (True, 0.0013, -0.0013),
    'long14': (True, 0.0014, -0.0014),
    'long15': (True, 0.0015, -0.0015),
    'long16': (True, 0.0016, -0.0016),
    'long17': (True, 0.0017, -0.0017),
    'long18': (True, 0.0018, -0.0018),
    'long19': (True, 0.0019, -0.0019),
    'short1': (False, -0.0001, 0.0001),
    'short2': (False, -0.0002, 0.0002),
    'short3': (False, -0.0003, 0.0003),
    'short4': (False, -0.0004, 0.0004),
    'short5': (False, -0.0005, 0.0005),
    'short6': (False, -0.0006, 0.0006),
    'short7': (False, -0.0007, 0.0007),
    'short8': (False, -0.0008, 0.0008),
    'short9': (False, -0.0009, 0.0009),
    'short10': (False, -0.0010, 0.0010),
    'short11': (False, -0.0011, 0.0011),
    'short12': (False, -0.0012, 0.0012),
    'short13': (False, -0.0013, 0.0013),
    'short14': (False, -0.0014, 0.0014),
    'short15': (False, -0.0015, 0.0015),
    'short16': (False, -0.0016, 0.0016),
    'short17': (False, -0.0017, 0.0017),
    'short18': (False, -0.0018, 0.0018),
    'short19': (False, -0.0019, 0.0019),
}
START = arrow.get('2015-01-01').timestamp

def main():
    table = 'candles_%s_%s' % (GRANULARITY, INSTRUMENT)
    mysql_client = MysqlClient()
    data = mysql_client.get_data(table, start=START)
    num_candles = len(data)

    for i, candle in enumerate(data):
        if i % 10000 == 0:
            print(arrow.get(candle['time']))
        if i + 1 == num_candles:
            break
        long_entry = data[i + 1]['ask_open']
        short_entry = data[i + 1]['bid_open']
        updates = {}

        j = i + 1

        decision = False

        while (j < num_candles):
            next_candle = data[j]

            for key, values in METRICS.items():
                if key in updates:
                    continue
                # if long
                if values[0]:
                    if next_candle['bid_low'] <= long_entry * (1 + values[2]):
                        updates[key] = 0
                        updates[key + '_count'] = j - i
                        continue
                    if next_candle['bid_high'] >= long_entry * (1 + values[1]):
                        updates[key] = 1
                        updates[key + '_count'] = j - i
                        continue
                # if short
                else:
                    if next_candle['ask_high'] >= short_entry * (1 + values[2]):
                        updates[key] = 0
                        updates[key + '_count'] = j - i
                        continue
                    if next_candle['ask_low'] <= short_entry * (1 + values[1]):
                        updates[key] = 1
                        updates[key + '_count'] = j - i
                        continue

            decision = True
            if len(updates) == len(METRICS) * 2:
                break
            j += 1
            decision = False

        if decision:
            updates['time'] = candle['time']
            mysql_client.save_data(updates, table + SUFFIX)


if __name__ == '__main__':
    main()
