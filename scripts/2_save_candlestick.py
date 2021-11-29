import arrow
import pandas as pd
import sys
import os
import mplfinance as fplt
import random
from multiprocessing import Pool
_root = os.path.dirname(os.path.abspath(__file__)).split(os.path.sep)[:-1]
sys.path.extend([
    os.path.sep.join(_root),
])
from mysql_client import MysqlClient

if len(sys.argv) != 4:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY] [year]'
    )
    sys.exit(1)
if int(sys.argv[1]) not in [1, 2, 3, 5, 10, 15, 30]:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY] [year]'
    )
    sys.exit(1)
if sys.argv[2] not in ['EUR_USD', 'USD_JPY']:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY] [year]'
    )
    sys.exit(1)
if int(sys.argv[3]) not in [2015, 2016, 2017, 2018]:
    print(
      '    eg python scripts/1_transform_binary_classification.py '
      '[1, 3, 5] [EUR_USD, USD_JPY] [year]'
    )
    sys.exit(1)

GRANULARITY = int(sys.argv[1])
INSTRUMENT = sys.argv[2]
YEAR = int(sys.argv[3])
METRIC = 'long10'
TRAIN_DIR = 'images/%s_%s_%s/train/%s'
TEST_DIR = 'images/%s_%s_%s/test/%s'
SAMPLE_RATE = 0.05
STYLE = fplt.make_mpf_style(base_mpf_style='charles', gridstyle='')
START = arrow.get('%s-01-01 00:00:00' % YEAR)
END = START.shift(years=1, days=5)
os.environ['TK_SILENCE_DEPRECATION'] = '1'


def render_image(df):
    time = df['time'][-1]
    arrow_time = arrow.get(time)
    year = arrow_time.year
    if arrow_time .minute == 0:
        print(arrow_time.datetime)
    label = df[METRIC][-1]

    if year != YEAR:
        return
    if year in [2015, 2016, 2017]:
        dir = TRAIN_DIR % (GRANULARITY, INSTRUMENT, METRIC, label)
    elif year in [2018]:
        dir = TEST_DIR % (GRANULARITY, INSTRUMENT, METRIC, label)
    else:
        return
    os.makedirs(dir, exist_ok=True)
    file = '%s/%s.jpg' % (dir, time)
    fplt.plot(
        df,
        type='candle',
        style=STYLE,
        axisoff=True,
        figratio=(10,10),
        figscale=1,
        savefig=dict(
            fname=file,
            dpi=100,
            bbox_inches='tight'
        )
    )


def main():
    mysql_client = MysqlClient()
    table = 'candles_%s_%s' % (GRANULARITY, INSTRUMENT)
    data = mysql_client.get_data(table, start=START, end=END)
    label_data = mysql_client.get_data(
        table + '_movement', start=START, end=END
    )
    df = pd.DataFrame(data)
    df_label = pd.DataFrame(label_data)
    df = df.merge(df_label, on='time', how='left')
    df['date'] = pd.to_datetime(df['time'], unit='s')
    df.index = pd.DatetimeIndex(df['date'])
    
    p = Pool(30)
    args = [
        df[i - 9:i]
        for i in range(9, len(df))
        if random.random() <= SAMPLE_RATE
    ]
    print('generated args')
    p.map(render_image, args)


if __name__ == '__main__':
    main()
