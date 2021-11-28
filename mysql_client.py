import arrow
import MySQLdb
from decimal import Decimal


CANDLES_ROW_MAPPER = [
    'time',
    'high',
    'close',
    'open',
    'low',
    'ask_high',
    'ask_close',
    'ask_open',
    'ask_low',
    'bid_high',
    'bid_close',
    'bid_open',
    'bid_low',
]

CANDLES_MOVEMENT_ROW_MAPPER = [
  'time',
  'long1',
  'long2',
  'long3',
  'long4',
  'long5',
  'long6',
  'long7',
  'long8',
  'long9',
  'long10',
  'long11',
  'long12',
  'long13',
  'long14',
  'long15',
  'long16',
  'long17',
  'long18',
  'long19',
  'short1',
  'short2',
  'short3',
  'short4',
  'short5',
  'short6',
  'short7',
  'short8',
  'short9',
  'short10',
  'short11',
  'short12',
  'short13',
  'short14',
  'short15',
  'short16',
  'short17',
  'short18',
  'short19',
  'long1_count',
  'long2_count',
  'long3_count',
  'long4_count',
  'long5_count',
  'long6_count',
  'long7_count',
  'long8_count',
  'long9_count',
  'long10_count',
  'long11_count',
  'long12_count',
  'long13_count',
  'long14_count',
  'long15_count',
  'long16_count',
  'long17_count',
  'long18_count',
  'long19_count',
  'short1_count',
  'short2_count',
  'short3_count',
  'short4_count',
  'short5_count',
  'short6_count',
  'short7_count',
  'short8_count',
  'short9_count',
  'short10_count',
  'short11_count',
  'short12_count',
  'short13_count',
  'short14_count',
  'short15_count',
  'short16_count',
  'short17_count',
  'short18_count',
  'short19_count',
]


class MysqlClient:

    def __init__(self):
        self.db = MySQLdb.connect(
            host='localhost',
            database='scalping',
            user='root',
            password='password'
        )

    def save_data(self, data, table):
        cursor = self.db.cursor()
        query = self.build_insert_query(data, table)
        cursor.execute(query)
        self.db.commit()
        cursor.close()

    def build_insert_query(self, data, table):
        return 'REPLACE INTO %s (%s) VALUES (%s)' % (
            table,
            ','.join(data.keys()),
            ','.join([str(v) for v in data.values()])
        )

    def get_data(self, table, start=None, end=None):
        cursor = self.db.cursor()
        query = "SELECT * FROM %s ORDER BY TIME" % table
        if start and end:
            query = (
              "SELECT * FROM %s WHERE time >= %s and time <= %s ORDER BY TIME"
              % (
                table, arrow.get(start).timestamp, arrow.get(end).timestamp
              )
            )
        elif start:
            query = "SELECT * FROM %s WHERE time >= %s ORDER BY TIME" % (
                table, arrow.get(start).timestamp
            )
        elif end:
            query = "SELECT * FROM %s WHERE time <= %s ORDER BY TIME" % (
                table, arrow.get(end).timestamp
            )
        cursor.execute(query)
        data = self.convert_sql_data(cursor, self.get_row_mapper(table))
        cursor.close()
        return data
    
    @staticmethod
    def convert_sql_data(cursor, mapper, convert_datetime=False):
        result = []
        for row in cursor:
            row_data = {}
            for i, column in enumerate(mapper):
                value = row[i]
                if convert_datetime and column == 'time':
                    value = arrow.get(value).timestamp
                if type(value) == Decimal:
                    value = float(value)
                row_data[column] = value
            result.append(row_data)
        return result

    def get_row_mapper(self, table):
      if '_movement' in table:
        return CANDLES_MOVEMENT_ROW_MAPPER
      else:
        return CANDLES_ROW_MAPPER
