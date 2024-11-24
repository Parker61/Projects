#!/usr/bin/env python
import sys
import csv
from datetime import datetime

mapping = {
    1: 'Credit card',
    2: 'Cash',
    3: 'No charge',
    4: 'Dispute',
    5: 'Unknown',
    6: 'Voided trip'
}


def perform_map():
    reader = csv.reader(sys.stdin)
    header = next(reader)

    for row in reader:
        try:
            tpep_pickup_datetime = row[1]
            payment_type = int(row[9])
            tip_amount = float(row[13])

            date = datetime.strptime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S')
            if date.year != 2020:
                continue

            # Проверяем валидность payment_type
            if payment_type not in mapping:
                continue

            month = date.strftime('%Y-%m')
            payment_type_str = mapping[payment_type]

            print(f'{month},{payment_type_str},{tip_amount}')
        except Exception as e:
            print(f'Mapper Exception: {e}', file=sys.stderr)
            continue  # Пропускаем строки с ошибками


if __name__ == '__main__':
    perform_map()
