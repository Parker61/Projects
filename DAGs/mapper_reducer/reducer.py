#!/usr/bin/env python
import sys
from collections import defaultdict


def perform_reduce():
    data = defaultdict(lambda: defaultdict(lambda: [0, 0]))  # {month: {payment_type: [sum, count]}}

    for line in sys.stdin:
        line = line.strip()
        if line == 'Month,Payment type,Tips average amount':
            continue
        try:
            month, payment_type, tip_amount = line.split(',')
            tip_amount = float(tip_amount)

            # Добавляем логирование
            print(f'Reducer: {month},{payment_type},{tip_amount}', file=sys.stderr)

        except ValueError:
            # Логируем ошибку разбора строки
            print(f'Reducer ValueError: {line}', file=sys.stderr)
            continue

        data[month][payment_type][0] += tip_amount
        data[month][payment_type][1] += 1

    # Сортируем данные по месяцам и типам оплаты
    sorted_data = sorted(data.items(), key=lambda x: x[0])

    # Выводим данные в формате CSV
    print('Month,Payment type,Tips average amount')
    for month, payment_data in sorted_data:
        for payment_type, (total_tips, count) in sorted(payment_data.items()):
            avg_tip = total_tips / count
            print(f'{month},{payment_type},{avg_tip:.2f}')


if __name__ == '__main__':
    perform_reduce()
