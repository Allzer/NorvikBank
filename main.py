from zeep import Client
import pandas as pd
from datetime import datetime, timedelta

# Указываем WSDL-адрес API ЦБ РФ
wsdl_url = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL"

# Создаём клиент
client = Client(wsdl_url)

# Задаём диапазон дат для получения ключевой ставки
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

# Запрашиваем данные
response = client.service.KeyRate(start_date, end_date)

# Обрабатываем данные
rates = []
for record in response['_value_1']['_value_1']:  # Доступ к данным из SOAP-ответа
    try:
        if 'KR' in record:
            rate_record = {
                "date": record['KR']['DT'].strftime("%Y-%m-%d"),
                "rate": float(record['KR']['Rate'])
            }
        elif 'DT' in record and 'Rate' in record:
            rate_record = {
                "date": record['DT'].strftime("%Y-%m-%d"),
                "rate": float(record['Rate'])
            }
        else:
            continue
        rates.append(rate_record)
    except Exception as e:
        print(f"Error processing record: {record}. Error: {e}")
        continue

# Создаём DataFrame с ключевыми ставками
key_rate_df = pd.DataFrame(rates)
key_rate_df['date'] = pd.to_datetime(key_rate_df['date'])

# Генерируем полный список дат в заданном диапазоне
date_range = pd.date_range(start=start_date, end=end_date, freq='D')
transactions_df = pd.DataFrame(date_range, columns=['date'])

# Объединяем данные
transactions_df = transactions_df.merge(key_rate_df, on='date', how='left')

# Для пустых ставок ищем ближайшую ставку
transactions_df['rate'] = transactions_df['rate'].fillna(
    transactions_df['date'].map(
        lambda d: key_rate_df.iloc[(key_rate_df['date'] - d).abs().argsort()[0]]['rate']
    )
)

# Рассчитываем упущенный доход (можно добавить фиксированный баланс, если требуется)
fixed_balance = 100000
transactions_df['lost_income'] = transactions_df.apply(
    lambda row: fixed_balance * (row['rate'] / 100) / 365, axis=1
)

# Добавляем столбец atm_id для слияния
transactions_df['atm_id'] = 'ATM_001'  # Здесь можешь использовать значение, которое тебе нужно

# Загружаем fees_df (предполагаем, что данные о комиссиях уже есть)
# Для примера создадим fees_df с подобными данными:
fees_data = {
    'atm_id': ['ATM_001', 'ATM_002'],
    'CashDeliveryFixedFee': [10, 20],
    'CashDeliveryPercentageFee': [0.5, 1.0],
    'CashDeliveryMinFee': [5, 10],
    'CashCollectionFixedFee': [15, 25],
    'CashCollectionPercentageFee': [0.3, 0.7],
    'CashCollectionMinFee': [7, 12]
}
fees_df = pd.DataFrame(fees_data)

# Преобразуем столбцы для слияния
fees_df['atm_id'] = fees_df['atm_id'].astype(str)

# Выполняем слияние с fees_df по столбцу atm_id
transactions_df = transactions_df.merge(fees_df[['atm_id', 'CashCollectionFixedFee', 
                                                  'CashCollectionPercentageFee', 'CashCollectionMinFee']], 
                                          on='atm_id', how='left')

# Выводим результат
print(transactions_df)

# Сохраняем итоговый DataFrame в CSV
transactions_df.to_csv("transactions_with_fees.csv", index=False)
