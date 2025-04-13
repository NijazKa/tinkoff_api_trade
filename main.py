import time
from datetime import timedelta, date
from tinkoff.invest import Client, CandleInterval, OrderType, OrderDirection
from tinkoff.invest.utils import now
import pandas as pd
import logging
from uuid import uuid4


TOKEN = ''
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

moneta = 'FUTSBERF0000' #прописываем тикер мосбиржи для работы

def data_prep(ticker, days=200):
    simple_df = []  # формируем датафрейм
    figi = ticker  # прописываем наш инструмент через FIGI
    with Client(TOKEN) as client:
        for candle in client.get_all_candles(
                figi=figi,
                from_=now() - timedelta(days=days),
                interval=CandleInterval.CANDLE_INTERVAL_HOUR,  # Таймфрейм = 1 час
        ):
            open_price = candle.open.units + candle.open.nano / 1e9
            high_price = candle.high.units + candle.high.nano / 1e9
            low_price = candle.low.units + candle.low.nano / 1e9
            close_price = candle.close.units + candle.close.nano / 1e9
            volume = candle.volume
            time = candle.time

            simple_df.append([time, open_price, high_price, low_price, close_price, volume])
            # print(candle)
        simple_df = pd.DataFrame(simple_df, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])
        simple_df.columns = simple_df.columns.str.lower()
        simple_df['time'] = simple_df['time'].dt.tz_localize(None)

    return simple_df, figi



def get_position(): # получить состояние позиций
    # клиент НУЖНО оборачивать в try - именно клиент выкидывает искл. в сл. ошибок API
    with Client(TOKEN) as client:
        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id
        #r = client.users.get_accounts()
        r = client.operations.get_positions(account_id=account_id)

        #
        # r = client.operations.get_operations(
        #     account_id=creds.account_id_main,
        #     from_= datetime.datetime(2021,1,1),
        #     to=datetime.datetime.now()
        # )
        #
        # r = client.instruments.bonds()
        # r = client.instruments.bonds(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_UNSPECIFIED)
        # figi = 'BBG00T22WKV5'
        # r = client.instruments.bond_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)
        # ticker = 'SU29013RMFS8'
        # class_code = 'TQOB'
        # r = client.instruments.bond_by(id=ticker, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, class_code=class_code)
        #
        # r = client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE)
        #
        # figi = 'BBG00T22WKV5'
        # r = client.market_data.get_order_book(figi=figi, depth=50) # 50 макс (стаканов (с глубиной 10, 20, 30, 40 или 50);)
        # r = len(r.bids)
        df = pd.DataFrame(r.futures)
        return df

def open_order(figi, quantity, trade): # открытие ордера
    with Client(TOKEN) as client:
        accounts = client.users.get_accounts()
        account_id = accounts.accounts[0].id
        if trade == 'BUY':
            request = client.orders.post_order(
                order_type=OrderType.ORDER_TYPE_MARKET,
                direction=OrderDirection.ORDER_DIRECTION_BUY,
                figi=figi,
                quantity=quantity,
                account_id=account_id,
                order_id=str(uuid4()),
            )
        elif trade == 'SELL':
            request = client.orders.post_order(
                order_type=OrderType.ORDER_TYPE_MARKET,
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                figi=figi,
                quantity=quantity,
                account_id=account_id,
                order_id=str(uuid4()),
            )
        #response = client.orders.post_order(request=request)
        return request




def close_all_order(): #закрытие всех открытых ордеров
    with Client(TOKEN) as client:
        response = client.users.get_accounts()
        account, *_ = response.accounts
        account_id = account.id
        logger.info("Orders: %s", client.orders.get_orders(account_id=account_id))
        client.cancel_all_orders(account_id=account.id)
        logger.info("Orders: %s", client.orders_stream.order_state_stream(account_id=account_id))




df = get_position() # получаем все позиции
# trade = df.loc[df['figi'] == moneta, 'balance']
# print(trade.iloc[-1])

def position_quantity(figi, df): # объем позиции в лотах
    q = df.loc[df['figi'] == figi, 'balance']
    q = q.iloc[-1]
    q = abs(q)
    return q

def position_trade(figi, df): #направление позиции
    trade = df.loc[df['figi'] == figi, 'balance']
    trade =  trade.iloc[-1]
    if trade < 0:
        return 'SHORT'
    elif trade > 0:
        return 'LONG'
    else:
        return None


# логика проверки состояния позиции
if df.empty == False and position_trade(moneta, df) == 'SHORT':
    print('монета в шорте, надо купить')
elif df.empty == False and position_trade(moneta, df) == 'LONG':
    print('монета в лонге, надо продать')
else:
    pass





# код сбора данныех с МОЕКС

moex_list = ['BBG004S68614', 'FUTBR1124000', 'BBG00475K6C3', 'FUTCNY122400', 'BBG004730RP0', 'BBG004731489', 'FUTGAZR12240', 'TCS20A107662', 'FUTLKOH12240', 'BBG004731032', 'BBG004RVFCY3', 'BBG004S68598', 'FUTNG1124000', 'BBG004S681B4', 'BBG00475KKY8', 'BBG000R607Y3', 'BBG004731354', 'BBG004730N88', 'FUTSI1224000', 'BBG004S681M2', 'FUTSBRF12240', 'FUTSILV12240', 'TCS00A107UL4', 'BBG00475KHX6', 'FUTVTBR12240', 'BBG004730ZJ9', 'TCS00A107T19', 'FUTGOLD12240', 'FUTRTS122400', 'FUTRTSM12240', 'FUTMXI122400', 'FUTSPYF12240', 'FUTED1224000', 'FUTPLD122400', 'FUTPLT122400']
first_date = date(2024, 8, 11) #забирем данные с текущей даты
second_date = date.today() # по сегодняшнее число
delta = second_date - first_date
days = delta.days

import pytz
timezone = pytz.timezone("Europe/Moscow")
for ticker in moex_list:
    simple_df, figi = data_prep(ticker, days)
    simple_df['time'] = simple_df['time'].dt.tz_localize('UTC').dt.tz_convert('Europe/Moscow')
    simple_df['time'] = simple_df['time'].dt.tz_localize(None)
    # Удаление строк с субботой и воскресеньем
    simple_df = simple_df.drop(simple_df[simple_df['time'].dt.dayofweek.isin([5, 6])].index)
    simple_df['ticker'] = ticker
    simple_df = simple_df.rename(columns={'time': 'datetime'})
    simple_df.to_csv(f'{ticker}.csv', index=False)
    print(f'Датафрейм {ticker} успешно скачан')
    time.sleep(2)


