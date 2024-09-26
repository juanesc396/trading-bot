import pandas as pd

import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

import kaleido
import os
import sys
import numpy as np

import logging
from logging.handlers import RotatingFileHandler

from binance_utilities.notificator import notificator
from binance_utilities.credentials import set_credentials
from binance_utilities.balance_reporter import weekly_balance, monthly_balance
from binance_utilities.channels import set_channel
from decimal import Decimal, ROUND_DOWN


NAME = 'HakuReporter'

lf = './logs_haku/reporter.log'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    handlers=[RotatingFileHandler(filename=lf, maxBytes=150 * (10**6), backupCount=5, mode='a')])


def extract_candles(timeframe, symbol):
    """
    Function that extract the daily candles to make graphs
    """
    try:
        from binance_utilities.utils import get_past_candles
        df = get_past_candles(symbol = symbol,
                              interval = timeframe,
                              start_time = initial_date,
                              end_time = final_date)
        return df
    except Exception as e:
        msg = 'Error trying to extract daily candles'
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error trying to cancel the trade'
        logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
        notificator(category = 'CRITICAL',
                    process = NAME,
                    line = exc_tb.tb_lineno,
                    msg = msg,
                    error = f'{exc_type}: {exc_obj}')
        


def check_data(symbol):
    """
    Function that extract the orders executed by the bot
    """
    dates = []
    i = initial_date
    f = final_date

    while i <= f:
        f_date = i + timedelta(days=1)

        dates.append({'init_day': int(i.timestamp()*1000),
                        'final_day': int(f_date.timestamp()*1000)})
        i = f_date
    try:
        # Getting and cleaning long orders
        orders = []
        for set in dates:
            orders.extend(binance.get_orders(symbol = symbol,
                                             startTime = int(set['init_day']),
                                             endTime = int(set['final_day'])))
        long_orders_df = pd.DataFrame(orders)
        if len(long_orders_df) > 0:
            for col in ['price', 'origQty', 'executedQty', 'stopPrice', 'cummulativeQuoteQty']:
                long_orders_df[col] = pd.to_numeric(long_orders_df[col])

        # Getting and cleaning long trades
        orders = []
        for set in dates:
            orders.extend(binance.my_trades(symbol = symbol,
                                            startTime = int(set['init_day']),
                                            endTime = int(set['final_day'])))
        long_trades_df = pd.DataFrame(orders)
        if len(long_trades_df) > 0:
            for col in ['qty', 'quoteQty', 'commission', 'price']:
                long_trades_df[col] = pd.to_numeric(long_trades_df[col])
            long_trades_df = long_trades_df.groupby('orderId').agg({'qty': 'sum',
                                                                    'quoteQty': 'sum',
                                                                    'commission': 'sum',
                                                                    'symbol': 'first',
                                                                    'commissionAsset': 'first',
                                                                    'price': 'mean'}).reset_index()
        # Getting and cleaning saved trades by the bot
        trades_csv = pd.read_csv('./orders_haku/orders_haku.csv',
                                 names = ['report_date', 'entry_date', 'trade_type', 'exit', 'entry_id',
                                          'stop_id', 'exit_id', 'tp1_id', 'breakeven_id'],
                                 index_col=0)
        if len(trades_csv) > 0:
            l = ['entry_id', 'stop_id', 'exit_id', 'tp1_id', 'breakeven_id']
            for i in l:
                trades_csv[i] = pd.to_numeric(trades_csv[i], errors='coerce')

        filtered_trades = trades_csv[date0:date1]

        orders = []
        for set in dates:
            orders.extend(binance.margin_all_orders(symbol = symbol,
                                                    startTime = int(set['init_day']),
                                                    endTime = int(set['final_day'])))
        short_orders_df = pd.DataFrame(orders)
        if len(short_orders_df) > 0:
            for col in ['price', 'origQty', 'executedQty', 'stopPrice', 'cummulativeQuoteQty']:
                short_orders_df[col] = pd.to_numeric(short_orders_df[col])

        orders = []
        for set in dates:
            orders.extend(binance.margin_my_trades(symbol = symbol,
                                                   startTime = int(set['init_day']),
                                                   endTime = int(set['final_day'])))
        short_trades_df = pd.DataFrame(orders)
        if len(short_trades_df) > 0:
            for col in ['qty', 'quoteQty', 'commission', 'price']:
                short_trades_df[col] = pd.to_numeric(short_trades_df[col])
            short_trades_df = short_trades_df.groupby('orderId').agg({'qty': 'sum',
                                                                    'quoteQty': 'sum',
                                                                    'commission': 'sum',
                                                                    'symbol': 'first',
                                                                    'commissionAsset': 'first',
                                                                    'price': 'mean'}).reset_index()
        print(f'Data: Short Orders Obtained: {len(short_orders_df)} - Short Trades: {len(short_trades_df)}')
        print(f'Data: Long Orders Obtained: {len(long_orders_df)} - Long Trades: {len(long_trades_df)}')
        print(f'Data: Filtered Trades Obtained: {len(filtered_trades)}')
        return {'long_orders_df': long_orders_df, 'long_trades_df': long_trades_df,
                    'short_orders_df': short_orders_df, 'short_trades_df': short_trades_df,
                    'filtered_trades': filtered_trades}
    except Exception as e:
        msg = 'Error trying to get orders data'
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
        notificator(category = 'CRITICAL',
                    process = NAME,
                    line = exc_tb.tb_lineno,
                    msg = msg,
                    error = f'{exc_type}: {exc_obj}')

def transform(data: dict):
    try:
        long_orders_df = data['long_orders_df']
        long_trades_df = data['long_trades_df']
        short_orders_df = data['short_orders_df']
        short_trades_df = data['short_trades_df']
        filtered_trades = data['filtered_trades']
        if len(filtered_trades) > 0:
            list_of_trades = []
            # Extracting data from orders
            for row in filtered_trades.itertuples():
                s = trade_type = entry_time = entry_fee =\
                tp1_order = tp1_size = tp1_pct_return = tp1_time = tp1_duration = tp1_fee = tp1_pnl =\
                stop_order = stop_size = stop_pct_return = stop_exit_time = stop_duration = stop_fee = stop_pnl =\
                sell_order = sell_size = sell_pct_return = sell_time = sell_duration = sell_fee = sell_pnl =\
                breakeven_order = breakeven_size = breakeven_pct_return = breakeven_time = breakeven_duration =\
                breakeven_fee = breakeven_pnl = None

                trade_type = row.trade_type
                entry_order = long_orders_df[long_orders_df.orderId == row.entry_id] if trade_type == 'long' \
                    else short_orders_df[short_orders_df.orderId == row.entry_id]
                entry_trade = long_trades_df[long_trades_df.orderId == row.entry_id] if trade_type == 'long' \
                    else short_trades_df[short_trades_df.orderId == row.entry_id]
                s = entry_order['symbol'].values[0]
                entry_cost = entry_order['cummulativeQuoteQty'].values[0]
                entry_size = entry_order['executedQty'].values[0]
                weighted_average_cost = entry_cost / entry_size
                exit = row.exit
                date = entry_time = pd.to_datetime(row.entry_date, utc=True)
                entry_fee = entry_trade['commission'].values[0]
                entry_fee = entry_fee if entry_trade['commissionAsset'].values[0] == 'USDT' \
                    else entry_fee * entry_trade['price'].values[0]
                entry_fee = round(entry_fee, 10)

                if not pd.isnull(row.tp1_id):
                    tp1_order = long_orders_df[long_orders_df.orderId == row.tp1_id] if trade_type == 'long' \
                        else short_orders_df[short_orders_df.orderId == row.tp1_id]
                    tp1_trade = long_trades_df[long_trades_df.orderId == row.entry_id] if trade_type == 'long' \
                        else short_trades_df[short_trades_df.orderId == row.entry_id]
                    tp1_cost = tp1_order['cummulativeQuoteQty'].values[0]
                    tp1_size = tp1_order['executedQty'].values[0]
                    tp1_time = pd.to_datetime(tp1_order['updateTime'].values[0], unit = 'ms', utc= True)
                    tp1_duration = tp1_time - entry_time
                    tp1_fee = tp1_trade['commission'].values[0]
                    tp1_fee = tp1_fee if tp1_trade['commissionAsset'].values[0] == 'USDT' \
                        else tp1_fee * tp1_trade['price'].values[0]
                    tp1_fee = round(tp1_fee, 10)
                    if trade_type == 'long':
                        tp1_pnl = round((tp1_cost - weighted_average_cost * tp1_size) - (entry_fee + tp1_fee), 2)
                        tp1_pct_return = round(((tp1_pnl - (entry_fee + tp1_fee)) / entry_cost) * 100, 2)
                    else:
                        tp1_pnl = round((weighted_average_cost * tp1_size - tp1_cost) - (entry_fee + tp1_fee), 2)
                        tp1_pct_return = round(((tp1_pnl - (entry_fee + tp1_fee)) / tp1_cost) * 100, 2)

                if row.exit == 'stop_loss_exit':
                    stop_order = long_orders_df[long_orders_df.orderId == row.stop_id] if trade_type == 'long' \
                        else short_orders_df[short_orders_df.orderId == row.stop_id]
                    stop_trade = long_trades_df[long_trades_df.orderId == row.entry_id] if trade_type == 'long' \
                        else short_trades_df[short_trades_df.orderId == row.entry_id]
                    stop_cost = stop_order['cummulativeQuoteQty'].values[0]
                    stop_size = stop_order['executedQty'].values[0]
                    stop_exit_time = pd.to_datetime(stop_order['updateTime'].values[0], unit = 'ms', utc = True)
                    stop_duration = stop_exit_time - entry_time
                    stop_fee = stop_trade['commission'].values[0]
                    stop_fee = stop_fee if stop_trade['commissionAsset'].values[0] == 'USDT' \
                        else stop_fee * stop_trade['price'].values[0]
                    stop_fee = round(stop_fee, 10)
                    if trade_type =='long':
                        stop_pnl = round((stop_cost - weighted_average_cost * stop_size) - (entry_fee + stop_fee), 2)
                        stop_pct_return = round(((stop_pnl - (entry_fee + stop_fee)) / entry_cost) * 100, 2)
                    else:
                        stop_pnl = round((weighted_average_cost * stop_size - stop_cost) - (entry_fee + stop_fee), 2)
                        stop_pct_return = round(((stop_pnl - (entry_fee + stop_fee)) / stop_cost) * 100, 2)

                elif row.exit == 'strategy_exit' or row.exit == 'canceled':
                    sell_order = long_orders_df[long_orders_df.orderId == row.exit_id] if trade_type == 'long' \
                    else short_orders_df[short_orders_df.orderId == row.exit_id]
                    sell_trade = long_trades_df[long_trades_df.orderId == row.entry_id] if trade_type == 'long' \
                        else short_trades_df[short_trades_df.orderId == row.entry_id]
                    sell_cost = sell_order['cummulativeQuoteQty'].values[0]
                    sell_size = sell_order['executedQty'].values[0]
                    sell_time = pd.to_datetime(sell_order['updateTime'].values[0], unit = 'ms', utc = True)
                    sell_duration = sell_time - entry_time
                    sell_fee = sell_trade['commission'].values[0]
                    sell_fee = sell_fee if sell_trade['commissionAsset'].values[0] == 'USDT' \
                        else sell_fee * sell_trade['price'].values[0]
                    sell_fee = round(sell_fee, 10)
                    if trade_type =='long':
                        sell_pnl = round((sell_cost - weighted_average_cost * sell_size) - (entry_fee + sell_fee), 2)
                        sell_pct_return = round(((sell_pnl - (entry_fee + sell_fee)) / entry_cost) * 100, 2)
                    else:
                        sell_pnl = round((weighted_average_cost * sell_size - sell_cost) - (entry_fee + sell_fee), 2)
                        sell_pct_return = round(((sell_pnl - (entry_fee + sell_fee)) / sell_cost) * 100, 2)

                elif row.exit == 'breakeven_exit':
                    breakeven_order = long_orders_df[long_orders_df.orderId == row.breakeven_id] if trade_type == 'long' \
                        else short_orders_df[short_orders_df.orderId == row.breakeven_id]
                    breakeven_trade = long_trades_df[long_trades_df.orderId == row.entry_id] if trade_type == 'long' \
                        else short_trades_df[short_trades_df.orderId == row.entry_id]
                    breakeven_cost = breakeven_order['cummulativeQuoteQty'].values[0]
                    breakeven_size = breakeven_order['executedQty'].values[0]
                    breakeven_time = pd.to_datetime(breakeven_order['updateTime'].values[0], unit = 'ms', utc = True)
                    breakeven_duration = breakeven_time - entry_time
                    breakeven_fee = breakeven_trade['commission'].values[0]
                    breakeven_fee = breakeven_fee if breakeven_trade['commissionAsset'].values[0] == 'USDT' \
                        else breakeven_fee * breakeven_trade['price'].values[0]
                    breakeven_fee = round(breakeven_fee, 10)
                    if trade_type =='long':
                        breakeven_pnl = round((breakeven_cost - weighted_average_cost * breakeven_size) - (entry_fee + breakeven_fee), 2)
                        breakeven_pct_return = round(((breakeven_pnl - (entry_fee + breakeven_fee)) / entry_cost) * 100, 2)
                    else:
                        breakeven_pnl = round((weighted_average_cost * breakeven_size - breakeven_cost) - (entry_fee + breakeven_fee), 2)
                        breakeven_pct_return = round(((breakeven_pnl - (entry_fee + breakeven_fee)) / breakeven_cost) * 100, 2)

                total_pnl = Decimal(sum([float(i) if i != None else 0 for i in [tp1_pnl, stop_pnl, sell_pnl, breakeven_pnl]]))\
                        .quantize(Decimal(f'{0:.2f}'), rounding = ROUND_DOWN)

                list_of_trades.append([date, s, entry_size, trade_type, exit, entry_time, entry_fee,
                                    tp1_order, tp1_size, tp1_pct_return, tp1_time, tp1_duration, tp1_fee, tp1_pnl,
                                    stop_order, stop_size, stop_pct_return, stop_exit_time, stop_duration, stop_fee, stop_pnl,
                                    sell_order, sell_size, sell_pct_return, sell_time, sell_duration, sell_fee, sell_pnl,
                                    breakeven_order, breakeven_size, breakeven_pct_return, breakeven_time, breakeven_duration,
                                    breakeven_fee, breakeven_pnl, total_pnl])

            trades = pd.DataFrame(list_of_trades,
                            columns = ['date', 's', 'entry_size', 'trade_type', 'exit', 'entry_time', 'entry_fee',
                                        'tp1_order', 'tp1_size', 'tp1_pct_return', 'tp1_time', 'tp1_duration', 'tp1_fee', 'tp1_pnl',
                                        'stop_order', 'stop_size', 'stop_pct_return', 'stop_exit_time', 'stop_duration', 'stop_fee', 'stop_pnl',
                                        'sell_order', 'sell_size', 'sell_pct_return', 'sell_time', 'sell_duration', 'sell_fee', 'sell_pnl',
                                        'breakeven_order', 'breakeven_size', 'breakeven_pct_return', 'breakeven_time', 'breakeven_duration',
                                        'breakeven_fee', 'breakeven_pnl', 'total_pnl'])

            for col in ['tp1_duration', 'stop_duration', 'sell_duration', 'tp1_time', 'breakeven_duration']:
                trades[col] = [str(i).split(".")[0] for i in trades[col]]
            trades.set_index(trades['date'], inplace=True)

            trades.to_csv('./orders_haku/trades_haku.csv', mode='a', index=False)
        else:
            trades = pd.DataFrame(columns = ['date', 's', 'entry_size', 'trade_type', 'exit', 'entry_time', 'entry_fee',
                                             'tp1_order', 'tp1_size', 'tp1_pct_return', 'tp1_time', 'tp1_duration', 'tp1_fee', 'tp1_pnl',
                                             'stop_order', 'stop_size', 'stop_pct_return', 'stop_exit_time', 'stop_duration', 'stop_fee', 'stop_pnl',
                                             'sell_order', 'sell_size', 'sell_pct_return', 'sell_time', 'sell_duration', 'sell_fee', 'sell_pnl',
                                             'breakeven_order', 'breakeven_size', 'breakeven_pct_return', 'breakeven_time', 'breakeven_duration',
                                             'breakeven_fee', 'breakeven_pnl', 'total_pnl'])
        return trades
    except Exception:
        msg = 'Error trying to transform data'
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
        notificator(category = 'CRITICAL',
                    process = NAME,
                    line = exc_tb.tb_lineno,
                    msg = msg,
                    error = f'{exc_type}: {exc_obj}')

def make_report(trades: pd.DataFrame, candles: pd.DataFrame):
    try:
        if len(trades) > 0:
            candles_fig = go.Figure(data=[go.Candlestick(x=candles.index,
                                                open=candles['Open'],
                                                high=candles['High'],
                                                low=candles['Low'],close=candles['Close'])])
            for row in trades.itertuples():
                if not pd.isnull(row.tp1_size):
                    candles_fig.add_shape(type='line',
                                        x0 = row.entry_time,
                                        x1 = row.tp1_time,
                                        y0=0.90 if row.trade_type == 'long' else 0.1,
                                        y1=0.90 if row.trade_type == 'long' else 0.1,
                                        yref='paper',
                                        label=dict(text=f'PNL: {row.tp1_pnl} | {row.tp1_pct_return}%',
                                                    font=dict(size=10),
                                                    padding=6),
                                                    line_color = '#ef233c' if row.tp1_pnl < 0 else '#27a300',
                                                    line_width=5)
                if row.exit == 'stop_loss_exit':
                    candles_fig.add_shape(type='line',
                                        x0 = row.entry_time,
                                        x1 = row.stop_exit_time,
                                        y0=0.95 if row.trade_type == 'long' else 0.05,
                                        y1=0.95 if row.trade_type == 'long' else 0.05,
                                        yref='paper',
                                        label=dict(text=f'PNL: {row.stop_pnl} | {row.stop_pct_return}%',
                                                    font=dict(size=10),
                                                    padding=6),
                                                    line_color = '#ef233c' if row.stop_pnl < 0 else '#27a300',
                                                    line_width=5)

                elif row.exit == 'breakeven_exit':
                    candles_fig.add_shape(type='line',
                                        x0 = row.entry_time,
                                        x1 = row.breakeven_time,
                                        y0=0.95 if row.trade_type == 'long' else 0.05,
                                        y1=0.95 if row.trade_type == 'long' else 0.05,
                                        yref='paper',
                                        label=dict(text=f'PNL: {row.breakeven_pnl} | {row.breakeven_pct_return}%',
                                                    font=dict(size=10),
                                                    padding=6),
                                                    line_color = '#ef233c' if row.breakeven_pnl < 0 else '#27a300',
                                                    line_width=5)
                elif row.exit == 'canceled':
                    candles_fig.add_shape(type='line',
                                        x0 = row.entry_time,
                                        x1 = row.sell_time,
                                        y0=0.95 if row.trade_type == 'long' else 0.05,
                                        y1=0.95 if row.trade_type == 'long' else 0.05,
                                        yref='paper',
                                        label=dict(text=f'PNL: {row.sell_pnl} | {row.sell_pct_return}%',
                                                    font=dict(size=10),
                                                    padding=6),
                                                    line_color = '#ef233c' if row.sell_pnl < 0 else '#27a300',
                                                    line_width=5)
                elif row.exit == 'strategy_exit' or row.exit == 'canceled':
                    candles_fig.add_shape(type='line',
                                        x0 = row.entry_time,
                                        x1 = row.sell_time,
                                        y0=0.95 if row.trade_type == 'long' else 0.05,
                                        y1=0.95 if row.trade_type == 'long' else 0.05,
                                        yref='paper',
                                        label=dict(text=f'PNL: {row.sell_pnl} | {row.sell_pct_return}%',
                                                    font=dict(size=10),
                                                    padding=6),
                                                    line_color = '#ef233c' if row.sell_pnl < 0 else '#27a300',
                                                    line_width=5)
            candles_fig.update_layout(width=1800, height=1200 , plot_bgcolor='#e9ecef')
            candles_fig.update_layout(margin=dict(l=10,r=10,b=10,t=45,pad=4),
                                    title = f'Trades del período: {date0} \u2794 {date1}')
            candles_fig.update(layout_xaxis_rangeslider_visible=False)
            # Saving candle graph
            candles_fig.write_image(f'img_haku/{date0}-{date1}-candles.png')


            columns = ['s', 'trade_type', 'exit', 'entry_size', 'tp1_pnl', 'stop_pnl', 'breakeven_pnl', 'sell_pnl', 'total_pnl']
            column_names = ['Par', 'Tipo', 'Método de Salida', 'Tamaño', 'PNL TakeProfit 1', 'PNL STOPLOSS', 'PNL BREAKEVEN', 'PNL EXIT', 'PNL Total']
            columns = [trades[column] for column in columns]
            colors = ['#2F8F4A' if row > 0 else '#DB5B32' for row in trades['total_pnl'].to_list()]
            table_height = (len(trades) * 23) + 80

            trades_table = go.Figure(go.Table(header=dict(values = column_names,
                                                        font = dict(color='black', size=13),
                                                        align='center',
                                                        fill_color = '#b5c1cd'),
                                                        cells=dict(values = columns,
                                                                    font = dict(color='black', size=11),
                                                                    fill_color = [colors],
                                                                    align='center')))
            trades_table.update_layout(width=1800, height = table_height,
                                    title = f'Trades del período: {date0} \u2794 {date1}',
                                    margin=dict(l=10,r=10,b=10,t=45,pad=4))

            # Saving table
            trades_table.write_image(f'img_haku/{date0}-{date1}-table.png')

            return {'complete': True, 'trades': len(trades), 'pnl': round(trades['total_pnl'].sum(), 2)}
        else:
            return {'complete': False}
    except Exception:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'Error trying to build report'
        logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
        notificator(category = 'CRITICAL',
                    process = NAME,
                    line = exc_tb.tb_lineno,
                    msg = msg,
                    error = f'{exc_type}: {exc_obj}')


def send_report(report, /):
    import discord
    from discord.ext import commands

    token = os.environ.get('DCTOKEN')

    intents = discord.Intents.default()
    intents.members = True
    intents.message_content = True

    bot = commands.Bot(command_prefix='!', description='Reporter', intents=intents)

    @bot.event
    async def on_ready():
        try:
            """
            Function that connect with Discord Server, and send the daily report
            """
            print(f'Logged in as {bot.user} (ID: {bot.user.id})')
            c = bot.get_channel(channel)
            if report['complete'] == True:
                weekly = weekly_balance()
                await c.send(f'### **Reporte del período {date0} \u2794 {date1}:** \
                            \nSe ejecutaron **{report["trades"]}** Trades Completos. \
                            \nEl valor PnL (Perdidas y Ganancias) fue de **{report["pnl"]}** USD')
                await c.send('El cambio del balance en la semana fue de:'
                             f'\nCuenta Spot: {weekly["weekly_spot_dif"]} \u2794 {weekly["weekly_spot_dif_per"]}%'
                             f'\nCuenta Margin: {weekly["weekly_margin_dif"]} \u2794 {weekly["weekly_margin_dif_per"]}%'
                             f'\nTotal: {weekly["weekly_total_dif"]} \u2794 {weekly["weekly_total_dif_per"]}%')
                await c.send(file=discord.File(f'img_haku/{date0}-{date1}-table.png'))
                await c.send(file=discord.File(f'img_haku/{date0}-{date1}-candles.png'))

            else:
                await c.send(f'### **Reporte del período {date0} \u2794 {date1}:** \
                            \nNo se realizaron Trades Completos.')

        except Exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error trying to send the report'
                logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}: {exc_obj}')
        await bot.close()
    bot.run(token)
    exit()

if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('haku.conf')
    real_trading_mode = config.getboolean('BotMode', 'RealTradingMode')
    pair = config.get('BotMode', 'pair')
    interval = config.get('BotMode', 'interval')
    days_interval = config.get('Reporter', 'days_interval')

    today = datetime.today()
    final_date = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc)
    initial_date = final_date - timedelta(days = 30)
    date0 = initial_date.strftime("%Y-%m-%d")
    date1 = final_date.strftime("%Y-%m-%d")

    channel = set_channel('haku_reporter')
    binance = set_credentials(real_trading_mode = real_trading_mode, k = 'HAKUBK', s = 'HAKUBS')
    data = check_data(symbol = pair)
    trades = transform(data)
    candles = extract_candles(timeframe = interval, symbol = pair)
    report = make_report(trades, candles)
    send_report(report)