import websocket

import talib as ta
import pandas as pd
import pandas_ta
from decimal import Decimal, ROUND_DOWN

import signal
import sys
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import json
from binance.error import ClientError

import requests
from discord import SyncWebhook

from binance_utilities.notificator import notificator
from binance_utilities.credentials import set_credentials
from binance_utilities.utils import get_past_candles
from binance_utilities.channels import set_channel


lf = 'logs_haku/haku.log'
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG,
                    handlers=[RotatingFileHandler(filename=lf, maxBytes=150 * (10**6), backupCount=5, mode='a')])

NAME = 'HakuBot'


class Bot():
    # Class that defines the trading bot
    def __init__(self, symbol: str,
                 interval: str,
                 margin_mode: bool,
                 dinamic_sizing: bool,
                 dinamic_size: int,
                 tp1_percent_size: int,
                 static_size: int = None):

        self.symbol = symbol
        self.interval = interval
        self.margin_mode = margin_mode
        self.dinamic_sizing = dinamic_sizing
        self.static_size = static_size
        self.limit_periods = 60
        self.size = dinamic_size/100
        self.minor_size_per = tp1_percent_size/100

        self.bruteforce_close = False
        self.online = True

        self.webhook_web = set_channel() if not real_trading_mode else set_channel('HAKU')
        signal.signal(signal.SIGTERM, self.panic_button_closer)

    def trade_notificator(self, date, mode, stage, action, symbol, quantity):
        """
        Send trade notifications to Discord Channel.

        Args:
            date (datetime):    Date of the close of the operation.
            mode (str):         Mode of the trade: Long or Short.
            stage (str):        State of the trade. e.g. Entry or TakeProfit
            action (str):       Which Order has been executed.
            symbol (str):       Symbol of the operation.
            quantity (str):     Quantity of the Operation
        """
        with requests.Session() as s:
            self.webhook = SyncWebhook.from_url(url=self.webhook_web, session=s)
            self.webhook.send(f'UTCDate: *{date}* - {mode} - {stage} - {action}: Symbol: **{symbol}** - Quantity: **{quantity}**')

    def get_size_to_trade(self, mode: str):
        """
        Calculates the balance of the account, then calculate the size to make the operation.

        Args:
            mode (str):         If long or short.

        Return:
            entry_size (float): Size of the entry.
            exit_size (float):  Size of the exit if not takeprofit.
            major_size (float): Size of the exit if takeprofit.
            minor_size (float): Size of the second exit if takeprofit is executed.

        """
        @retry(stop=stop_after_attempt(3), retry = retry_if_exception_type(ClientError), reraise = True)
        def _get_size_to_trade():
            if mode == 'long':
                for element in self.binance.account()['balances']:
                    if element['asset'] == 'USDT':
                        usdt = element['free']
                for element in self.binance.account()['balances']:
                    if element['asset'] == self.symbol.rstrip('USDT'):
                        symbol_balance = element['free']
            else:
                for element in self.binance.margin_account()['userAssets']:
                    if element['asset'] == 'USDT':
                        usdt = element['free']
                for element in self.binance.margin_account()['userAssets']:
                    if element['asset'] == self.symbol.rstrip('USDT'):
                        symbol_balance = element['free']

            if self.dinamic_sizing:
                size = float(usdt) * self.size / float(self.prepared_data['Close'][-1])
            else:
                size = self.static_size / float(self.prepared_data['Close'][-1])
            entry_size = Decimal(size).quantize(Decimal(f'{0:.{self.lot_size_steps}f}'), rounding = ROUND_DOWN)
            entry_size_fee_out = Decimal(float(entry_size) * (1 - self.commisions))\
                .quantize(Decimal(f'{0:.{self.lot_size_steps}f}'), rounding = ROUND_DOWN)
            exit_size = entry_size if (float(symbol_balance) + float(entry_size_fee_out)) > entry_size else entry_size_fee_out
            minor_size = Decimal(float(exit_size) * self.minor_size_per)\
                .quantize(Decimal(f'{0:.{self.lot_size_steps}f}'), rounding = ROUND_DOWN)
            major_size = Decimal(exit_size - minor_size)\
                .quantize(Decimal(f'{0:.{self.lot_size_steps}f}'), rounding = ROUND_DOWN)
            print(f'{entry_size} - {major_size} - {minor_size}')
            return float(entry_size), float(exit_size), float(major_size), float(minor_size)
        try:
            return _get_size_to_trade()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error trying to get size'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def get_margin_interest(self, percentage:int = 100):
        """
        Returns the amount of capital that is in debt with the exchange.

        Args:
            percentage (int): Percentage to pay.
        """
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), retry = retry_if_exception_type(ClientError), reraise = True)
        def _get_margin_interest():
            margin_data = self.binance.margin_account()
            for i in margin_data['userAssets']:
                if i['asset'] == self.symbol.rstrip('USDT'):
                    element = i
            interest = float(element['netAsset']) * (percentage/100)
            interest = Decimal(interest)\
                .quantize(Decimal(f'{0:.{self.lot_size_steps}f}'), rounding = ROUND_DOWN)
            interest = abs(interest)
            return str(interest)
        try:
            return _get_margin_interest()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error trying to get margin interest'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def get_exchange_info(self):
        """
        Returns the commissions rates from operations and lot size restrictions.

        Returns:
            self.lot_size_steps: set the lot restriction.
            self.commisions:     set the commisions.
        """
        @retry(stop=stop_after_attempt(3), retry = retry_if_exception_type(ClientError), reraise = True)
        def _get_exchange_info():
            data = self.binance.exchange_info(symbol = self.symbol)['symbols'][0]
            lot_size_steps = len(data['filters'][1]['stepSize'].replace('.','')[1:].rstrip('0'))
            commisions = float(self.binance.account()['commissionRates']['taker'])
            self.lot_size_steps, self.commisions = lot_size_steps, commisions
        try:
            return _get_exchange_info()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error trying to get exchange info'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def get_initial_data(self):
        """
        Function that get previous data to calculate past candles

        Action:
            Save in self.raw_data the previous candles to calculate indicators.
        """
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), retry = retry_if_exception_type(ClientError), reraise = True)
        def _get_initial_data():
            self.raw_data = get_past_candles(symbol = self.symbol,
                                             interval = self.interval,
                                             limit = self.limit_periods)
            logging.info('Initial Data getted')
        try:
            _get_initial_data()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error trying to get initial data'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def data_preparation(self, dataframe):
        """
        Function that prepare the data and applies indicators

        Actions:
            Performs indicator calculations and saves them in self.prepared_data.

        """
        def supertrend_ind(df):
            length = 10
            multiplier = 1.8

            st_df = pandas_ta.supertrend(df['High'], df['Low'], df['Close'], length=length, multiplier=multiplier)
            return st_df[f'SUPERTd_10_{multiplier}']

        def ash_ind(df):
            length = 16
            smooth = 4

            price1 = df['Close']
            price2 = df['Close'].shift(1)

            bulls = ((price1-price2).abs()+(price1-price2)) * 0.5
            bears = ((price1-price2).abs()-(price1-price2)) * 0.5

            avg_bulls = ta.EMA(bulls, length)
            avg_bears = ta.EMA(bears, length)

            smooth_bulls = ta.EMA(avg_bulls, smooth)
            smooth_bears = ta.EMA(avg_bears, smooth)

            difference = (smooth_bulls - smooth_bears).abs()

            return smooth_bulls, smooth_bears, difference

        def atr_sl_tp(df, multiplier: float = 1.5, period: int = 14):
            atr = ta.ATR(df['High'], df['Low'], df['Close'], timeperiod=period)

            atr_up = df['Close'] + atr * multiplier
            atr_dn = df['Close'] - atr * multiplier

            return atr_up.round(2), atr_dn.round(2)

        @retry(stop=stop_after_attempt(3), retry = retry_if_exception_type(ClientError), reraise = True)
        def _data_preparation():
            df = dataframe.tail(100).copy()
            # Appling indicators
            df['smooth_bulls'], df['smooth_bears'], df['difference'] = ash_ind(df)
            df['supertrend_ind'] = supertrend_ind(df)
            df['atr_up_1.5'], df['atr_dn_1.5'] = atr_sl_tp(df, 1.5, 14)
            df['atr_up_1'], df['atr_dn_1'] = atr_sl_tp(df, 1, 14)
            self.prepared_data = df
            logging.info('Data prepared')

        try:
            _data_preparation()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'Error trying to prepare data'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def strategy(self, df):
        """
        Function that apply the strategy to the data and calculates if an order should be executed
        """

        def entry_long_conditions():
            supertrend_condition = df['supertrend_ind'][-1] == 1
            ash_condition = all([df['smooth_bulls'][-1] > df['smooth_bulls'][-2],
                                 df['difference'][-1]   > df['smooth_bears'][-1]])
            return all([supertrend_condition, ash_condition])

        def exit_long_conditions():
            supertrend_condition = df['supertrend_ind'][-1] == -1
            mode = self.trade['trade_mode'] == 'long'
            return all([supertrend_condition, mode])

        def entry_short_conditions():
            supertrend_condition = df['supertrend_ind'][-1] == -1
            ash_condition = all([df['smooth_bears'][-1] > df['smooth_bears'][-2],
                                 df['difference'][-1]   > df['smooth_bulls'][-1]])
            return all([supertrend_condition, ash_condition, self.margin_mode])

        def exit_short_conditions():
            supertrend_condition = df['supertrend_ind'][-1] == 1
            mode = self.trade['trade_mode'] == 'short'
            return all([supertrend_condition, mode])

        def tp1_checker():
            if self.trade['trade_mode'] == 'long' and not self.trade['tp1_executed']:
                return df['High'][-1] > self.trade['tp_1']
            elif self.trade['trade_mode']  == 'short' and not self.trade['tp1_executed']:
                return df['Low'][-1] < self.trade['tp_1']

        def stop_loss_checker():
            if self.trade['trade_mode'] == 'long':
                return df['Low'][-1] < self.trade['stoploss_price']
            elif self.trade['trade_mode']  == 'short':
                return df['High'][-1] > self.trade['stoploss_price']

        def breakeven_checker():
            if self.trade['trade_mode'] == 'long':
                return df['Low'][-1] < self.trade['breakeven_price']
            elif self.trade['trade_mode']  == 'short':
                return df['High'][-1] > self.trade['breakeven_price']


        def entry_long():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                # Calculate pre data to trade
                self.trade['entry_size'], self.trade['exit_size'], self.trade['major_size'], self.trade['minor_size'] = self.get_size_to_trade(mode = 'long')
                self.trade['trade_mode'] = 'long'
                self.trade['tp_1'] = df['atr_up_1'][-1]
                self.trade['stoploss_price'] = df['atr_dn_1.5'][-1]
                self.trade['breakeven_price'] = round(df['Close'][-1], 2)
                # Executing Buy Order

                long_entry_order = self.binance.new_order(symbol = self.symbol,
                                                          type = 'MARKET',
                                                          side = 'BUY',
                                                          quantity = self.trade['entry_size'])
                self.trade['entry_time'] = long_entry_order['transactTime']
                self.trade['entry_id'] = long_entry_order['orderId']

                logging.info(f'Long - ENTRY STAGE - BUY ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {self.trade["entry_size"]}')
                self.trade_notificator(date = pd.Timestamp(long_entry_order['transactTime'], unit='ms'),
                                       mode = 'Long',
                                       stage = 'ENTRY STAGE',
                                       action = 'BUY ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = self.trade["entry_size"])
            try:
                _trade()
            except Exception:
                msg = 'The BUY ORDER cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')



        def entry_short():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                self.trade['entry_size'], self.trade['exit_size'], self.trade['major_size'], self.trade['minor_size'] = self.get_size_to_trade(mode = 'short')
                self.trade['trade_mode'] = 'short'
                self.trade['tp_1'] = df['atr_dn_1'][-1]
                self.trade['stoploss_price'] = df['atr_up_1.5'][-1]
                self.trade['breakeven_price'] = round(df['Close'][-1], 2)
                # Executing Entry Short Order
                short_entry_order = self.binance.new_margin_order(symbol = self.symbol,
                                                                  side = 'SELL',
                                                                  type = 'MARKET',
                                                                  quantity = self.trade['entry_size'],
                                                                  sideEffectType = 'MARGIN_BUY',
                                                                  isIsolated = 'FALSE')
                self.trade['entry_time'] = short_entry_order['transactTime']
                self.trade['entry_order'] = short_entry_order
                self.trade['entry_id'] = short_entry_order['orderId']
                logging.info(f'Short - ENTRY STAGE - MARGIN SELL ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {self.trade["entry_size"]}')
                self.trade_notificator(date = pd.Timestamp(short_entry_order['transactTime'], unit='ms'),
                                       mode = 'Short',
                                       stage = 'ENTRY STAGE',
                                       action = 'MARGIN SELL ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = self.trade["entry_size"])
            try:
                _trade()
            except Exception:
                msg = 'The MARGIN SELL ORDER cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def long_stoploss():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                stop_order = self.binance.new_order(symbol = self.symbol,
                                                    type = 'MARKET',
                                                    side = 'SELL',
                                                    quantity = self.trade['exit_size'])
                self.trade['stop_id'] = stop_order['orderId']
                logging.info(f'Long - EXIT STAGE - STOPLOSS ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {self.trade["exit_size"]}')
                self.trade_notificator(date = pd.Timestamp(stop_order['transactTime'], unit='ms'),
                                       mode = 'Long',
                                       stage = 'EXIT STAGE',
                                       action = 'STOPLOSS ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = self.trade["exit_size"])
                self.trade['exit'] = 'stop_loss_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'The STOPLOSS EXIT cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def long_tp1():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                tp1_order = self.binance.new_order(symbol = self.symbol,
                                                   type = 'MARKET',
                                                   side = 'SELL',
                                                   quantity = self.trade['minor_size'])
                self.trade['tp1_id'] = tp1_order['orderId']
                self.trade['tp1_executed'] = True
                logging.info(f'Long - TAKEPROFIT1 STAGE - SELL ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {self.trade["minor_size"]}')
                self.trade_notificator(date = pd.Timestamp(tp1_order['transactTime'], unit='ms'),
                                       mode = 'Long',
                                       stage = 'TAKEPROFIT1 STAGE',
                                       action = 'SELL ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = self.trade["minor_size"])
            try:
                _trade()
            except Exception:
                msg = 'The TAKEPROFIT 1 cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def long_breakeven():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                breakeven_order = self.binance.new_order(symbol = self.symbol,
                                                         type = 'MARKET',
                                                         side = 'SELL',
                                                         quantity = self.trade['major_size'])
                self.trade['breakeven_id'] = breakeven_order['orderId']
                logging.info(f'Long - EXIT STAGE - BREAKEVEN ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {self.trade["major_size"]}')
                self.trade_notificator(date = pd.Timestamp(breakeven_order['transactTime'], unit='ms'),
                                       mode = 'Long',
                                       stage = 'EXIT STAGE',
                                       action = 'BREAKEVEN ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = self.trade["major_size"])
                self.trade['exit'] = 'breakeven_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'The STOPLOSS EXIT cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def long_exit():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                size = self.trade['major_size'] if self.trade['tp1_executed'] else self.trade['exit_size']
                sell_order = self.binance.new_order(symbol = self.symbol,
                                                    type = 'MARKET',
                                                    side = 'SELL',
                                                    quantity = size)
                self.trade['exit_id'] = sell_order['orderId']
                logging.info(f'Long - EXIT STAGE - SELL EXIT ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {size}')
                self.trade_notificator(date = pd.Timestamp(sell_order['transactTime'], unit='ms'),
                                       mode = 'Long',
                                       stage = 'EXIT STAGE',
                                       action = 'SELL EXIT ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = size)
                self.trade['exit'] = 'strategy_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'The SELL EXIT cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def short_stoploss():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                exit_with_fee = self.get_margin_interest()
                buy_order = self.binance.new_margin_order(symbol = self.symbol,
                                                          type = 'MARKET',
                                                          side = 'BUY',
                                                          quantity = exit_with_fee,
                                                          isIsolated = 'FALSE',
                                                          sideEffectType = 'AUTO_REPAY')
                self.trade['stop_id'] = buy_order['orderId']
                logging.info(f'Short - EXIT STAGE - STOPLOSS ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {exit_with_fee}')
                self.trade_notificator(date = pd.Timestamp(buy_order['transactTime'], unit='ms'),
                                       mode = 'Short',
                                       stage = 'EXIT STAGE',
                                       action = 'STOPLOSS ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = exit_with_fee)
                self.trade['exit'] = 'stop_loss_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'Error executing BUY ORDER'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def short_tp1():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                exit_size = self.get_margin_interest(percentage=tp1_percent_size)
                tp1_order =  self.binance.new_margin_order(symbol = self.symbol,
                                                           type = 'MARKET',
                                                           side = 'BUY',
                                                           quantity = exit_size,
                                                           sideEffectType = 'AUTO_REPAY',
                                                           isIsolated = 'FALSE')
                logging.info(f'Short - TAKEPROFIT1 - BUY MARGIN ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {exit_size}')
                self.trade_notificator(date = pd.Timestamp(tp1_order['transactTime'], unit='ms'),
                                       mode = 'Short',
                                       stage = 'TAKEPROFIT1',
                                       action = 'BUY MARGIN ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = exit_size)
                self.trade['tp1_executed'] = True
                self.trade['tp1_id'] = tp1_order['orderId']
            try:
                _trade()
            except Exception:
                msg = 'SHORT - The TAKEPROFIT ORDER cannot be executed'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def short_breakeven():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                exit_size = self.get_margin_interest()
                breakeven_order = self.binance.new_margin_order(symbol = self.symbol,
                                                                type = 'MARKET',
                                                                side = 'BUY',
                                                                quantity = exit_size,
                                                                isIsolated = 'FALSE',
                                                                sideEffectType = 'AUTO_REPAY')
                logging.info(f'Short - EXIT STAGE - BUY MARGIN ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {exit_size}')
                self.trade_notificator(date = pd.Timestamp(breakeven_order['transactTime'], unit='ms'),
                                       mode = 'Short',
                                       stage = 'EXIT STAGE',
                                       action = 'BUY MARGIN ORDER EXECUTED',
                                       symbol = self.symbol,
                                       quantity = exit_size)
                self.trade['breakeven_id'] = breakeven_order['orderId']
                self.trade['exit'] = 'breakeven_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'Error executing the BREAKEVEN ORDER'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        def short_exit():
            @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
            def _trade():
                exit_with_fee = self.get_margin_interest()
                buy_order = self.binance.new_margin_order(symbol = self.symbol,
                                                          type = 'MARKET',
                                                          side = 'BUY',
                                                          quantity = exit_with_fee,
                                                          isIsolated = 'FALSE',
                                                          sideEffectType = 'AUTO_REPAY')
                self.trade['exit_id'] = buy_order['orderId']
                logging.info(f'Short - EXIT STAGE - BUY AUTO_REPAY ORDER EXECUTED: Symbol: {self.symbol} - '
                             f'Quantity: {exit_with_fee}')
                self.trade_notificator(date = pd.Timestamp(buy_order['transactTime'], unit='ms'),
                                       mode = 'Short',
                                       stage = 'EXIT STAGE',
                                       action = 'BUY ORDER AUTO_REPAY EXECUTED',
                                       symbol = self.symbol,
                                       quantity = exit_with_fee)
                self.trade['exit'] = 'strategy_exit'
                self.save_trade()
                self.reset_trade()
            try:
                _trade()
            except Exception:
                msg = 'Error executing BUY ORDER'
                exc_type, exc_obj, exc_tb = sys.exc_info()
                err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
                logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}({exc_obj.error_code}): {err_msg}')


        # If there is not running position, create a new one
        if self.trade['trade_mode'] == None:

            if entry_long_conditions():
                entry_long()

            if entry_short_conditions():
                entry_short()
            logging.info('Strategy executed')
            return

        # If there is a running position, check if an exit is available
        elif self.trade['trade_mode'] == 'long':
            if stop_loss_checker():
                long_stoploss()

            if tp1_checker():
                long_tp1()
                return
            if self.trade['tp1_executed']:
                if breakeven_checker():
                    long_breakeven()

            if exit_long_conditions():
                long_exit()

        elif self.trade['trade_mode'] == 'short':
            if stop_loss_checker():
                short_stoploss()

            if tp1_checker():
                short_tp1()
                return
            if self.trade['tp1_executed']:
                if breakeven_checker():
                    short_breakeven()

            if exit_short_conditions():
                short_exit()

        logging.info('Strategy executed')


    def cancel_trade(self):
        """
        Function that cancel the trade
        """
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
        def _cancel_long():
            size = self.trade['exit_size'] if not self.trade['tp1_executed'] else self.trade['major_size']
            cancel_long_order = self.binance.new_order(symbol = self.symbol,
                                                       type = 'MARKET',
                                                       side = 'SELL',
                                                       quantity = size)
            self.trade['exit_id'] = cancel_long_order['orderId']
            self.trade['exit'] = 'canceled'
            self.save_trade()
            logging.info(f'Long - CANCEL STAGE - SELL ORDER EXECUTED: Symbol: {self.symbol} - '
                         f'Quantity: {size}')
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
        def _cancel_short():
            exit_with_fee = self.get_margin_interest()
            buy_order = self.binance.new_margin_order(symbol = self.symbol,
                                                      type = 'MARKET',
                                                      side = 'BUY',
                                                      quantity = exit_with_fee,
                                                      isIsolated = 'FALSE',
                                                      sideEffectType = 'AUTO_REPAY')
            self.trade['exit_id'] = buy_order['orderId']
            self.trade['exit'] = 'canceled'
            self.save_trade()
            logging.info(f'Short - CANCEL STAGE - BUY ORDER EXECUTED: Symbol: {self.symbol} - '
                         f'Quantity: {exit_with_fee}')
        try:
            if self.trade['trade_mode'] == 'long':
                _cancel_long()
            elif self.trade['trade_mode'] == 'short':
                _cancel_short()
        except Exception:
            msg = f'Error cancelling {self.trade["trade_mode"].upper()} Trade'
            exc_type, exc_obj, exc_tb = sys.exc_info()
            err_msg = exc_obj.error_message if hasattr(exc_obj, 'error_message') else 'No message'
            logging.critical(f'{msg}: {exc_type}: {err_msg} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}({exc_obj.error_code}): {err_msg}')

    def reset_trade(self):
        self.trade = {'tp1_executed': False,
                      'exit_executed': False,
                      'entry_id': None,
                      'stop_id': None,
                      'exit_id': None,
                      'tp1_id': None,
                      'breakeven_id': None,
                      'exit': None,
                      'size': None,
                      'tp_1': None,
                      'stoploss_price': None,
                      'trade_mode': None}

        logging.info('Trade reset')


    def save_trade(self):
        """
        Saving the trades

        Actions:
            Extract from self.trade the data from the operation and save into .csv file.
        """
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(5), retry = retry_if_exception_type(ClientError), reraise = True)
        def _save_trade():
            report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            entry_date = datetime.fromtimestamp(self.trade['entry_time']/1000).strftime("%Y-%m-%d %H:%M:%S")
            mode = self.trade['trade_mode']
            exit = self.trade['exit']
            entry_id = self.trade['entry_id']
            stop_id = self.trade['stop_id']
            exit_id = self.trade['exit_id']
            tp1_id = self.trade['tp1_id']
            breakeven_id = self.trade['breakeven_id']

            with open(f'orders_haku/orders_haku.csv', mode='a') as f:
                f.write(f'{report_date},{entry_date},{mode},{exit},{entry_id},{stop_id},'
                        f'{exit_id},{tp1_id},{breakeven_id}\n')
            logging.info('Trade saved')
        try:
            _save_trade()
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            msg = 'The trade cannot be saved'
            logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        line = exc_tb.tb_lineno,
                        msg = msg,
                        error = f'{exc_type}: {exc_obj}')

    def panic_button_closer(self, *args):
        self.bruteforce_close = True
        self.online = False
        self.cancel_trade()
        self.ws.close()


    def start_trading(self, real_trading_mode = False):
        """
        Starting the bot.
        """

        def on_message(ws, msg):
            """
            Recives the data from websocket and send into dataset.
            """
            msg = json.loads(msg)
            event_time = pd.to_datetime(msg['E'], unit = 'ms')
            start_time = pd.to_datetime(msg['k']['t'], unit = 'ms')
            open     = float(msg['k']['o'])
            high     = float(msg['k']['h'])
            low      = float(msg['k']['l'])
            close    = float(msg['k']['c'])
            volume   = float(msg['k']['v'])
            complete = msg['k']['x']

            if complete == True:
                print(f'Symbol: {self.symbol.upper()} | Time: {event_time} | Price: {close}')
                self.raw_data.loc[start_time] = [open, high, low, close, volume]
                self.data_preparation(self.raw_data)
                self.strategy(self.prepared_data)

        def on_error(ws, error):
            if error != None:
                logging.error(f'{type(error)}: {error}')
                notificator(category = 'ERROR',
                            process = NAME,
                            msg = f'{error}')


        def on_close(ws, close_status_code, close_msg):
            if self.bruteforce_close:
                self.cancel_trade()
                msg = 'Closing BOT by BruteForce'
            else:
                msg = f'Closing by Status: {close_status_code}: {close_msg}'
            logging.critical(f'{msg}')
            notificator(category = 'CRITICAL',
                        process = NAME,
                        msg = msg)


        def on_open(ws):
            logging.info('Starting DATA STREAMING')
            notificator(category = 'SUCCESSFUL',
                        process = NAME,
                        msg = f'Successful Connection')

        if real_trading_mode:
            ws_endpoint = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@kline_{self.interval}"
        else:
            ws_endpoint = f"wss://testnet.binance.vision/ws/{self.symbol.lower()}@kline_{self.interval}"

        self.binance = set_credentials(real_trading_mode = real_trading_mode,
                                       k = 'HAKUBK',
                                       s = 'HAKUBS')
        self.reset_trade()
        self.get_initial_data()
        self.get_exchange_info()

        websocket.enableTrace(False)

        while self.online:
            try:
                logging.info('Starting Trading Bot...')
                self.ws = websocket.WebSocketApp(ws_endpoint,
                                                 on_open = on_open,
                                                 on_message = on_message,
                                                 on_close = on_close,
                                                 on_error = on_error)
                self.ws.run_forever()
            except Exception:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                msg = 'Error trying to set the connection with Exchange'
                logging.critical(f'{msg}: {exc_type}: {exc_obj} - Line: {exc_tb.tb_lineno}')
                notificator(category = 'CRITICAL',
                            process = NAME,
                            line = exc_tb.tb_lineno,
                            msg = msg,
                            error = f'{exc_type}: {exc_obj}')
        exit()


if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('haku.conf')
    real_trading_mode = config.getboolean('BotMode', 'RealTradingMode')
    margin_mode = config.getboolean('BotMode', 'margin')
    pair = config.get('BotMode', 'pair')
    interval = config.get('BotMode', 'interval')
    tp1_percent_size = config.getfloat('SizeConfiguration', 'tp1_percent_size')
    dinamic_sizing = config.getboolean('DinamicSizingConfiguration', 'dinamic_sizing')
    dinamic_size = config.getfloat('DinamicSizingConfiguration', 'dinamic_size')
    static_size = config.getfloat('DinamicSizingConfiguration', 'static_size')


    bot = Bot(symbol = pair,
              interval = interval,
              margin_mode = margin_mode,
              dinamic_sizing = dinamic_sizing,
              static_size = static_size,
              dinamic_size = dinamic_size,
              tp1_percent_size = tp1_percent_size)
    bot.start_trading(real_trading_mode = real_trading_mode)