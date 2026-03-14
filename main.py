import akshare as ak
import pandas as pd
import talib
import time
import requests
import urllib.parse
import hashlib
import hmac
import base64
from datetime import datetime

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json

import strategy

# --- 日志配置 ---
def setup_logger():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logger = logging.getLogger("StockMonitor")
    logger.setLevel(logging.INFO)
    
    # 避免重复添加handler
    if not logger.handlers:
        # 控制台输出
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # 文件输出，每日轮换
        log_file = os.path.join(log_dir, 'monitor.log')
        fh = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        
        logger.addHandler(ch)
        logger.addHandler(fh)
        
    return logger

logger = setup_logger()

class Stock:
    def __init__(self, ticker, name, judge_buy_ids, judge_sell_ids):
        self.ticker = ticker
        self.name = name
        # 支持传入单个 ID 或 ID 列表
        self.judge_buy_ids = [judge_buy_ids] if isinstance(judge_buy_ids, int) else judge_buy_ids
        self.judge_sell_ids = [judge_sell_ids] if isinstance(judge_sell_ids, int) else judge_sell_ids

class ETFMonitor:
    def __init__(self, stocks_file='stocks.json', dingtalk_webhook=None, dingtalk_secret=None):
        self.stocks_file = stocks_file
        self.stocks = []
        self.dingtalk_webhook = dingtalk_webhook
        self.dingtalk_secret = dingtalk_secret
        self.load_stocks_from_file() # 初始化加载
    
    def load_stocks_from_file(self):
        """从文件动态加载股票列表"""
        try:
            if not os.path.exists(self.stocks_file):
                logger.error(f"股票配置文件 {self.stocks_file} 不存在")
                return
            
            with open(self.stocks_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                new_stocks = []
                for item in data:
                    stock = Stock(
                        ticker=item['ticker'],
                        name=item['name'],
                        judge_buy_ids=item['judge_buy_ids'],
                        judge_sell_ids=item['judge_sell_ids']
                    )
                    new_stocks.append(stock)
                
                # 简单判断是否有变化并更新
                if len(new_stocks) != len(self.stocks) or \
                   [s.ticker for s in new_stocks] != [s.ticker for s in self.stocks]:
                    self.stocks = new_stocks
                    logger.info(f"成功加载 {len(self.stocks)} 只监控股票")
        except Exception as e:
            logger.error(f"加载股票配置文件时出错: {e}")
    
    def send_dingtalk_alert(self, message):
        """发送钉钉告警"""
        if self.dingtalk_webhook and self.dingtalk_secret:
            try:
                # 构造签名
                timestamp = str(round(time.time() * 1000))
                secret_enc = self.dingtalk_secret.encode('utf-8')
                string_to_sign = '{}\n{}'.format(timestamp, self.dingtalk_secret)
                string_to_sign_enc = string_to_sign.encode('utf-8')
                hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
                sign = urllib.parse.quote_plus(base64.b64encode(hmac_code).decode('utf-8'))
                
                # 构造完整的webhook URL
                webhook_url = f"{self.dingtalk_webhook}&timestamp={timestamp}&sign={sign}"
                
                # 构造请求体
                data = {
                    "msgtype": "text",
                    "text": {
                        "content": message
                    }
                }
                
                headers = {'Content-Type': 'application/json'}
                response = requests.post(webhook_url, json=data, headers=headers)
                if response.status_code == 200:
                    logger.info(f"钉钉告警发送成功")
                else:
                    logger.error(f"钉钉告警发送失败: {response.text}")
            except Exception as e:
                logger.error(f"发送钉钉告警时出错: {e}")

        elif self.dingtalk_webhook:
            # 不需要签名的情况
            try:
                data = {
                    "msgtype": "text",
                    "text": {
                        "content": message
                    }
                }
                response = requests.post(self.dingtalk_webhook, json=data)
                if response.status_code == 200:
                    logger.info("钉钉告警发送成功")
                else:
                    logger.error(f"钉钉告警发送失败: {response.text}")
            except Exception as e:
                logger.error(f"发送钉钉告警时出错: {e}")
    
    def send_alert(self, message):
        """发送钉钉告警"""
        self.send_dingtalk_alert(message)
    
    def convert_to_ak_format(self, ticker):
        """转换股票代码格式为akshare所需格式"""
        # 如果是6位数字，判断是沪深还是北交所
        if ticker.isdigit() and len(ticker) == 6:
            if ticker.startswith('6'):  # 上交所
                return f"{ticker}.SH"
            elif ticker.startswith(('0', '1', '3')):  # 深交所
                return f"{ticker}.SZ"
            elif ticker.startswith('4') or ticker.startswith('8'):  # 北交所
                return f"{ticker}.BJ"
        return ticker
    
    def get_stock_data(self, ticker, interval='daily', max_retries=3):
        """获取A股、港股、ETF或指数的数据，并补全实时快照"""
        retries = 0
        while retries < max_retries:
            try:
                # 1. 判断资产类型
                asset_type = self._get_asset_type(ticker)
                
                # 2. 获取历史数据
                data = self._fetch_data(ticker, asset_type, interval)
                
                if data is not None and not data.empty:
                    # 3. 统一数据格式
                    data = self._format_data(data)
                    
                    # 4. 如果是日线且在交易时间，尝试获取实时快照补全
                    if interval == 'daily' and asset_type in ['a_stock', 'etf', 'index']:
                        data = self._append_realtime_spot(ticker, asset_type, data)
                    
                    return data
                else:
                    break
            except Exception as e:
                logger.error(f"获取{ticker}数据时出错: {e}")
                retries += 1
                time.sleep(1)
        return None

    def _append_realtime_spot(self, ticker, asset_type, hist_data):
        """获取实时快照并追加到历史数据末尾"""
        try:
            # 简单判断当前是否可能在交易时间内 (9:00 - 15:30)
            now = datetime.now()
            if now.hour < 9 or now.hour > 16:
                return hist_data

            if asset_type == 'etf':
                spot_df = ak.fund_etf_spot_em()
                spot = spot_df[spot_df['代码'] == ticker]
                if not spot.empty:
                    last_price = float(spot['最新价'].iloc[0])
                    last_time = pd.to_datetime(now.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    # 如果实时数据的时间比历史数据新，则追加
                    if last_time.date() > hist_data.index[-1].date():
                        new_row = pd.DataFrame({
                            'Open': [float(spot['开盘价'].iloc[0])],
                            'High': [float(spot['最高价'].iloc[0])],
                            'Low': [float(spot['最低价'].iloc[0])],
                            'Close': [last_price],
                            'Volume': [float(spot['成交量'].iloc[0])]
                        }, index=[last_time])
                        hist_data = pd.concat([hist_data, new_row])
                        # logger.info(f"[{ticker}] 已补全当日实时快照: {last_price}")
            
            elif asset_type == 'a_stock':
                spot_df = ak.stock_zh_a_spot_em()
                spot = spot_df[spot_df['代码'] == ticker]
                if not spot.empty:
                    last_price = float(spot['最新价'].iloc[0])
                    last_time = pd.to_datetime(now.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    if last_time.date() > hist_data.index[-1].date():
                        new_row = pd.DataFrame({
                            'Open': [float(spot['开盘价'].iloc[0])],
                            'High': [float(spot['最高价'].iloc[0])],
                            'Low': [float(spot['最低价'].iloc[0])],
                            'Close': [last_price],
                            'Volume': [float(spot['成交量'].iloc[0])]
                        }, index=[last_time])
                        hist_data = pd.concat([hist_data, new_row])
                        # logger.info(f"[{ticker}] 已补全当日实时快照: {last_price}")
            
            elif asset_type == 'index':
                # 指数实时行情
                spot_df = ak.stock_zh_index_spot_em(symbol=ticker)
                if not spot_df.empty:
                    last_price = float(spot_df['最新价'].iloc[0])
                    last_time = pd.to_datetime(now.strftime('%Y-%m-%d %H:%M:%S'))
                    
                    if last_time.date() > hist_data.index[-1].date():
                        new_row = pd.DataFrame({
                            'Open': [float(spot_df['开盘价'].iloc[0])],
                            'High': [float(spot_df['最高价'].iloc[0])],
                            'Low': [float(spot_df['最低价'].iloc[0])],
                            'Close': [last_price],
                            'Volume': [float(spot_df['成交量'].iloc[0])]
                        }, index=[last_time])
                        hist_data = pd.concat([hist_data, new_row])
                        # logger.info(f"[{ticker}] 已补全指数当日实时快照: {last_price}")
        except Exception as e:
            logger.warning(f"获取{ticker}实时快照失败: {e}")
            
        return hist_data

    def _get_asset_type(self, ticker):
        """根据ticker判断资产类型"""
        if ticker.startswith('sh') or ticker.startswith('sz') or ticker in ['000001', '399006']:
            return 'index'
        if len(ticker) == 5 and ticker.isdigit():
            return 'hk_stock'
        if len(ticker) == 6 and ticker.isdigit():
            if ticker.startswith('15') or ticker.startswith('51'):
                return 'etf'
            else:
                return 'a_stock'
        return 'unknown'

    def _fetch_data(self, ticker, asset_type, interval):
        """根据资产类型和周期调用不同的akshare接口"""
        if asset_type == 'index':
            if interval in ['daily', 'weekly']:
                index_code = ticker if ticker.startswith('s') else ('sh' + ticker if ticker == '000001' else 'sz' + ticker)
                data = ak.stock_zh_index_daily(symbol=index_code)
                if interval == 'weekly':
                    return self._resample_to_weekly(data, date_col='date', ohlc_cols={'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
                return data
        
        elif asset_type == 'a_stock':
            if interval == 'weekly':
                return ak.stock_zh_a_hist(symbol=ticker, period="weekly", adjust="qfq")
            elif interval == 'daily':
                return ak.stock_zh_a_hist(symbol=ticker, period="daily", adjust="qfq")
            elif interval in ['60m', '120m']:
                data_60 = ak.stock_zh_a_hist_min_em(symbol=ticker, period="60", adjust="qfq")
                if interval == '120m' and not data_60.empty:
                    return self._resample_to_120min(data_60)
                return data_60

        elif asset_type == 'etf':
            if interval == 'weekly':
                return ak.fund_etf_hist_em(symbol=ticker, period="weekly", adjust="qfq")
            elif interval == 'daily':
                return ak.fund_etf_hist_em(symbol=ticker, period="daily", adjust="qfq")
            elif interval in ['60m', '120m']:
                data_60 = ak.fund_etf_hist_min_em(symbol=ticker, period="60", adjust="qfq")
                if interval == '120m' and not data_60.empty:
                    return self._resample_to_120min(data_60)
                return data_60

        elif asset_type == 'hk_stock':
            if interval in ['daily', 'weekly']:
                data = ak.stock_hk_hist(symbol=ticker, period=interval, adjust="qfq")
                return data

        return None # 默认或不支持的返回None

    def _resample_to_weekly(self, data, date_col, ohlc_cols):
        """将日线数据重采样为周线"""
        data[date_col] = pd.to_datetime(data[date_col])
        data.set_index(date_col, inplace=True)
        resampled_data = data.resample('W-FRI').apply(ohlc_cols).dropna()
        return resampled_data.reset_index()

    def _resample_to_120min(self, data_60):
        """将60分钟数据重采样为120分钟"""
        data_60['时间'] = pd.to_datetime(data_60['时间'])
        data_60.set_index('时间', inplace=True)
        ohlc_dict = {'开盘': 'first', '最高': 'max', '最低': 'min', '收盘': 'last', '成交量': 'sum'}
        resampled_data = data_60.resample('120min').apply(ohlc_dict).dropna()
        return resampled_data.reset_index()

    def _format_data(self, data):
        """统一不同接口返回的数据格式"""
        rename_dict = {
            "日期": "Date", "时间": "Date", "date": "Date",
            "开盘": "Open", "open": "Open",
            "收盘": "Close", "close": "Close",
            "最高": "High", "high": "High",
            "最低": "Low", "low": "Low",
            "成交量": "Volume", "volume": "Volume"
        }
        data.rename(columns={k: v for k, v in rename_dict.items() if k in data.columns}, inplace=True)
        data.set_index(pd.to_datetime(data["Date"]), inplace=True)
        return data

    def monitor(self):
        """监控所有股票"""
        logger.info("====== 开始新一轮监控 ======")
        for stock in self.stocks:
            try:
                logger.info(f"--- 正在检查 {stock.name}({stock.ticker}) ---")
                
                # 1. 统一获取所需数据
                data_weekly = strategy.calculate_indicators(self.get_stock_data(stock.ticker, 'weekly'))
                data_daily = strategy.calculate_indicators(self.get_stock_data(stock.ticker, 'daily'))
                data_120m = strategy.calculate_indicators(self.get_stock_data(stock.ticker, '120m'))
                data_60m = strategy.calculate_indicators(self.get_stock_data(stock.ticker, '60m'))
                
                # 2. 记录RSI值 (增加日期显示)
                if data_daily is not None and not data_daily.empty and 'RSI' in data_daily.columns and \
                   data_weekly is not None and not data_weekly.empty and 'RSI' in data_weekly.columns:
                    daily_rsi = data_daily['RSI'].iloc[-1]
                    weekly_rsi = data_weekly['RSI'].iloc[-1]
                    last_date = data_daily.index[-1].strftime('%Y-%m-%d %H:%M:%S')
                    logger.info(f"[{stock.name}] 数据时间: {last_date}")
                    logger.info(f"[{stock.name}] 当前RSI - 日线: {daily_rsi:.2f}, 周线: {weekly_rsi:.2f}")
                else:
                    logger.warning(f"[{stock.name}] 无法获取日线或周线RSI数据")

                # 3. 将数据传递给判断函数
                all_data = {
                    'weekly': data_weekly,
                    'daily': data_daily,
                    '120m': data_120m,
                    '60m': data_60m
                }

                # 检查卖出
                sell_msgs = strategy.judge_sell(stock.name, stock.judge_sell_ids, all_data)
                if sell_msgs:
                    sell_msg = "\n".join(sell_msgs)
                    logger.warning(sell_msg) # 使用warning级别记录买卖信号
                    self.send_alert(sell_msg)
                
                # 检查买入
                def get_index_data_func(ticker):
                    return strategy.calculate_indicators(self.get_stock_data(ticker, 'daily'))
                
                buy_msgs = strategy.judge_buy(stock.name, stock.judge_buy_ids, all_data, get_index_data_func)
                if buy_msgs:
                    buy_msg = "\n".join(buy_msgs)
                    logger.warning(buy_msg)
                    self.send_alert(buy_msg)

            except Exception as e:
                logger.error(f"处理股票 {stock.name} 时发生严重错误: {e}", exc_info=True)
        
        return False
    
    def monitor_loop(self, check_interval=43200):
        """持续监控循环 (默认每12小时检查一次并重载文件)"""
        while True:
            self.load_stocks_from_file() # 每轮开始前检查并重新加载
            self.monitor()
            logger.info(f"本轮监控完成，{check_interval}秒后开始下一轮...")
            time.sleep(check_interval)

if __name__ == '__main__':
    # 钉钉机器人配置
    dingtalk_webhook = "https://oapi.dingtalk.com/robot/send?access_token=f2f2dbca162d963b1619dbdbde91f2d70572836aac58b94ea4713e42167da160"
    dingtalk_secret = 'SECbf80e6ffe5fdd6ac64c51008d0a56303fdba94f6296d75f4d7134abc5fa49395'
    
    # 创建监控实例 (自动从 stocks.json 加载)
    monitor = ETFMonitor(
        stocks_file='config/stocks.json',
        dingtalk_webhook=dingtalk_webhook, 
        dingtalk_secret=dingtalk_secret
    )
    
    # 启动监控循环 (默认每12小时检查一次)
    monitor.monitor_loop(check_interval=43200)
