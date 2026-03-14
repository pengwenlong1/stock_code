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

    def calculate_indicators(self, data):
        """计算技术指标 (对齐国内行情软件算法)"""
        if data is None or data.empty:
            return None
        
        # 1. 计算 RSI (使用国内通用的 SMA(x, N, 1) 逻辑)
        # 这种方法比 talib.RSI 更能对齐东财、通达信
        close = data['Close']
        delta = close.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        
        # 国内 RSI 公式: RSI = SMA(MAX(Close-LC,0),N,1) / SMA(ABS(Close-LC),N,1) * 100
        # 在 pandas 中，ewm(alpha=1/N, adjust=False) 与 SMA(N, 1) 等效
        # 使用 6 日 RSI 以对齐东方财富等软件默认设置
        ma_up = up.ewm(alpha=1/6, adjust=False).mean()
        ma_down = down.ewm(alpha=1/6, adjust=False).mean()
        data['RSI'] = ma_up / (ma_up + ma_down) * 100
        
        # 2. 计算 MACD (使用用户要求的标准参数: 12, 26, 9)
        # MACD (DIF), MACD_signal (DEA), MACD_hist (MACD柱)
        data['MACD'], data['MACD_signal'], data['MACD_hist'] = talib.MACD(
            data['Close'], fastperiod=12, slowperiod=26, signalperiod=9
        )
        # 对齐国内行情软件，MACD柱通常需要 * 2
        data['MACD_hist'] = data['MACD_hist'] * 2
        
        # 3. 计算 SAR (使用参数: 10, 2, 20)
        # 对应 talib: acceleration=0.02 (2%), maximum=0.2 (20%)
        # 10 通常代表计算周期，talib 会根据数据序列自动从头开始计算
        data['SAR'] = talib.SAR(data['High'], data['Low'], acceleration=0.02, maximum=0.2)
        
        return data

    def detect_divergence(self, data, type='top'):
        """
        检测背离（顶背离或底背离）
        逻辑：仅根据 MACD DIF 进行判断。价格创新高/新低，但 DIF 未创新高/新低。
        """
        if data is None or len(data) < 30:
            return False
        
        close = data['Close'].values
        dif = data['MACD'].values # DIF
        macd_hist = data['MACD_hist'].values # MACD 柱
        
        # 寻找局部极值点 (增加窗口过滤，要求左右至少 N 个点更低/更高)
        def find_extrema(values, is_max=True, window=3):
            extrema = []
            for i in range(window, len(values) - window):
                if is_max:
                    if all(values[i] > values[i-j] for j in range(1, window+1)) and \
                       all(values[i] >= values[i+j] for j in range(1, window+1)):
                        extrema.append(i)
                else:
                    if all(values[i] < values[i-j] for j in range(1, window+1)) and \
                       all(values[i] <= values[i+j] for j in range(1, window+1)):
                        extrema.append(i)
            return extrema

        if type == 'top':
            # 顶背离：价格创新高，但 MACD DIF 未创新高
            peaks = find_extrema(close, is_max=True, window=3)
            if len(peaks) >= 2:
                p1, p2 = peaks[-2], peaks[-1] # p1 较早，p2 较晚
                
                # 过滤高点过于接近的情况 (至少间隔 5 个周期)
                if p2 - p1 < 5:
                    return False
                
                # 价格创新高，但 DIF 未创新高
                if close[p2] > close[p1] and dif[p2] < dif[p1]:
                    # 额外检查：如果当前 DIF 正在强力上穿 0 轴或 MACD 柱在快速增长，则忽略背离
                    if dif[p2] > 0 and macd_hist[p2] > macd_hist[p2-1] > 0:
                        return False
                    return True
        else:
            # 底背离：价格创新低，但 MACD DIF 未创新低
            troughs = find_extrema(close, is_max=False, window=3)
            if len(troughs) >= 2:
                t1, t2 = troughs[-2], troughs[-1]
                
                if t2 - t1 < 5:
                    return False
                    
                # 价格创新低，但 DIF 未创新低
                if close[t2] < close[t1] and dif[t2] > dif[t1]:
                    # 如果 DIF 正在强力下穿 0 轴或 MACD 柱在快速下跌，则忽略底背离
                    if dif[t2] < 0 and macd_hist[t2] < macd_hist[t2-1] < 0:
                        return False
                    return True
        
        return False

    def is_death_cross(self, data, window=3):
        """MACD死叉 (检查最近 window 个周期内是否出现过)"""
        if data is None or len(data) < window + 1:
            return False
        for i in range(1, window + 1):
            if data['MACD'].iloc[-i-1] > data['MACD_signal'].iloc[-i-1] and \
               data['MACD'].iloc[-i] < data['MACD_signal'].iloc[-i]:
                return True
        return False

    def is_golden_cross(self, data, window=3):
        """MACD金叉 (检查最近 window 个周期内是否出现过)"""
        if data is None or len(data) < window + 1:
            return False
        for i in range(1, window + 1):
            if data['MACD'].iloc[-i-1] < data['MACD_signal'].iloc[-i-1] and \
               data['MACD'].iloc[-i] > data['MACD_signal'].iloc[-i]:
                return True
        return False
    def judge_sell(self, stock, all_data):
        """根据 judge_sell_ids 判断卖出条件 (3天窗口逻辑)"""
        messages = []
        for sell_id in stock.judge_sell_ids:
            if sell_id == 1:
                data_weekly = all_data.get('weekly')
                data_daily = all_data.get('daily')
                data_120m = all_data.get('120m')
                
                if data_weekly is None or data_daily is None or data_120m is None:
                    continue

                # 最近3天的周线最大RSI
                max_weekly_rsi = data_weekly['RSI'].tail(3).max()
                
                # 1. 周线顶背离 + 120分钟死叉(3天内) -> 清仓
                if self.detect_divergence(data_weekly, 'top') and self.is_death_cross(data_120m, window=3):
                    messages.append(f"【{stock.name}】卖出信号(条件1-清仓): 触发[周线顶背离 + 120分钟死叉]，建议清仓")

                # 2. 日线顶背离 + 120分钟死叉(3天内) -> 出 1/2
                elif self.detect_divergence(data_daily, 'top') and self.is_death_cross(data_120m, window=3):
                    messages.append(f"【{stock.name}】卖出信号(条件1-减半): 触发[日线顶背离 + 120分钟死叉]，建议出1/2")

                # 3. 日线SAR跌破(3天内) -> 阶梯式卖出
                else:
                    is_sar_breakdown_recent = False
                    for i in range(1, 4):
                        if len(data_daily) >= i+1:
                            if data_daily['Close'].iloc[-i-1] > data_daily['SAR'].iloc[-i-1] and \
                               data_daily['Close'].iloc[-i] < data_daily['SAR'].iloc[-i]:
                                is_sar_breakdown_recent = True
                                break

                    if is_sar_breakdown_recent:
                        trigger_reason = f"触发[日线SAR跌破]，当前周线RSI({max_weekly_rsi:.2f})"
                        if max_weekly_rsi > 90:
                            messages.append(f"【{stock.name}】卖出信号(条件1-阶梯): {trigger_reason} > 90，建议卖出所有")
                        elif max_weekly_rsi > 85:
                            messages.append(f"【{stock.name}】卖出信号(条件1-阶梯): {trigger_reason} > 85，建议卖出剩余1/2")
                        elif max_weekly_rsi > 80:
                            messages.append(f"【{stock.name}】卖出信号(条件1-阶梯): {trigger_reason} > 80，建议卖出1/3")

        return "\n".join(messages) if messages else None

    def judge_buy(self, stock, all_data):
        """根据 judge_buy_ids 判断买入条件 (3天窗口逻辑)"""
        messages = []
        for buy_id in stock.judge_buy_ids:
            data_weekly = all_data.get('weekly')
            data_daily = all_data.get('daily')
            data_120m = all_data.get('120m')

            if data_weekly is None or data_daily is None or data_120m is None:
                continue

            # 最近3天的RSI极值
            min_daily_rsi = data_daily['RSI'].tail(3).min()
            min_weekly_rsi = data_weekly['RSI'].tail(3).min()
            
            # 买入条件1 (保守型): 日线RSI<20且周线RSI<25
            if buy_id == 1:
                if min_daily_rsi < 20 and min_weekly_rsi < 25:
                    messages.append(f"【{stock.name}】买入信号(条件1-保守): 触发[日线RSI({min_daily_rsi:.2f})<20 且 周线RSI({min_weekly_rsi:.2f})<25]")
            
            # 买入条件2 (标准型): 日线RSI<25且周线RSI<30 或 日线底背离+120分钟金叉
            elif buy_id == 2:
                if min_daily_rsi < 25 and min_weekly_rsi < 30:
                    messages.append(f"【{stock.name}】买入信号(条件2-标准): 触发[日线RSI({min_daily_rsi:.2f})<25 且 周线RSI({min_weekly_rsi:.2f})<30]")
                elif self.detect_divergence(data_daily, 'bottom') and self.is_golden_cross(data_120m, window=3):
                    messages.append(f"【{stock.name}】买入信号(条件2-标准): 触发[日线底背离 + 120分钟金叉]")
            
            # 买入条件3 (增强型): 条件2 或 核心指数RSI<25
            elif buy_id == 3:
                # 检查自身超卖
                if min_daily_rsi < 25 and min_weekly_rsi < 30:
                    messages.append(f"【{stock.name}】买入信号(条件3-增强): 触发[日线RSI({min_daily_rsi:.2f})<25 且 周线RSI({min_weekly_rsi:.2f})<30]")
                # 检查自身底背离
                elif self.detect_divergence(data_daily, 'bottom') and self.is_golden_cross(data_120m, window=3):
                    messages.append(f"【{stock.name}】买入信号(条件3-增强): 触发[日线底背离 + 120分钟金叉]")
                else:
                    # 检查指数低位
                    for idx_ticker, idx_name in [('000001', '上证指数'), ('399006', '创业板指')]:
                        idx_data = self.calculate_indicators(self.get_stock_data(idx_ticker, 'daily'))
                        if idx_data is not None and not idx_data.empty and 'RSI' in idx_data.columns:
                            idx_min_rsi = idx_data['RSI'].tail(3).min()
                            if idx_min_rsi < 25:
                                messages.append(f"【{stock.name}】买入信号(条件3-增强): 触发[{idx_name}日线RSI({idx_min_rsi:.2f})<25]")
                                break
            
            # 买入条件4 (极度超卖): 日线RSI<20且周线RSI<20
            elif buy_id == 4:
                if min_daily_rsi < 20 and min_weekly_rsi < 20:
                    messages.append(f"【{stock.name}】买入信号(条件4-极度超卖): 触发[日线RSI({min_daily_rsi:.2f})<20 且 周线RSI({min_weekly_rsi:.2f})<20]")

        return "\n".join(messages) if messages else None

    def monitor(self):
        """监控所有股票"""
        logger.info("====== 开始新一轮监控 ======")
        for stock in self.stocks:
            try:
                logger.info(f"--- 正在检查 {stock.name}({stock.ticker}) ---")
                
                # 1. 统一获取所需数据
                data_weekly = self.calculate_indicators(self.get_stock_data(stock.ticker, 'weekly'))
                data_daily = self.calculate_indicators(self.get_stock_data(stock.ticker, 'daily'))
                data_120m = self.calculate_indicators(self.get_stock_data(stock.ticker, '120m'))
                data_60m = self.calculate_indicators(self.get_stock_data(stock.ticker, '60m'))
                
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
                sell_msg = self.judge_sell(stock, all_data)
                if sell_msg:
                    logger.warning(sell_msg) # 使用warning级别记录买卖信号
                    self.send_alert(sell_msg)
                
                # 检查买入
                buy_msg = self.judge_buy(stock, all_data)
                if buy_msg:
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
