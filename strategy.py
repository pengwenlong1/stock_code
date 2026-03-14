import pandas as pd
import talib

def calculate_indicators(data):
    """计算技术指标 (对齐国内行情软件算法)"""
    if data is None or data.empty:
        return None
    
    # 1. 计算 RSI (使用国内通用的 SMA(x, N, 1) 逻辑)
    close = data['Close']
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    
    # 国内 RSI 公式: RSI = SMA(MAX(Close-LC,0),N,1) / SMA(ABS(Close-LC),N,1) * 100
    ma_up = up.ewm(alpha=1/6, adjust=False).mean()
    ma_down = down.ewm(alpha=1/6, adjust=False).mean()
    data['RSI'] = ma_up / (ma_up + ma_down) * 100
    
    # 2. 计算 MACD (12, 26, 9)
    data['MACD'], data['MACD_signal'], data['MACD_hist'] = talib.MACD(
        data['Close'], fastperiod=12, slowperiod=26, signalperiod=9
    )
    data['MACD_hist'] = data['MACD_hist'] * 2
    
    # 3. 计算 SAR (10, 2, 20)
    data['SAR'] = talib.SAR(data['High'], data['Low'], acceleration=0.02, maximum=0.2)
    
    return data

def detect_divergence(data, type='top'):
    """检测背离 (仅根据 MACD DIF)"""
    if data is None or len(data) < 30:
        return False
    
    close = data['Close'].values
    dif = data['MACD'].values # DIF
    macd_hist = data['MACD_hist'].values # MACD 柱
    
    def find_last_significant_extrema(values, is_max=True, window=5):
        for i in range(len(values) - window - 1, window, -1):
            if is_max:
                if all(values[i] > values[i-j] for j in range(1, window+1)) and \
                   all(values[i] > values[i+j] for j in range(1, window+1)):
                    return i
            else:
                if all(values[i] < values[i-j] for j in range(1, window+1)) and \
                   all(values[i] < values[i+j] for j in range(1, window+1)):
                    return i
        return None

    curr = len(close) - 1
    
    if type == 'top':
        p1 = find_last_significant_extrema(close, is_max=True, window=5)
        if p1 is not None and curr - p1 >= 5:
            if close[curr] > close[p1] and dif[curr] < dif[p1]:
                if macd_hist[curr] > macd_hist[curr-1]: return False
                if dif[curr] > dif[curr-1]: return False
                if dif[curr] > 0 and dif[curr-1] <= 0: return False
                return True
    else:
        p1 = find_last_significant_extrema(close, is_max=False, window=5)
        if p1 is not None and curr - p1 >= 5:
            if close[curr] < close[p1] and dif[curr] > dif[p1]:
                if macd_hist[curr] < macd_hist[curr-1]: return False
                if dif[curr] < dif[curr-1]: return False
                return True
    return False

def is_death_cross(data, window=3):
    if data is None or len(data) < window + 1:
        return False
    for i in range(1, window + 1):
        if data['MACD'].iloc[-i-1] > data['MACD_signal'].iloc[-i-1] and \
           data['MACD'].iloc[-i] < data['MACD_signal'].iloc[-i]:
            return True
    return False

def is_golden_cross(data, window=3):
    if data is None or len(data) < window + 1:
        return False
    for i in range(1, window + 1):
        if data['MACD'].iloc[-i-1] < data['MACD_signal'].iloc[-i-1] and \
           data['MACD'].iloc[-i] > data['MACD_signal'].iloc[-i]:
            return True
    return False

def judge_sell(stock_name, judge_sell_ids, all_data):
    messages = []
    for sell_id in judge_sell_ids:
        if sell_id == 1:
            data_weekly = all_data.get('weekly')
            data_daily = all_data.get('daily')
            data_120m = all_data.get('120m')
            
            if data_weekly is None or data_daily is None or data_120m is None:
                continue

            max_weekly_rsi = data_weekly['RSI'].tail(3).max()
            
            if detect_divergence(data_weekly, 'top') and is_death_cross(data_120m, window=3):
                messages.append(f"【{stock_name}】卖出信号(条件1-清仓): 触发[周线顶背离 + 120分钟死叉]，建议清仓")
            elif detect_divergence(data_daily, 'top') and is_death_cross(data_120m, window=3):
                messages.append(f"【{stock_name}】卖出信号(条件1-减半): 触发[日线顶背离 + 120分钟死叉]，建议出1/2")
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
                        messages.append(f"【{stock_name}】卖出信号(条件1-阶梯): {trigger_reason} > 90，建议卖出所有")
                    elif max_weekly_rsi > 85:
                        messages.append(f"【{stock_name}】卖出信号(条件1-阶梯): {trigger_reason} > 85，建议卖出剩余1/2")
                    elif max_weekly_rsi > 80:
                        messages.append(f"【{stock_name}】卖出信号(条件1-阶梯): {trigger_reason} > 80，建议卖出1/3")
    return messages

def judge_buy(stock_name, judge_buy_ids, all_data, get_index_data_func=None):
    messages = []
    for buy_id in judge_buy_ids:
        data_weekly = all_data.get('weekly')
        data_daily = all_data.get('daily')
        data_120m = all_data.get('120m')

        if data_weekly is None or data_daily is None or data_120m is None:
            continue

        min_daily_rsi = data_daily['RSI'].tail(3).min()
        min_weekly_rsi = data_weekly['RSI'].tail(3).min()
        
        if buy_id == 1:
            if min_daily_rsi < 20 and min_weekly_rsi < 25:
                messages.append(f"【{stock_name}】买入信号(条件1-保守): 触发[日线RSI({min_daily_rsi:.2f})<20 且 周线RSI({min_weekly_rsi:.2f})<25]")
        elif buy_id == 2:
            if min_daily_rsi < 25 and min_weekly_rsi < 30:
                messages.append(f"【{stock_name}】买入信号(条件2-标准): 触发[日线RSI({min_daily_rsi:.2f})<25 且 周线RSI({min_weekly_rsi:.2f})<30]")
            elif detect_divergence(data_daily, 'bottom') and is_golden_cross(data_120m, window=3):
                messages.append(f"【{stock_name}】买入信号(条件2-标准): 触发[日线底背离 + 120分钟金叉]")
        elif buy_id == 3:
            if min_daily_rsi < 25 and min_weekly_rsi < 30:
                messages.append(f"【{stock_name}】买入信号(条件3-增强): 触发[日线RSI({min_daily_rsi:.2f})<25 且 周线RSI({min_weekly_rsi:.2f})<30]")
            elif detect_divergence(data_daily, 'bottom') and is_golden_cross(data_120m, window=3):
                messages.append(f"【{stock_name}】买入信号(条件3-增强): 触发[日线底背离 + 120分钟金叉]")
            elif get_index_data_func:
                for idx_ticker, idx_name in [('000001', '上证指数'), ('399006', '创业板指')]:
                    idx_data = get_index_data_func(idx_ticker)
                    if idx_data is not None and not idx_data.empty and 'RSI' in idx_data.columns:
                        idx_min_rsi = idx_data['RSI'].tail(3).min()
                        if idx_min_rsi < 25:
                            messages.append(f"【{stock_name}】买入信号(条件3-增强): 触发[{idx_name}日线RSI({idx_min_rsi:.2f})<25]")
                            break
        elif buy_id == 4:
            if min_daily_rsi < 20 and min_weekly_rsi < 20:
                messages.append(f"【{stock_name}】买入信号(条件4-极度超卖): 触发[日线RSI({min_daily_rsi:.2f})<20 且 周线RSI({min_weekly_rsi:.2f})<20]")
    return messages
