import pandas as pd
import akshare as ak
import strategy
from datetime import datetime, timedelta
import os
import matplotlib.pyplot as plt
import random
import time

class BacktestEngine:
    def __init__(self, ticker, name, buy_ids, sell_ids, start_date, end_date, initial_cash=100000):
        self.ticker = ticker
        self.name = name
        self.buy_ids = buy_ids
        self.sell_ids = sell_ids
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.cash = initial_cash
        self.position = 0
        self.initial_cash = initial_cash
        self.history = []
        
        # 加载数据
        print(f"正在加载 {name}({ticker}) 的回测数据...")
        self.data_daily = self._get_hist_data(ticker, 'daily')
        self.data_weekly = self._get_hist_data(ticker, 'weekly')
        self.data_120m = self._get_hist_data(ticker, '120m')
        
        # 预计算所有指标
        if self.data_daily is not None:
            self.data_daily = strategy.calculate_indicators(self.data_daily)
        if self.data_weekly is not None:
            self.data_weekly = strategy.calculate_indicators(self.data_weekly)
        if self.data_120m is not None:
            self.data_120m = strategy.calculate_indicators(self.data_120m)
        
        # 指数数据缓存
        self.index_data = {}

    def _get_hist_data(self, ticker, period):
        # 极度保守的数据抓取策略，最大限度保护 IP
        cache_dir = "data_cache"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        
        cache_file = os.path.join(cache_dir, f"{ticker}_{period}.csv")
        
        # 1. 优先使用本地任何已有的缓存 (不再强制要求是今天的)
        if os.path.exists(cache_file):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
            # print(f"    [缓存] 发现本地数据 (日期: {file_mtime})，正在读取...")
            df = pd.read_csv(cache_file)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            
            # 如果缓存是今天的，直接返回
            if file_mtime == datetime.now().date():
                return df
            
            # 如果不是今天的，我们也先拿着它，等会儿网络失败了就用它
            print(f"    [提示] 本地缓存已过期，但在这种严格模式下，我们将优先使用它以减少网络请求。")
            return df

        # 2. 只有在本地完全没数据的情况下，才去网络拉取
        import time
        import random
        
        # 引入全局冷却时间，确保即使是不同周期的请求也不会挨得太近
        print(f"    [安全模式] 正在冷却中，准备发起网络请求...")
        time.sleep(random.uniform(10, 20)) # 每次网络请求前强制等待 10-20 秒
        
        max_retries = 3 
        for i in range(max_retries):
            try:
                # 递增的重试延时
                if i > 0:
                    wait_time = random.uniform(30, 60) * i 
                    print(f"    [重试] 等待 {wait_time:.1f} 秒后进行第 {i+1} 次尝试...")
                    time.sleep(wait_time)
                
                df = None
                if period == 'daily':
                    df = ak.stock_zh_a_hist(symbol=ticker, period="daily", adjust="qfq")
                    if df is None or df.empty:
                        df = ak.stock_zh_a_daily(symbol="sz"+ticker if ticker.startswith('3') or ticker.startswith('0') else "sh"+ticker, adjust="qfq")
                elif period == 'weekly':
                    df = ak.stock_zh_a_hist(symbol=ticker, period="weekly", adjust="qfq")
                elif period == '120m':
                    df = ak.stock_zh_a_hist_min_em(symbol=ticker, period="60", adjust="qfq")
                    if df is not None and not df.empty:
                        df['时间'] = pd.to_datetime(df['时间'])
                        df.set_index('时间', inplace=True)
                        ohlc_dict = {'开盘': 'first', '最高': 'max', '最低': 'min', '收盘': 'last', '成交量': 'sum'}
                        df = df.resample('120min').apply(ohlc_dict).dropna()
                        df.reset_index(inplace=True)
                
                if df is not None and not df.empty:
                    rename_dict = {"日期": "Date", "时间": "Date", "date": "Date", "开盘": "Open", "收盘": "Close", "最高": "High", "最低": "Low", "成交量": "Volume"}
                    df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
                    df.to_csv(cache_file, index=False)
                    print(f"    [成功] 数据已下载并同步至本地缓存。")
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)
                    return df
            except Exception as e:
                print(f"    [失败] 网络下载 {period} 异常 (尝试 {i+1}/{max_retries}): {e}")
                if "RemoteDisconnected" in str(e):
                    print("    [警告] 服务器目前仍然封锁您的 IP。建议停止运行，等待 1 小时后再试。")
        
        return None

    def _get_index_data(self, ticker, current_date):
        if ticker not in self.index_data:
            cache_dir = "data_cache"
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
            cache_file = os.path.join(cache_dir, f"index_{ticker}.csv")
            
            df = None
            # 优先从本地加载
            if os.path.exists(cache_file):
                file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
                if file_mtime == datetime.now().date():
                    df = pd.read_csv(cache_file)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)
            
            # 本地没有则从网络下载
            if df is None:
                try:
                    df = ak.stock_zh_index_daily(symbol="sh"+ticker if ticker=="000001" else "sz"+ticker)
                    if df is not None:
                        rename_dict = {"date": "Date", "open": "Open", "close": "Close", "high": "High", "low": "Low", "volume": "Volume"}
                        df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
                        df.to_csv(cache_file, index=False)
                        df['Date'] = pd.to_datetime(df['Date'])
                        df.set_index('Date', inplace=True)
                except Exception as e:
                    print(f"    获取指数 {ticker} 数据失败: {e}")
                    if os.path.exists(cache_file):
                        df = pd.read_csv(cache_file)
                        df['Date'] = pd.to_datetime(df['Date'])
                        df.set_index('Date', inplace=True)
            
            if df is not None:
                self.index_data[ticker] = strategy.calculate_indicators(df)
            else:
                return None
        
        return self.index_data[ticker][:current_date]

    def run(self):
        if self.data_daily is None:
            print("错误: 日线数据缺失，无法运行回测。请检查网络或手动准备 data_cache 文件夹。")
            return

        # 遍历回测区间内的每一个交易日
        try:
            test_dates = self.data_daily.loc[self.start_date:self.end_date].index
        except Exception as e:
            print(f"错误: 无法确定回测日期范围: {e}")
            return
            
        print(f"开始回测: {self.start_date.date()} 至 {self.end_date.date()}，共 {len(test_dates)} 个交易日")
        
        for i, curr_date in enumerate(test_dates):
            # 准备当前时刻能看到的所有周期数据 (截止到 curr_date)
            all_data = {
                'daily': self.data_daily[:curr_date],
                'weekly': self.data_weekly[:curr_date] if self.data_weekly is not None else None,
                '120m': self.data_120m[:curr_date] if self.data_120m is not None else None
            }
            
            # 模拟信号判断
            def get_index_func(t): return self._get_index_data(t, curr_date)
            
            buy_msgs = strategy.judge_buy(self.name, self.buy_ids, all_data, get_index_func)
            sell_msgs = strategy.judge_sell(self.name, self.sell_ids, all_data)
            
            price = self.data_daily.loc[curr_date, 'Close']
            
            # 交易执行逻辑
            if buy_msgs and self.position == 0:
                shares = self.cash // price
                self.position = shares
                self.cash -= shares * price
                print(f"\n>>> [{curr_date.date()}] 买入执行")
                for msg in buy_msgs:
                    print(f"    理由: {msg}")
                print(f"    成交价格: {price:.2f}, 持仓股数: {self.position}")
            
            elif sell_msgs and self.position > 0:
                print(f"\n<<< [{curr_date.date()}] 卖出执行")
                for msg in sell_msgs:
                    print(f"    理由: {msg}")
                print(f"    成交价格: {price:.2f}, 释放资金: {self.position * price:.2f}")
                self.cash += self.position * price
                self.position = 0
            
            # 记录每日净值
            total_value = self.cash + self.position * price
            self.history.append({
                'Date': curr_date,
                'Value': total_value,
                'Price': price,
                'Position': self.position
            })

        self.report()

    def report(self):
        if not self.history:
            print("没有交易记录")
            return
            
        df = pd.DataFrame(self.history)
        df.set_index('Date', inplace=True)
        
        # 1. 策略收益计算
        final_value = df['Value'].iloc[-1]
        total_return = (final_value - self.initial_cash) / self.initial_cash * 100
        
        # 2. 基准收益计算 (买入并持有)
        start_price = df['Price'].iloc[0]
        final_price = df['Price'].iloc[-1]
        benchmark_return = (final_price - start_price) / start_price * 100
        
        # 3. 超额收益 (Alpha)
        alpha = total_return - benchmark_return
        
        # 4. 计算最大回撤
        df['Peak'] = df['Value'].cummax()
        df['Drawdown'] = (df['Value'] - df['Peak']) / df['Peak'] * 100
        max_drawdown = df['Drawdown'].min()
        
        # 5. 基准归一化 (用于图表对比)
        df['Benchmark_Normalized'] = df['Price'] / start_price * self.initial_cash
        
        print("\n" + "="*40)
        print(f"回测报告: {self.name}({self.ticker})")
        print(f"回测区间: {df.index[0].date()} 至 {df.index[-1].date()}")
        print("-" * 40)
        print(f"初始资金: {self.initial_cash:,.2f}")
        print(f"最终价值: {final_value:,.2f}")
        print("-" * 40)
        print(f"策略累计收益: {total_return:+.2f}%")
        print(f"基准(持有)收益: {benchmark_return:+.2f}%")
        print(f"超额收益 (Alpha): {alpha:+.2f}%")
        print(f"最大回撤: {max_drawdown:.2f}%")
        print("="*40)
        
        # 可视化
        try:
            plt.figure(figsize=(12, 8))
            
            # 上图: 策略净值 vs 基准净值
            plt.subplot(2, 1, 1)
            plt.plot(df.index, df['Value'], label=f'Strategy (Return: {total_return:+.1f}%)', color='blue', linewidth=2)
            plt.plot(df.index, df['Benchmark_Normalized'], label=f'Benchmark (Return: {benchmark_return:+.1f}%)', color='gray', linestyle='--', alpha=0.7)
            plt.title(f"Performance Comparison: {self.name}")
            plt.ylabel("Net Value")
            plt.grid(True, alpha=0.3)
            plt.legend()
            
            # 下图: 股价与仓位
            plt.subplot(2, 1, 2)
            plt.plot(df.index, df['Price'], label='Stock Price', color='orange', alpha=0.8)
            plt.ylabel("Price")
            plt.twinx()
            plt.fill_between(df.index, 0, df['Position'], label='Position', color='green', alpha=0.2)
            plt.ylabel("Position (Shares)")
            plt.title("Price & Position")
            plt.grid(True, alpha=0.3)
            plt.legend(loc='upper right')
            
            plt.tight_layout()
            plt.savefig('backtest_result.png')
            print("\n回测图表已保存至 backtest_result.png")
        except Exception as e:
            print(f"可视化失败: {e}")

if __name__ == "__main__":
    # 示例：回测 卓胜微 过去一年的表现
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=720)).strftime('%Y-%m-%d')
    
    # 尝试加载
    ticker = "603087"
    name = "甘李药业"
    
    engine = BacktestEngine(
        ticker=ticker, 
        name=name, 
        buy_ids=[3], 
        sell_ids=[1], 
        start_date=start_date, 
        end_date=end_date
    )
    engine.run()

