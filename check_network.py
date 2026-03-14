import akshare as ak
import requests
import socket
import time

def check_network():
    print("--- 网络连通性诊断 ---")
    
    # 1. 检查基础网络
    targets = ["www.baidu.com", "quote.eastmoney.com"]
    for target in targets:
        try:
            start = time.time()
            requests.get(f"https://{target}", timeout=5)
            print(f"√ 基础网络: 成功连接 {target} (耗时: {time.time()-start:.2f}s)")
        except Exception as e:
            print(f"× 基础网络: 无法连接 {target} ({e})")

    # 2. 检查 DNS
    try:
        ip = socket.gethostbyname("quote.eastmoney.com")
        print(f"√ DNS解析: quote.eastmoney.com -> {ip}")
    except Exception as e:
        print(f"× DNS解析失败: {e}")

    # 3. 检查 akshare 接口 (不同数据源)
    print("\n--- akshare 接口测试 ---")
    
    sources = [
        ("EastMoney (默认)", lambda: ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20240101", end_date="20240105", adjust="qfq")),
        ("Sina (备用)", lambda: ak.stock_zh_a_daily(symbol="sh000001", start_date="20240101", end_date="20240105")),
        ("指数数据", lambda: ak.stock_zh_index_daily(symbol="sh000001"))
    ]

    for name, func in sources:
        try:
            print(f"正在测试 {name}...")
            df = func()
            if df is not None and not df.empty:
                print(f"  √ {name} 接口正常 (返回 {len(df)} 行数据)")
            else:
                print(f"  ? {name} 接口返回为空")
        except Exception as e:
            print(f"  × {name} 接口失败: {e}")
            if "RemoteDisconnected" in str(e) or "Connection aborted" in str(e):
                print("    [提示] 这种错误通常意味着你的 IP 正在被数据源服务器频率限制或临时拉黑。")

    print("\n--- 解决方案建议 ---")
    print("1. 如果所有接口都失败: 你的 IP 极有可能已被数据源封锁。请尝试更换网络环境（如手机热点）或等待几小时。")
    print("2. 如果只有 EastMoney 失败: 服务器目前拒绝了你的请求，请增加请求间隔时间。")
    print("3. 手动模式: 你可以从东方财富官网下载 CSV 数据，改名放入 data_cache/ 目录，回测程序会自动读取。")

if __name__ == "__main__":
    check_network()
