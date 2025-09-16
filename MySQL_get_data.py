import requests
import mysql.connector
from datetime import datetime, timezone, timedelta
import time

# 连接 MySQL 数据库
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='密码',
    database='数据库'
)
cursor = conn.cursor()


def get_latest_ts():
    cursor.execute("SELECT timestamp FROM `okx_doge_ustd_1m` ORDER BY timestamp DESC LIMIT 1")
    result = cursor.fetchone()
    return result[0] if result else None


def fetch_binance_klines(start_time, end_time):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": "DOGEUSDT",  # 修正交易对符号
        "interval": "1m",
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        error_msg = response.json().get('msg', '未知错误')
        print(f"Binance API错误: {e} | 详细信息: {error_msg}")
        return None
    except Exception as e:
        print(f"请求失败: {e}")
        return None


def main():
    end_timestamp = int(datetime.now().timestamp() * 1000)
    start_timestamp = int(datetime(2020, 1, 1).timestamp() * 1000)

    latest_ts = get_latest_ts()
    if latest_ts:
        start_timestamp = latest_ts + 1

    while start_timestamp < end_timestamp:
        print(f"拉取范围: {start_timestamp} -> {end_timestamp}")
        klines = fetch_binance_klines(start_timestamp, end_timestamp)
        if not klines:
            print("等待重试...")
            time.sleep(10)
            continue

        success_count = 0
        last_ts_in_batch = start_timestamp

        for kline in klines:
            try:
                timestamp = kline[0]
                readable_time = datetime.fromtimestamp(timestamp / 1000, timezone.utc)
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])
                quote_volume = float(kline[7])

                cursor.execute("""
                    INSERT INTO `okx_doge_ustd_1m` 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        open_price = VALUES(open_price),
                        high_price = VALUES(high_price),
                        low_price = VALUES(low_price),
                        close_price = VALUES(close_price),
                        volume = VALUES(volume),
                        quote_volume = VALUES(quote_volume)
                """, (readable_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume))

                success_count += 1
                last_ts_in_batch = timestamp
            except mysql.connector.Error as e:
                print(f"数据库错误: {e}")

        conn.commit()
        print(f"插入 {success_count} 条记录，最后时间戳: {last_ts_in_batch}")

        if success_count > 0:
            start_timestamp = last_ts_in_batch + 1
        else:
            break

        time.sleep(1)


if __name__ == "__main__":
    main()
    cursor.close()
    conn.close()