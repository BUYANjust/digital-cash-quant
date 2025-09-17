

import pandas as pd
import numpy as np
from decimal import Decimal
import matplotlib.pyplot as plt
import time
import os
import okx.Trade as Trade
import schedule
import requests
from okx.Account import AccountAPI
from requests.adapters import HTTPAdapter
from twisted.words.xish.domish import elementStream
from urllib3.util.retry import Retry
from ssl import SSLError
import mysql.connector
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
import sys
from datetime import datetime, timezone
import pymysql



latest_eq_flow_usdt = None
def get_latest_eq_flow_usdt():
    global latest_eq_flow_usdt  # 声明要修改全局变量

    # 连接数据库
    connection = pymysql.connect(
        host='localhost',  # 数据库地址
        user='root',  # 数据库用户名
        password='密码',  # 数据库密码
        database='数据库',  # 使用的数据库名
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT eq_flow_usdt
            FROM doge_usdt
            ORDER BY timestamp DESC
            LIMIT 1
            """
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                latest_eq_flow_usdt =  float(result['eq_flow_usdt'])
                print("最新 eq_flow_usdt：", latest_eq_flow_usdt)
            else:
                print("没有查到数据")

    finally:
        connection.close()
    print(f'latest_eq_flow_usdt:{latest_eq_flow_usdt}')

# 定义全局变量
latest_usdt_margin = None
latest_doge_margin = None

def get_latest_margins():
    global latest_usdt_margin, latest_doge_margin

    # 建立数据库连接
    connection = pymysql.connect(
        host='localhost',  # 数据库地址
        user='root',  # 数据库用户名
        password='密码',  # 数据库密码
        database='数据库',  # 使用的数据库名
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            sql = """
            SELECT usdt_margin, doge_margin
            FROM doge_usdt
            ORDER BY timestamp DESC  -- 注意根据正确的时间字段排最新
            LIMIT 1
            """
            cursor.execute(sql)
            result = cursor.fetchone()

            if result:
                # 转成Decimal以避免类型错误
                latest_usdt_margin = float(str(result['usdt_margin']))
                latest_doge_margin = float(str(result['doge_margin']))
            else:
                print("未找到数据。")
    except Exception as e:
        print("查询失败：", e)
    finally:
        connection.close()

get_latest_eq_flow_usdt()
# 查询账户资金
def get_account_funds():
    global doge_cashBal
    global usdt_cashBal

    def get_cashBal_from_latest_row():
        conn = mysql.connector.connect(
            host='localhost',  # 数据库地址
            user='root',  # 数据库用户名
            password='密码',  # 数据库密码
            database='数据库'  # 使用的数据库名
        )
        cursor = conn.cursor(dictionary=True)

        # 查询最新一行的数据
        query = """
            SELECT doge_cashBal, usdt_cashBal FROM doge_usdt 
            WHERE timestamp = (SELECT MAX(timestamp) FROM doge_usdt)
        """
        cursor.execute(query)

        # 获取查询结果
        result = cursor.fetchone()  # 返回的是一个字典，或 None 如果没有结果
        doge_cashBal_value, usdt_cashBal_value = 0, 0  # 初始化默认值

        if result:
            doge_cashBal_value = result.get("doge_cashBal", 0)  # 如果查询结果中有 doge_pos，则获取其值，否则默认 0
            usdt_cashBal_value = result.get("usdt_cashBal", 0)  # 如果查询结果中有 usdt_pos，则获取其值，否则默认 0

        # 关闭连接
        cursor.close()
        conn.close()

        return doge_cashBal_value, usdt_cashBal_value

    # 查看账户余额
    doge_cashBal, usdt_cashBal = get_cashBal_from_latest_row()
    print(doge_cashBal, usdt_cashBal)


get_account_funds()

columns_usdt = ['usdt']
data_usdt = pd.DataFrame(columns=columns_usdt)

columns_rise_earning = ['earning']
data_rise_earning = pd.DataFrame(columns=columns_rise_earning)

columns_rise_loss = ['loss']
data_rise_loss = pd.DataFrame(columns=columns_rise_loss)

columns_down_earning = ['earning']
data_down_earning = pd.DataFrame(columns=columns_down_earning)

columns_down_loss = ['loss']
data_down_loss = pd.DataFrame(columns=columns_down_loss)

usdt_rise_k = 1
usdt_down_k = 1
rise_n = 0
rise_p = 0
rise_p_n = 0
rise_b = 0
rise_q = 0
rise_q_n = 0

down_n = 0
down_p = 0
down_p_n = 0
down_b = 0
down_q = 0
down_q_n = 0

usdt_protect = 0
usdt_protect_trade = 0

rise_kelly = 0
down_kelly = 0
def place_order_isolated(amount, side, reason, price=None):
    global order_sure
    global doge_delta
    global usdt_delta
    global eq_flow_usdt
    global doge_cashBal
    global usdt_cashBal
    global doge_margin
    global usdt_margin
    global doge_liab
    global usdt_liab
    global doge_interest
    global usdt_interest

    global data_usdt
    global data_rise_earning
    global data_rise_loss
    global data_down_earning
    global data_down_loss

    global usdt_rise_k
    global usdt_down_k
    global usdt_protect
    global usdt_protect_trade

    global rise_n
    global rise_p
    global rise_p_n
    global rise_b
    global rise_q
    global rise_q_n

    global down_n
    global down_p
    global down_p_n
    global down_b
    global down_q
    global down_q_n

    global rise_kelly
    global down_kelly

    place_order_db_config = {
        "host": 'localhost',  # 数据库主机
        "user": "root",  # 数据库用户名
        "password": "密码",  # 数据库密码
        "database": "数据库",  # 数据库名
    }

    def ensure_table_exists(table_name):
        connection = connect_to_db()
        if connection:
            cursor = connection.cursor()
            # 检查表是否存在
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = '{place_order_db_config['database']}' AND table_name = '{table_name}'
            """)
            table_exists = cursor.fetchone()[0]

            # 如果表不存在，则创建表
            if not table_exists:
                cursor.execute(f"""
                    CREATE TABLE `{table_name}` (
                        readable_time VARCHAR(50),
                        timestamp BIGINT PRIMARY KEY,
                        leverage INT,
                        tdMode VARCHAR(50),
                        amount DECIMAL(20, 8),
                        price DECIMAL(20, 8),
                        side VARCHAR(10),
                        ordType VARCHAR(10),
                        doge_pos DECIMAL(20, 8),
                        usdt_pos DECIMAL(20, 8),
                        doge_cashBal DECIMAL(20, 8),
                        usdt_cashBal DECIMAL(20, 8),
                        doge_margin DECIMAL(20, 8),
                        usdt_margin DECIMAL(20, 8),
                        doge_liab DECIMAL(20, 8),
                        usdt_liab DECIMAL(20, 8),
                        doge_interest DECIMAL(20, 8),
                        usdt_interest DECIMAL(20, 8),
                        doge_earning DECIMAL(20, 8),
                        usdt_earning DECIMAL(20, 8),
                        earning_change_to_usdt DECIMAL(20, 8),
                        eq_flow_usdt DECIMAL(20, 8)
                    )
                """)
                print(f"表 {table_name} 已创建。")
            else:
                print(f"表 {table_name} 已存在。")

            connection.commit()
            cursor.close()
            connection.close()

    # 连接数据库
    def connect_to_db():
        try:
            connection = mysql.connector.connect(**place_order_db_config)
            if connection.is_connected():
                print("成功连接到数据库")
                return connection
        except Error as e:
            print(f"连接数据库失败: {e}")
            return None

    connection = connect_to_db()

    # 根据传入的价格和订单类型
    params = {
        "instId": "doge-USDT",
        "tdMode": "isolated",
        "timestamp": time_flow,
        "side": side,
        "ordType": "market",
        "px": Decimal(str(latest_price)),  # 确保使用 Decimal
        "sz": Decimal(str(abs(amount))),  # 确保使用 Decimal
        "leverage": 5
    }

    # 将 timestamp 转换为可读时间
    readable_time = datetime.fromtimestamp(time_flow / 1000, timezone.utc)

    # 确保表存在
    table_name = params["instId"].replace("-", "_")
    ensure_table_exists(table_name)

    # 确保 amount 和 price 值从 params 中获取
    db_amount = params["sz"]
    db_price = params["px"]

    # 获取 timestamp 最大的记录
    cursor = connection.cursor(dictionary=True)  # 设置游标为字典模式
    cursor.execute(f"""
        SELECT timestamp,doge_pos, usdt_pos, doge_margin, usdt_margin, doge_liab, usdt_liab, doge_cashBal,usdt_cashBal, doge_interest,usdt_interest
        FROM `{table_name}`
        WHERE timestamp = (SELECT MAX(timestamp) FROM `{table_name}`)
    """)
    last_record = cursor.fetchone()

    # 提取 previous 数据
    # 从数据库记录中提取的初始变量，若无记录则初始化为 0
    previous_timestamp = last_record['timestamp']
    doge_pos = Decimal(last_record['doge_pos']) if last_record else Decimal(0)  # doge 持仓
    usdt_pos = Decimal(last_record['usdt_pos']) if last_record else Decimal(0)  # USDT 持仓
    doge_margin = Decimal(last_record['doge_margin']) if last_record else Decimal(0)  # doge 保证金
    usdt_margin = Decimal(last_record['usdt_margin']) if last_record else Decimal(0)  # USDT 保证金
    doge_liab = Decimal(last_record['doge_liab']) if last_record else Decimal(0)  # doge 负债
    usdt_liab = Decimal(last_record['usdt_liab']) if last_record else Decimal(0)  # USDT 负债
    doge_cashBal = Decimal(last_record['doge_cashBal']) if last_record else Decimal(0)  # doge 现金余额
    usdt_cashBal = Decimal(last_record['usdt_cashBal']) if last_record else Decimal(0)  # USDT 现金余额
    doge_interest = Decimal(last_record['doge_interest']) if last_record else Decimal(0)  # doge 现金余额
    usdt_interest = Decimal(last_record['usdt_interest']) if last_record else Decimal(0)  # USDT 现金余额

    doge_earning = Decimal(0)
    usdt_earning = Decimal(0)

    price = Decimal(str(latest_price))  # 最新市场价格

    if params["ordType"] == "market":
        amount_gain = Decimal(str(abs(amount * 0.999)))  # 订单数量
        amount_cost = Decimal(str(abs(amount)))
    else:
        print(f"还没做这个订单类型")
        raise TypeError

    def count_full_hours(start_timestamp, end_timestamp):
        # 将时间戳转换为UTC时间的datetime对象
        start_time = datetime.fromtimestamp(start_timestamp / 1000, tz=timezone.utc)  # 修改这里
        end_time = datetime.fromtimestamp(end_timestamp / 1000, tz=timezone.utc)  # 修改这里

        # 变量初始化
        full_hour_count = 0

        # 如果开始时间是整点，跳过它
        if start_time.minute == 0 and start_time.second == 0:
            start_time += timedelta(hours=1)

        # 如果结束时间是整点，计入
        if end_time.minute == 0 and end_time.second == 0:
            end_time -= timedelta(seconds=1)  # 确保不超出这个整点

        # 循环从start_time到end_time，每次递增1小时
        current_time = start_time
        while current_time <= end_time:
            # 每次遇到一个整点时间，变量加1
            full_hour_count += 1
            # 向后推移1小时
            current_time += timedelta(hours=1)

        return full_hour_count

    times_interest = count_full_hours(previous_timestamp, time_flow)
    doge_rate = Decimal(0.00000114)
    usdt_rate = Decimal(0.00000342)
    doge_interest += times_interest * doge_rate * doge_liab
    usdt_interest += times_interest * usdt_rate * usdt_liab

    # 新逻辑框架
    if doge_pos == 0 and usdt_pos == 0:
        # 情况 1：doge 和 USDT 持仓均为 0
        # 1.检查side
        # 2.计算所需保证金
        # 3.检查cash是否能支付
        # 4.增加保证金
        # 5.支付保证金
        # 6.更新负债
        # 7.更新持仓
        if side == "buy":
            # 买单逻辑
            margin_predict = amount_cost / (params["leverage"] * price)  # 计算所需保证金
            if margin_predict <= doge_cashBal:
                print("进入1: 使用 doge 余额支付保证金")
                doge_margin = margin_predict
                doge_cashBal -= margin_predict
                usdt_liab = amount_cost  # 更新 USDT 负债
                doge_pos = doge_margin + amount_gain / price  # 更新 doge 持仓
            else:
                print("进入2: doge 余额不足，使用 USDT 支付剩余保证金")
                doge_margin = margin_predict
                margin_predict -= doge_cashBal
                doge_cashBal = 0
                usdt_cashBal -= margin_predict * price  # 从 USDT 余额中扣减
                if usdt_cashBal < 0:
                    raise ValueError("USDT 余额不足，程序中止。")
                usdt_liab = amount_cost
                doge_pos = doge_margin + amount_gain / price

        elif side == "sell":
            # 卖单逻辑
            margin_predict = amount_cost * price / params["leverage"]  # 计算所需保证金
            if margin_predict <= usdt_cashBal:
                print("进入3: 使用 USDT 余额支付保证金")
                usdt_margin = margin_predict
                usdt_cashBal -= margin_predict
                doge_liab = amount_cost  # 更新 doge 负债
                usdt_pos = usdt_margin + amount_gain * price  # 更新 USDT 持仓
            else:
                print("进入4: USDT 余额不足，使用 doge 支付剩余保证金")
                usdt_margin = margin_predict
                margin_predict -= usdt_cashBal
                usdt_cashBal = 0
                doge_cashBal -= margin_predict / price  # 从 doge 余额中扣减
                if doge_cashBal < 0:
                    raise ValueError("doge 余额不足，程序中止。")
                doge_liab = amount_cost
                usdt_pos = usdt_margin + amount_gain * price

        elif side == "close":
            raise ValueError("未建仓")

    elif doge_pos == 0 and usdt_pos != 0:
        # 情况 2：doge 持仓为 0，USDT 持仓不为 0
        # 1.检查side,判断为加仓还是平仓，加仓为建仓逻辑
        # 2.计算交易获得量
        # 3.检查能否还清负债
        # 4.1 还债，减仓
        # 4.2 平仓，反向开仓
        # 5.计算反向开仓数量
        # 6.添加保证金
        # 7.支付保证金
        # 8.更新负债
        # 9.更新持仓
        if side == "buy":
            doge_delta = amount_gain / price  # 计算 doge 变化量
            if doge_delta < doge_liab + doge_interest:
                print("进入5: 减少 doge 负债")
                doge_liab -= doge_delta - doge_interest
                doge_interest = 0
                usdt_pos -= amount_cost  # 更新 USDT 持仓
            else:
                print("进入6: 清算 doge 负债")
                usdt_cashBal += usdt_pos - ((doge_liab + doge_interest) * price) / Decimal(0.999)  # 将剩余 USDT 更新为现金余额
                usdt_earning = usdt_pos - usdt_margin - ((doge_liab + doge_interest) * price) / Decimal(0.999)
                usdt_pos = 0  # 清空 USDT 持仓
                doge_liab = 0  # 清空 doge 负债
                doge_interest = 0
                usdt_margin = 0
                margin_predict = (amount_cost - (doge_liab + doge_interest) * price / Decimal(0.999)) / (
                            params["leverage"] * price)
                if margin_predict <= doge_cashBal:
                    print("进入6.1: 使用 doge 余额支付剩余保证金")
                    doge_margin += margin_predict
                    doge_cashBal -= margin_predict
                    usdt_liab = amount_cost - (doge_liab + doge_interest) * price / Decimal(0.999)
                    doge_pos = doge_margin + (amount_gain - (doge_liab + doge_interest) * price) / price
                else:
                    print("进入7: 使用 USDT 支付剩余保证金")
                    doge_margin += margin_predict
                    margin_predict -= doge_cashBal
                    doge_cashBal = 0
                    usdt_cashBal -= margin_predict * price
                    if usdt_cashBal < 0:
                        raise ValueError("USDT 余额不足，程序中止。")
                    usdt_liab = amount_cost - (doge_liab + doge_interest) * price / Decimal(0.999)
                    doge_pos = doge_margin + (amount_gain - (doge_liab + doge_interest) * price) / price

        elif side == "sell":
            print("进入8: 卖单逻辑")
            margin_predict = amount_cost * price / params["leverage"]
            if margin_predict <= usdt_cashBal:
                print("进入8.1: 使用 USDT 余额支付保证金")
                usdt_margin += margin_predict
                usdt_cashBal -= margin_predict
                doge_liab += amount_cost
                usdt_pos += margin_predict + amount_gain * price
            else:
                print("进入9: 使用 doge 支付剩余保证金")
                usdt_margin += margin_predict
                margin_delta = margin_predict
                margin_predict -= usdt_cashBal
                usdt_cashBal = 0
                doge_cashBal -= margin_predict / price
                if doge_cashBal < 0:
                    raise ValueError("doge 余额不足，程序中止。")
                doge_liab += amount_cost
                usdt_pos += margin_delta + amount_gain * price

        elif side == "close":
            usdt_cashBal += usdt_pos - ((doge_liab + doge_interest) * price) / Decimal(0.999)
            usdt_earning = usdt_pos - usdt_margin - ((doge_liab + doge_interest) * price) / Decimal(0.999)
            usdt_pos = 0  # 清空 USDT 持仓
            doge_liab = 0  # 清空 doge 负债
            doge_interest = 0
            usdt_margin = 0

            # if down_kelly == 1:
            #     down_kelly = 0
            #     if usdt_earning > 0:
            #         down_earning = pd.DataFrame({'earning': [usdt_earning]})  # 注意用列表包裹值
            #         data_down_earning = pd.concat([data_down_earning,down_earning], ignore_index=True)
            #     if usdt_earning < 0:
            #         down_loss = pd.DataFrame({'loss': [usdt_earning]})  # 注意用列表包裹值
            #         data_down_loss = pd.concat([data_down_loss,down_loss], ignore_index=True)
            #
            #     new_row = pd.DataFrame({'usdt': [usdt_cashBal]})  # 注意用列表包裹值
            #     data_usdt = pd.concat([data_usdt, new_row], ignore_index=True)
            #     down_n += 1
            #
            #     if len(data_usdt) == 1:
            #         if data_usdt.iloc[-1]['usdt'] >= Decimal(500):
            #             down_p_n += 1
            #         elif data_usdt.iloc[-1]['usdt'] < Decimal(500):
            #             down_q_n += 1
            #
            #     if len(data_usdt) >= 2:
            #         if (data_usdt.iloc[-1]['usdt'] - data_usdt.iloc[-2]['usdt']) >= 0 :
            #             down_p_n += 1
            #             down_p = down_p_n / down_n
            #         elif (data_usdt.iloc[-1]['usdt'] - data_usdt.iloc[-2]['usdt']) < 0:
            #             down_q_n += 1
            #
            #     if down_p_n > 0 and down_q_n > 0:
            #         down_b = -data_down_earning['earning'].mean()/data_down_loss['loss'].mean()
            #         usdt_down_k = ((down_p*down_b) - (1-down_p))/down_b
            #
            #         print(f'usdt_down_k:{usdt_down_k}')

    elif doge_pos != 0 and usdt_pos == 0:
        # 情况 3：doge 持仓不为 0，USDT 持仓为 0
        # 1.检查side,判断为加仓还是平仓，加仓为建仓逻辑
        # 2.计算交易获得量
        # 3.检查能否还清负债
        # 4.1 还债，减仓
        # 4.2 平仓，反向开仓
        # 5.计算反向开仓数量
        # 6.添加保证金
        # 7.支付保证金
        # 8.更新负债
        # 9.更新持仓
        if side == "buy":
            margin_predict = amount_cost / (params["leverage"] * price)
            if margin_predict <= doge_cashBal:
                print("进入10: 使用 doge 余额支付保证金")
                doge_margin += margin_predict
                doge_cashBal -= margin_predict
                usdt_liab += amount_cost
                doge_pos += margin_predict + amount_gain / price
            else:
                print("进入11: doge 余额不足，使用 USDT 支付剩余保证金")
                doge_margin += margin_predict
                margin_delta = margin_predict
                margin_predict -= doge_cashBal
                doge_cashBal = 0
                usdt_cashBal -= margin_predict * price
                if usdt_cashBal < 0:
                    raise ValueError("USDT 余额不足，程序中止。")
                usdt_liab += amount_cost
                doge_pos += margin_delta + amount_gain / price

        elif side == "sell":
            print("进入12: 卖单逻辑")
            usdt_delta = amount_gain * price  # 计算 USDT 变化量
            if usdt_delta < usdt_liab + usdt_interest:
                print("进入12.1: 减少 USDT 负债")
                usdt_liab -= usdt_delta - usdt_interest
                doge_pos -= amount_cost
            else:
                print("进入13: 清算 USDT 负债")
                doge_cashBal += doge_pos - ((usdt_liab + usdt_interest) / price) / Decimal(0.999)
                doge_earning = doge_pos - doge_margin - ((usdt_liab + usdt_interest) / price) / Decimal(0.999)
                usdt_interest = 0
                doge_pos = 0
                usdt_liab = 0
                doge_margin = 0
                margin_predict = (amount_cost - (usdt_liab + usdt_interest) / (price * Decimal(0.999))) * price / \
                                 params["leverage"]
                if margin_predict <= usdt_cashBal:
                    print("进入13.1: 使用 USDT 余额支付保证金")
                    usdt_margin += margin_predict
                    usdt_cashBal -= margin_predict
                    doge_liab += amount_cost - (usdt_liab + usdt_interest) / (price * Decimal(0.999))
                    usdt_pos += usdt_margin + (amount_gain - (usdt_liab + usdt_interest) / price) * price
                else:
                    print("进入14: 使用 doge 支付剩余保证金")
                    usdt_margin += margin_predict
                    margin_predict -= usdt_cashBal
                    usdt_cashBal = 0
                    doge_cashBal -= margin_predict / price
                    if doge_cashBal < 0:
                        raise ValueError("doge 余额不足，程序中止。")
                    doge_liab += amount_cost - (usdt_liab + usdt_interest) / (price * Decimal(0.999))
                    usdt_pos += usdt_margin + (amount_gain - (usdt_liab + usdt_interest) / price) * price

        elif side == "close":
            doge_cashBal += doge_pos - ((usdt_liab + usdt_interest) / price) / Decimal(0.999)
            doge_earning = doge_pos - doge_margin - ((usdt_liab + usdt_interest) / price) / Decimal(0.999)
            usdt_interest = 0
            doge_pos = 0
            usdt_liab = 0
            doge_margin = 0

            usdt_cashBal += doge_cashBal*price / Decimal(0.999)#之所以后续发现现货变多，是由于USDT借贷出去，相当于加大投入了
            doge_cashBal = 0

            # usdt_earning += doge_earning * price  # 之所以后续发现现货变多，是由于USDT借贷出去，相当于加大投入了
            # doge_earning = 0

            # if rise_kelly == 1:
            #     rise_kelly = 0
            #     if usdt_earning > 0:
            #         rise_earning = pd.DataFrame({'earning': [usdt_earning]})  # 注意用列表包裹值
            #         data_rise_earning = pd.concat([data_rise_earning, rise_earning], ignore_index=True)
            #     if usdt_earning < 0:
            #         rise_loss = pd.DataFrame({'loss': [usdt_earning]})  # 注意用列表包裹值
            #         data_rise_loss = pd.concat([data_rise_loss, rise_loss], ignore_index=True)
            #
            #     new_row = pd.DataFrame({'usdt': [usdt_cashBal]})  # 注意用列表包裹值
            #     data_usdt = pd.concat([data_usdt, new_row], ignore_index=True)
            #     rise_n += 1
            #
            #     if len(data_usdt) == 1:
            #         if data_usdt.iloc[-1]['usdt'] >= Decimal(500):
            #             print('OK')
            #             rise_p_n += 1
            #         elif data_usdt.iloc[-1]['usdt'] < Decimal(500):
            #             rise_q_n += 1
            #
            #     if len(data_usdt) >= 2:
            #         if (data_usdt.iloc[-1]['usdt'] - data_usdt.iloc[-2]['usdt']) >= 0:
            #             rise_p_n += 1
            #             rise_p = rise_p_n / rise_n
            #         elif (data_usdt.iloc[-1]['usdt'] - data_usdt.iloc[-2]['usdt']) < 0:
            #             rise_q_n += 1
            #
            #     if rise_p_n > 0 and rise_q_n > 0:
            #
            #         rise_b = -data_rise_earning['earning'].mean() / data_rise_loss['loss'].mean()
            #         usdt_rise_k = ((rise_p * rise_b) - (1 - rise_p)) / rise_b
            #
            #         print(f'usdt_rise_k:{usdt_rise_k}')

    print("模拟下单参数:", params)

    new_doge_pos = doge_pos
    new_usdt_pos = usdt_pos
    eq_flow_usdt = usdt_cashBal + new_usdt_pos - usdt_liab + (new_doge_pos - doge_liab + doge_cashBal) * price
    earning_change_to_usdt = doge_earning * price + usdt_earning

    cursor.close()
    # 将订单信息保存到数据库
    if connection:
        cursor = connection.cursor()
        cursor.execute(f"""
                    INSERT INTO `{table_name}` (readable_time, timestamp, leverage, tdMode, amount, price, side, ordType, doge_pos, usdt_pos, eq_flow_usdt, doge_cashBal, usdt_cashBal, doge_margin, usdt_margin, doge_liab, usdt_liab, doge_interest, usdt_interest, doge_earning, usdt_earning,earning_change_to_usdt ) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s)
                    ON DUPLICATE KEY UPDATE price = VALUES(price), amount = VALUES(amount),  doge_pos = VALUES(doge_pos), usdt_pos = VALUES(usdt_pos), eq_flow_usdt = VALUES(eq_flow_usdt),doge_cashBal = VALUES(doge_cashBal), usdt_cashBal = VALUES(usdt_cashBal), doge_margin = VALUES(doge_margin), usdt_margin = VALUES(usdt_margin), doge_liab = VALUES(doge_liab), usdt_liab = VALUES(usdt_liab), doge_interest = VALUES(doge_interest), usdt_interest = VALUES(usdt_interest), doge_earning = VALUES(doge_earning), usdt_earning = VALUES(usdt_earning), earning_change_to_usdt = VALUES(earning_change_to_usdt)
                """, (
            readable_time, params["timestamp"], params["leverage"], params["tdMode"], db_amount, db_price, side,
            params["ordType"], new_doge_pos, new_usdt_pos, eq_flow_usdt, doge_cashBal, usdt_cashBal, doge_margin,
            usdt_margin,
            doge_liab, usdt_liab, doge_interest, usdt_interest, doge_earning, usdt_earning, earning_change_to_usdt))
        connection.commit()
        cursor.close()
        connection.close()  # 连接归还到连接池

    print(f"模拟订单已保存到表 {table_name} 中")
    order_sure = 1


# 现货下单

# 初始化一个空的DataFrame，定义列名
columns = ['timestamp', 'close', 'high', 'low', 'volume']
data_frame = pd.DataFrame(columns=columns)

columns_new = ['latest_price']
data_new = pd.DataFrame(columns=columns_new)

columns_reserve = ['pivot_point', 'R1', 'R2', 'R3', 'S1', 'S2', 'S3', 'short_volume_signal', 'mid_volume_signal',
                   'short_term_trend', 'mid_term_trend', 'main_trend']
data_reserve = pd.DataFrame(columns=columns_reserve)

# # 获取100个历史数据，假设API返回的数据包含时间戳、收盘价、最高价、最低价、成交量等
def fetch_initial_data():
    try:
        # 连接 MySQL 数据库
        conn = mysql.connector.connect(
            host='localhost',  # 数据库地址
            user='root',  # 数据库用户名
            password='密码',  # 数据库密码
            database='数据库'  # 使用的数据库名
        )
        cursor = conn.cursor(dictionary=True)

        # 从数据库获取 time_flow_start 之前的100条数据（不包括 time_flow_start）
        query = """
        SELECT * FROM okx_doge_ustd_1h
        WHERE timestamp < %s
        ORDER BY timestamp DESC
        LIMIT 250
        """
        cursor.execute(query, (time_flow_start,))
        results = cursor.fetchall()

        # 转换数据为 DataFrame
        initial_data = []
        for item in (results):  #
            timestamp = datetime.fromtimestamp(item['timestamp'] / 1000, timezone.utc)
            close = float(item['close_price'])
            high = float(item['high_price'])
            low = float(item['low_price'])
            volume = float(item['volume'])
            initial_data.append([timestamp, close, high, low, volume])

        # 返回 DataFrame
        columns = ['timestamp', 'close', 'high', 'low', 'volume']
        return pd.DataFrame(initial_data, columns=columns)

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return pd.DataFrame(columns=['timestamp', 'close', 'high', 'low', 'volume'])

    finally:
        # 关闭数据库连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# 每分钟获取最新数据，并更新data_frame
def update_data():
    global data_frame

    while True:  # 无限重试
        conn = None  # 初始化连接
        cursor = None  # 初始化游标
        try:
            # 连接 MySQL 数据库
            conn = mysql.connector.connect(
                host='localhost',  # 数据库地址
                user='root',  # 数据库用户名
                password='密码',  # 数据库密码
                database='数据库'  # 使用的数据库名
            )
            cursor = conn.cursor(dictionary=True)

            # 查询整行数据
            query = """
            SELECT * FROM okx_doge_ustd_1h WHERE timestamp = %s
            """
            cursor.execute(query, (time_plan,))
            result = cursor.fetchone()

            # 检查结果是否为空
            if not result:
                print(f"No data found for timestamp: {time_plan}")
                return None

            # 提取结果并处理
            close = result['close_price']
            high = result['high_price']
            low = result['low_price']
            volume = result['volume']
            timestamp = datetime.fromtimestamp(result['timestamp'] / 1000, timezone.utc)

            # 将新数据添加到 new_data 中
            new_data = pd.DataFrame([[timestamp, close, high, low, volume]], columns=columns)

            # 通过 concat 合并并更新 data_frame，保持最多 100 条数据
            data_frame = pd.concat([new_data, data_frame], ignore_index=True)

            if len(data_frame) > 250:
                data_frame = data_frame.iloc[:-1].reset_index(drop=True)
            print(data_frame.head())

            # 输出最新的 data_frame 内容并保存到 CSV 文件
            data_frame.to_csv('market_data.csv', index=False)  # 保存为 CSV 文件

            # 返回查询结果
            return result

        except mysql.connector.Error as err:
            print(f"Database error: {err}")
            print("Retrying...")  # 打印每次重试的信息
            time.sleep(2)  # 等待 2 秒后重试

        except UnboundLocalError as e:
            print(f"UnboundLocalError occurred: {e}")
            print("Retrying in 2 seconds...")
            time.sleep(2)  # 等待 2 秒后重试

        finally:
            # 关闭数据库连接
            if cursor:
                cursor.close()
            if conn:
                conn.close()


def calculate_support_resistance():
    global data_frame
    global pivot_point, R1, R2, R3, S1, S2, S3

    high = short_high_price
    low = short_low_price
    close = latest_price

    pivot_point = (high + low + close) / 3
    R1 = (2 * pivot_point) - low
    R2 = pivot_point + (high - low)
    R3 = high + 2 * (pivot_point - low)
    S1 = (2 * pivot_point) - high
    S2 = pivot_point - (high - low)
    S3 = low - 2 * (high - pivot_point)

    data_reserve['pivot_point'] = pivot_point
    data_reserve['R1'] = R1
    data_reserve['R2'] = R2
    data_reserve['R3'] = R3
    data_reserve['S1'] = S1
    data_reserve['S2'] = S2
    data_reserve['S3'] = S3
    # print(f"pivot_point: {pivot_point}")
    print(f'R1:{R1},R2:{R2}')

    return data_reserve


def volume_analysis():
    global data_frame
    global short_volume_signal
    global mid_volume_signal
    global long_volume_signal

    global short_volume_ratio
    global mid_volume_ratio
    global long_volume_ratio
    global volume_ratio

    global short_slope_volume_ratio
    global mid_slope_volume_ratio
    global long_slope_volume_ratio

    global new_volume
    global before_volume

    global new_volume_short_ratio
    global new_volume_mid_ratio
    global new_volume_long_ratio

    global slope_volume_ratio

    global short_volume_trend_signal
    global mid_volume_trend_signal
    global long_volume_trend_signal

    global volume_slope_signal

    # 使用灵活的成交量分析
    short_window_size = 7
    mid_window_size = 30
    long_window_size = 90

    long_long_window_size = 240
    current_window_size = 5


    short_size = 6
    mid_size = 29
    long_size = 89
    long_long_size = 239
    current_size = 5

    short_avg_volume = data_frame['volume'].rolling(window=short_window_size).mean().iloc[short_size]
    mid_avg_volume = data_frame['volume'].rolling(window=mid_window_size).mean().iloc[mid_size]
    long_avg_volume = data_frame['volume'].rolling(window=long_window_size).mean().iloc[long_size]
    current_avg_volume = data_frame['volume'].rolling(window=current_window_size).mean().iloc[current_size]


    short_avg_volume_before = data_frame['volume'].rolling(window=short_window_size).mean().iloc[short_size+1]
    mid_avg_volume_before = data_frame['volume'].rolling(window=mid_window_size).mean().iloc[mid_size+1]
    long_avg_volume_before = data_frame['volume'].rolling(window=long_window_size).mean().iloc[long_size+1]

    new_volume = data_frame.iloc[0]['volume']
    before_volume = data_frame.iloc[1]['volume']

    new_volume_short_ratio = new_volume / short_avg_volume
    new_volume_mid_ratio = new_volume / mid_avg_volume
    new_volume_long_ratio = new_volume / long_avg_volume

    short_volume_ratio = short_avg_volume / mid_avg_volume
    mid_volume_ratio = mid_avg_volume / long_avg_volume
    long_volume_ratio = short_avg_volume / long_avg_volume

    short_slope_volume_ratio = (short_avg_volume-short_avg_volume_before) / (mid_avg_volume-mid_avg_volume_before)
    mid_slope_volume_ratio = (mid_avg_volume-mid_avg_volume_before) / (long_avg_volume-long_avg_volume_before)
    long_slope_volume_ratio = (short_avg_volume-short_avg_volume_before) / (long_avg_volume-long_avg_volume_before)

    slope_volume_ratio = new_volume/before_volume

    if new_volume_short_ratio >= 1:
        short_volume_signal = '短期放量'
    elif new_volume_short_ratio < 1:
        short_volume_signal = '短期缩量'

    if short_volume_ratio >= 1:
        short_volume_trend_signal = '短期放量趋势'
    elif short_volume_ratio < 1:
        short_volume_trend_signal = '短期缩量趋势'

    if new_volume_mid_ratio >= 1:
        mid_volume_signal = '中期放量'
    elif new_volume_mid_ratio < 1:
        mid_volume_signal = '中期缩量'

    if mid_volume_ratio >= 1:
        mid_volume_trend_signal = '中期放量趋势'
    elif mid_volume_ratio < 1:
        mid_volume_trend_signal = '中期缩量趋势'

    if new_volume_long_ratio >= 1:
        long_volume_signal = '长期放量'
    elif new_volume_long_ratio < 1:
        long_volume_signal = '长期缩量'

    if long_volume_ratio >= 1:
        long_volume_trend_signal = '长期放量趋势'
    elif long_volume_ratio < 1:
        long_volume_trend_signal = '长期缩量趋势'

    if slope_volume_ratio >= 1:
        volume_slope_signal = '交易量仰角'
    elif slope_volume_ratio < 1:
        volume_slope_signal = '交易量俯角'

    print(f'slope_volume_ratio:{volume_slope_signal}，new_volume_short_ratio:{short_volume_signal},short_volume_ratio:{short_volume_trend_signal}')

    return data_reserve


# 确定趋势
def calculate_trends():
    global data_frame
    global short_term_trend
    global mid_term_trend
    global long_term_trend
    global long_long_term_trend

    global main_trend
    global short_term_ma
    global mid_term_ma
    global long_term_ma
    global long_long_term_ma

    global short_amplitude_ratio
    global mid_amplitude_ratio
    global long_amplitude_ratio

    global short_slope_ratio
    global mid_slope_ratio
    global long_slope_ratio

    global short_price_ratio
    global mid_price_ratio
    global long_price_ratio

    global new_price
    global before_price

    global new_price_short_ratio
    global new_price_mid_ratio
    global new_price_long_ratio

    global slope_price_ratio
    global price_slope_signal

    global rise_switch
    global down_switch
    # 转换数据列为数值类型
    # high_price = latest_data.get('high', None)
    # low_price = latest_data.get('low', None)

    short_window_size = 7
    mid_window_size = 30
    long_window_size = 90
    long_long_window_size = 240

    short_size = 6
    mid_size = 29
    long_size = 89
    long_long_size = 239

    data_frame['close'] = pd.to_numeric(data_frame['close'])
    data_frame['volume'] = pd.to_numeric(data_frame['volume'])
    data_frame['high'] = pd.to_numeric(data_frame['high'])
    data_frame['low'] = pd.to_numeric(data_frame['low'])

    # 使用简单移动平均线来确定短期和中期趋势
    short_term_ma = data_frame['close'].rolling(window=short_window_size).mean().iloc[short_size]  # 短期平均线 (20天)
    mid_term_ma = data_frame['close'].rolling(window=mid_window_size).mean().iloc[mid_size]  # 中期平均线 (50天)
    long_term_ma = data_frame['close'].rolling(window=long_window_size).mean().iloc[long_size]  # 中期平均线 (50天)
    long_long_term_ma = data_frame['close'].rolling(window=long_long_window_size).mean().iloc[long_long_size]  # 中期平均线 (50天)

    if latest_price > long_term_ma > long_long_term_ma:
        rise_switch = 1
    else:
        rise_switch = 0
    if latest_price < long_term_ma < long_long_term_ma:
        down_switch = 1
    else:
        down_switch = 0

    print(f"rise_switch: {rise_switch},down_switch: {down_switch}")
    print(f"long_term_ma: {long_term_ma},long_long_term_ma: {long_long_term_ma}")

    short_term_ma_before = data_frame['close'].rolling(window=short_window_size).mean().iloc[short_size+1]
    mid_term_ma_before = data_frame['close'].rolling(window=mid_window_size).mean().iloc[mid_size+1]  # 中期平均线 (50天)
    long_term_ma_before = data_frame['close'].rolling(window=long_window_size).mean().iloc[long_size+1]  # 中期平均线 (50天)
    long_long_term_ma_before = data_frame['close'].rolling(window=long_long_window_size).mean().iloc[long_long_size + 1]  # 中期平均线 (50天)

    new_price = data_frame.iloc[0]['close']
    before_price = data_frame.iloc[1]['close']

    new_price_short_ratio = new_price / short_term_ma
    new_price_mid_ratio = new_price / mid_term_ma
    new_price_long_ratio = new_price / long_term_ma

    slope_price_ratio = new_price / before_price

    # 使用移动平均线和成交量来分别确定短期和中期趋势
    # 短期趋势判断逻辑
    short_price_ratio = short_term_ma / mid_term_ma
    mid_price_ratio = mid_term_ma / long_term_ma
    long_price_ratio = short_term_ma / long_term_ma

    short_slope_ratio = (short_term_ma-short_term_ma_before) / (mid_term_ma-mid_term_ma_before)
    mid_slope_ratio = (mid_term_ma-mid_term_ma_before) / (long_term_ma-long_term_ma_before)
    long_slope_ratio = (short_term_ma-short_term_ma_before) / (long_term_ma - long_term_ma_before)

    short_term_conditions = [(short_term_ma > mid_term_ma),
                             (short_term_ma < mid_term_ma)
                             ]

    short_term_choices = ['短期上升趋势', '短期下降趋势']
    short_term_trend = np.select(short_term_conditions, short_term_choices, default='短期震荡趋势')

    # # 中期趋势判断逻辑
    mid_term_conditions = [(mid_term_ma > long_term_ma),
                           (mid_term_ma < long_term_ma)
                           ]

    mid_term_choices = ['中期上升趋势', '中期下降趋势']
    mid_term_trend = np.select(mid_term_conditions, mid_term_choices, default='中期震荡趋势')

    # # 长期趋势判断逻辑
    long_term_conditions = [(short_term_ma > long_term_ma),
                           (short_term_ma < long_term_ma)
                           ]

    long_term_choices = ['长期上升趋势', '长期下降趋势']
    long_term_trend = np.select(long_term_conditions, long_term_choices, default='长期震荡趋势')

    long_long_term_conditions = [(short_term_ma > long_long_term_ma),
                           (short_term_ma < long_long_term_ma)
                           ]

    long_long_term_choices = ['长长期上升趋势', '长长期下降趋势']
    long_long_term_trend = np.select(long_long_term_conditions, long_long_term_choices, default='长期震荡趋势')

    # 细分短期震荡趋势

    short_random_window_size = 6
    mid_random_window_size = 29
    long_random_window_size = 89

    short_high_diff = data_frame['high'].rolling(window=short_random_window_size).max().iloc[short_random_window_size] - \
                data_frame['low'].rolling(window=short_random_window_size).min().iloc[short_random_window_size]
    short_avg_close = data_frame['close'].rolling(window=short_random_window_size).mean().iloc[short_random_window_size]
    short_amplitude_ratio = short_high_diff / short_avg_close

    mid_high_diff = data_frame['high'].rolling(window=mid_random_window_size).max().iloc[mid_random_window_size] - \
                data_frame['low'].rolling(window=mid_random_window_size).min().iloc[mid_random_window_size]
    mid_avg_close = data_frame['close'].rolling(window=mid_random_window_size).mean().iloc[mid_random_window_size]
    mid_amplitude_ratio = mid_high_diff / mid_avg_close

    long_high_diff = data_frame['high'].rolling(window=long_random_window_size).max().iloc[long_random_window_size] - \
                data_frame['low'].rolling(window=long_random_window_size).min().iloc[long_random_window_size]
    long_avg_close = data_frame['close'].rolling(window=long_random_window_size).mean().iloc[long_random_window_size]
    long_amplitude_ratio = long_high_diff / long_avg_close



    # 确定主趋势
    main_trend_conditions = [
        # 主趋势为上升趋势：短期和中期均为上升趋势
        (short_term_trend == '短期上升趋势'),
        # 主趋势为下降趋势：短期和中期均为下降趋势
        (short_term_trend == '短期下降趋势')
    ]

    main_trend_choices = ['上升趋势', '下降趋势']
    main_trend = np.select(main_trend_conditions, main_trend_choices, default='无')
    data_reserve['main_trend'] = main_trend

    if slope_price_ratio >= 1:
        price_slope_signal = '价格仰角'
    elif slope_price_ratio < 1:
        price_slope_signal = '价格俯角'

    print(f"short_term_trend: {short_term_trend}，mid_term_trend: {mid_term_trend},long_term_trend: {long_term_trend}")
    print(f"price_slope_signal: {price_slope_signal}")
    return


def get_market_data():
    global latest_price

    while True:  # 无限重试
        cursor = None  # 初始化 cursor
        conn = None  # 初始化 conn
        try:
            # 连接 MySQL 数据库
            conn = mysql.connector.connect(
                host='localhost',  # 数据库地址
                user='root',  # 数据库用户名
                password='密码',  # 数据库密码
                database='数据库'  # 使用的数据库名
            )
            cursor = conn.cursor()

            # 查询 close_price
            query = """
                SELECT close_price FROM okx_doge_ustd_1m WHERE timestamp = %s
                """
            cursor.execute(query, (time_flow,))
            result = cursor.fetchone()
            data_new['latest_price'] = result

            # 检查结果是否为空
            if result:
                latest_price = result[0]

            else:
                print(f"No close_price found for timestamp: {time_flow}")
                return None

            # 如果没有触发错误，退出循环
            break  # 正常获取数据，退出重试

        except mysql.connector.Error as err:
            print(f"Database error: {err}")
            print(f"Retrying...")  # 打印每次重试的信息
            time.sleep(2)  # 等待 10 秒后重试

        except UnboundLocalError as e:
            # 捕获 UnboundLocalError 并重新执行
            print(f"UnboundLocalError occurred: {e}")
            print(f"Retrying in 10 seconds...")
            time.sleep(2)  # 等待 10 秒后重试

        finally:
            # 关闭数据库连接
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# 配置重试策略
def create_retry_session():
    session = requests.Session()
    retry = Retry(
        total=1,  # 最大重试次数
        backoff_factor=2,  # 每次重试间隔时间的基数，单位为秒
        status_forcelist=[429, 500, 502, 503, 504],  # 哪些状态码时需要重试
        allowed_mbtcods=["GET", "POST"],  # 允许重试的请求方法
        raise_on_status=False  # 对于返回的错误状态码不直接抛出异常
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# 示例主程序
if __name__ == "__main__":
    # 定时更新交易预案数据并执行交易计划
    def price_info():
        global short_high_price
        global short_low_price
        global mid_high_price
        global mid_low_price
        global long_high_price
        global long_low_price
        global long_long_high_price
        global long_long_low_price

        global short_high_price_data
        global short_low_price_data
        global mid_high_price_data
        global mid_low_price_data
        global long_high_price_data
        global long_low_price_data
        global long_long_high_price_data
        global long_long_low_price_data

        global close_price_data

        global form_high_price
        global form_high_second_price
        global form_low_price
        global form_low_second_price

        global volatility
        global volatility_index_distance
        global high_gap_ratio
        global low_gap_ratio
        global high_index_distance
        global low_index_distance

        global v_high_price

        global double_low_time_1
        global double_low_time_2

        global double_low_price_1
        global double_low_price_2
        global double_low_slope

        global double_top_slope
        global double_top_time_1
        global double_top_time_2
        global double_top_price_1
        global double_top_price_2

        global double_top_low_price
        global double_low_high_price

        global sorted_max_volume_highest_price_data
        global sorted_max_volume_lowest_price_data

        global mid_max_volume_idx
        global mid_max_volume_highest_price
        global mid_max_volume_lowest_price

        global short_max_volume_idx
        global short_max_volume_highest_price
        global short_max_volume_lowest_price

        global long_max_volume_idx
        global long_max_volume_highest_price
        global long_max_volume_lowest_price

        global high_price_data_4h
        global low_price_data_4h
        global close_price_data_4h
        global r15_high_price
        global r15_low_price

        global data_usdt
        global usdt_average

        # print(f'做多凯利仓位:{usdt_rise_k}')
        # print(f'做空凯利仓位:{usdt_down_k}')
        # print(f'做多次数:{rise_n}')
        # print(f'做空次数:{down_n}')
        # print(f'做多成功次数:{rise_p_n}')
        # print(f'做空成功次数:{down_p_n}')
        # print(f'做多失败次数:{rise_q_n}')
        # print(f'做空失败次数:{down_q_n}')
        # print(f'做多盈亏比:{rise_b}')
        # print(f'做空盈亏比:{down_b}')
        # print(f'做多胜率:{rise_p}')
        # print(f'做空胜率:{down_p}')
        #
        # if len(data_usdt) > 0:
        #     print(f'最新资金{data_usdt.iloc[-1]['usdt']}')
        #
        #     print(f'做多平均盈利:{data_rise_earning['earning'].mean()}')
        #     print(f'做多平均亏损:{data_rise_loss['loss'].mean()}')
        #
        #     print(f'做空平均盈利:{data_down_earning['earning'].mean()}')
        #     print(f'做空平均亏损:{data_down_loss['loss'].mean()}')




        high_price_data_4h = data_frame.iloc[0:28]['high']
        low_price_data_4h = data_frame.iloc[0:28]['low']
        close_price_data_4h = data_frame.iloc[0:28]['close']

        v_high_price_data = data_frame.iloc[0:5]['high']
        v_low_price_data = data_frame.iloc[0:5]['low']

        v_high_price = v_high_price_data.max()
        v_high_price_index = v_high_price_data.idxmax()

        v_low_price = v_low_price_data.min()
        v_low_second_price_index = v_low_price_data.idxmin()

        volatility = abs(v_high_price - v_low_price)
        volatility_index_distance = abs(v_high_price_index - v_low_second_price_index)




        form_high_price_data = data_frame.iloc[0:7]['high']
        form_low_price_data = data_frame.iloc[0:7]['low']

        form_high_price = form_high_price_data.max()
        sorted_high = sorted(form_high_price_data, reverse=True)
        form_high_second_price = sorted_high[1]  # 次高价
        form_high_price_index = form_high_price_data.idxmax()
        form_high_second_price_index = form_high_price_data[form_high_price_data == form_high_second_price].index[0]

        # 处理 low 列
        form_low_price = form_low_price_data.min()
        sorted_low = sorted(form_low_price_data)
        form_low_second_price = sorted_low[1]  # 次低价
        form_low_second_price_index = form_low_price_data.idxmin()
        form_low_price_index = form_low_price_data[form_low_price_data == form_low_second_price].index[0]

        high_gap_ratio = (form_high_price-form_high_second_price) / form_high_price
        low_gap_ratio = (form_low_price - form_low_second_price) / form_low_price
        high_index_distance = abs(form_high_price_index - form_high_second_price_index)
        low_index_distance = abs(form_low_price_index - form_low_second_price_index)

        if low_index_distance >= 4:
            if form_low_price_index > form_low_second_price_index:
                double_low_price_data = data_frame.iloc[form_low_second_price_index+1:form_low_price_index-1]['low']
                double_low_high_price = double_low_price_data.max()
                double_low_low_price = double_low_price_data.min()
                double_low_high_price_index = double_low_price_data.idxmax()
                double_low_low_price_index = double_low_price_data.idxmin()
                double_low_slope = (double_low_high_price - double_low_low_price) / (double_low_high_price_index - double_low_low_price_index)
                print(f'double_low_slope:{double_low_slope}')
                if double_low_high_price_index > double_low_low_price_index:
                    double_low_time_1 = double_low_low_price_index
                    double_low_time_2 = double_low_high_price_index
                    double_low_price_1 = double_low_low_price
                    double_low_price_2 = double_low_high_price

                if double_low_high_price_index < double_low_low_price_index:
                    double_low_time_1 = double_low_high_price_index
                    double_low_time_2 = double_low_low_price_index
                    double_low_price_1 = double_low_high_price
                    double_low_price_2 = double_low_low_price

            if form_low_price_index < form_low_second_price_index:
                double_low_price_data= data_frame.iloc[form_low_price_index+1:form_low_second_price_index-1]['low']
                double_low_high_price = double_low_price_data.max()
                double_low_low_price = double_low_price_data.min()
                double_low_high_price_index = double_low_price_data.idxmax()
                double_low_low_price_index = double_low_price_data.idxmin()
                double_low_slope = (double_low_high_price-double_low_low_price)/(double_low_high_price_index-double_low_low_price_index)
                print(f'double_low_slope:{double_low_slope}')

                if double_low_high_price_index > double_low_low_price_index:
                    double_low_time_1 = double_low_low_price_index
                    double_low_time_2 = double_low_high_price_index
                    double_low_price_1 = double_low_low_price
                    double_low_price_2 = double_low_high_price

                if double_low_high_price_index < double_low_low_price_index:
                    double_low_time_1 = double_low_high_price_index
                    double_low_time_2 = double_low_low_price_index
                    double_low_price_1 = double_low_high_price
                    double_low_price_2 = double_low_low_price

        if high_index_distance >= 4:
            if form_high_price_index > form_high_second_price_index:
                double_top_price_data = data_frame.iloc[form_high_second_price_index+1:form_high_price_index-1]['high']
                double_top_high_price = double_top_price_data.max()
                double_top_low_price = double_top_price_data.min()
                double_top_high_price_index = double_top_price_data.idxmax()
                double_top_low_price_index = double_top_price_data.idxmin()
                double_top_slope = (double_top_high_price - double_top_low_price) / (double_top_high_price_index - double_top_low_price_index)
                print(f'double_top_slope:{double_top_slope}')
                if double_top_high_price_index > double_top_low_price_index:
                    double_top_time_1 = double_top_low_price_index
                    double_top_time_2 = double_top_high_price_index
                    double_top_price_1 = double_top_low_price
                    double_top_price_2 = double_top_high_price

                if double_top_high_price_index < double_top_low_price_index:
                    double_top_time_1 = double_top_high_price_index
                    double_top_time_2 = double_top_low_price_index
                    double_top_price_1 = double_top_high_price
                    double_top_price_2 = double_top_low_price

            if form_high_price_index < form_high_second_price_index:
                double_top_price_data= data_frame.iloc[form_high_price_index+1:form_high_second_price_index-1]['high']
                double_top_high_price = double_top_price_data.max()
                double_top_low_price = double_top_price_data.min()
                double_top_high_price_index = double_top_price_data.idxmax()
                double_top_low_price_index = double_top_price_data.idxmin()
                double_top_slope = (double_top_high_price-double_top_low_price)/(double_top_high_price_index-double_top_low_price_index)
                print(f'double_top_slope:{double_top_slope}')

                if double_top_high_price_index > double_top_low_price_index:
                    double_top_time_1 = double_top_low_price_index
                    double_top_time_2 = double_top_high_price_index
                    double_top_price_1 = double_top_low_price
                    double_top_price_2 = double_top_high_price

                if double_top_high_price_index < double_top_low_price_index:
                    double_top_time_1 = double_top_high_price_index
                    double_top_time_2 = double_top_low_price_index
                    double_top_price_1 = double_top_high_price
                    double_top_price_2 = double_top_low_price
        #
        # print(f'high_gap_ratio:{high_gap_ratio},low_gap_ratio:{low_gap_ratio}')
        # # print(f'double_low_slope:{double_low_slope}')
        # print(f'high_index_distance:{high_index_distance},low_index_distance:{low_index_distance}')
        # print(f"高价数值: {form_high_price}, 原始索引: {form_high_price_index}")
        # print(f"次高价数值: {form_high_second_price}, 原始索引: {form_high_second_price_index}")
        # print(f"低价数值: {form_low_price}, 原始索引: {form_low_price_index}")
        # print(f"次低价数值: {form_low_second_price}, 原始索引: {form_low_second_price_index}")
        # print(f'volatility:{volatility},volatility_index_distance:{volatility_index_distance}')



        short_high_price_data = data_frame.iloc[0:6]['high']
        short_high_price = short_high_price_data.max()
        short_low_price_data = data_frame.iloc[0:6]['low']
        short_low_price = short_low_price_data.min()
        mid_high_price_data = data_frame.iloc[0:29]['high']
        mid_high_price = mid_high_price_data.max()
        mid_low_price_data = data_frame.iloc[0:29]['low']
        mid_low_price = mid_low_price_data.min()
        long_high_price_data = data_frame.iloc[0:89]['high']
        long_high_price = long_high_price_data.max()
        long_low_price_data = data_frame.iloc[0:89]['low']
        long_low_price = long_low_price_data.min()
        long_long_high_price_data = data_frame.iloc[0:239]['high']
        long_long_high_price = long_long_high_price_data.max()
        long_long_low_price_data = data_frame.iloc[0:239]['low']
        long_long_low_price = long_long_low_price_data.min()

        r15_high_price_data = data_frame.iloc[0:14]['high']
        r15_high_price = mid_high_price_data.max()
        r15_low_price_data = data_frame.iloc[0:14]['low']
        r15_low_price = mid_low_price_data.min()


        close_price_data = data_frame.iloc[:240]['close']

        short_max_volume_idx = data_frame.iloc[0:7]['volume'].idxmax()
        mid_max_volume_idx = data_frame.iloc[0:30]['volume'].idxmax()
        long_max_volume_idx = data_frame.iloc[0:90]['volume'].idxmax()
        long_long_max_volume_idx = data_frame.iloc[0:240]['volume'].idxmax()

        mid_max_volume_highest_price = data_frame.loc[mid_max_volume_idx, 'high']
        mid_max_volume_lowest_price = data_frame.loc[mid_max_volume_idx, 'low']
        long_max_volume_highest_price = data_frame.loc[long_max_volume_idx, 'high']
        long_max_volume_lowest_price = data_frame.loc[long_max_volume_idx, 'low']
        short_max_volume_highest_price = data_frame.loc[short_max_volume_idx, 'high']
        short_max_volume_lowest_price = data_frame.loc[short_max_volume_idx, 'low']
        long_long_max_volume_highest_price = data_frame.loc[long_long_max_volume_idx, 'high']
        long_long_max_volume_lowest_price = data_frame.loc[long_long_max_volume_idx, 'low']

        max_volume_highest_price_data = [short_max_volume_highest_price, mid_max_volume_highest_price,long_max_volume_highest_price]
        max_volume_lowest_price_data = [short_max_volume_lowest_price, mid_max_volume_lowest_price,long_max_volume_lowest_price]
        sorted_max_volume_highest_price_data = sorted(max_volume_highest_price_data)
        sorted_max_volume_lowest_price_data = sorted(max_volume_lowest_price_data)
        print(f"短期最高价格：{short_high_price},中期最高价格：{mid_high_price},长期最高价格：{long_high_price}  "
              f"短期最低价格：{short_low_price}，中期最低价格：{mid_low_price}，长期最低价格：{long_low_price}")


    global time_flow
    global time_plan
    global time_flow_start

    time_interval = 60 * 60 * 1000
    time_flow_start = 1609459200000

    time_flow = time_flow_start
    time_plan = time_flow - time_interval
    last_executed_timeflow = time_flow_start
    investment_amount = 750
    trading_plan = TradingPlan()

    rise_switch = 0
    down_switch = 0

    get_market_data()
    print(f'最新价格{latest_price}')

    # 获取100条初始数据
    data_frame = fetch_initial_data()

    price_info()

    calculate_support_resistance()
    volume_analysis()
    calculate_trends()


    order_sure = 1
    count = 0
    sign = 0

    def trade_reserve():
        global sign

        update_data()
        price_info()
        calculate_support_resistance()
        volume_analysis()
        calculate_trends()
        sign += 1

    while True:
        get_market_data()
        trading_plan.execute_plan()
        schedule.run_pending()

        time_plan = time_flow - time_interval
        if time_flow - last_executed_timeflow >= 60 * 60 * 1000:
            print(f"Executing trade logic at: {datetime.fromtimestamp(time_flow / 1000, timezone.utc)}")

            # 执行逻辑
            trade_reserve()

            # 更新上次执行的 time_flow 值
            last_executed_timeflow = time_flow
        time_flow += 60000  # 增加 1 秒 (1000 毫秒)
