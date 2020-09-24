# -*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import pandas as pd
import pymysql
"""
Created on Sun Mar  1 01:07:05 2020

@author: Yue Hua Liu
"""
"""
本程式會篩選出目前為止 當月漲幅 > x % 的股票，透過排程工具，將其排在每月最後一天執行
x 預設為 20
"""


#  資料庫連線設定
def connect_proxy():
    db = pymysql.connect(
        '127.0.0.1',
        '********',
        '********',
        'stock')
    return db


def connect_online():
    db = pymysql.connect(
        '**.**.**.**',
        '********',
        '********',
        'stock')
    return db


# 找出當前月份
month = datetime.today().month
day = datetime.today().day


# 資料庫連接，上線要改回去
db = connect_proxy()
cursor_now = db.cursor()
# month-1 到時要把-1改掉(已改)
cursor_now.execute(f"""
               SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE
               FROM tw_price
               WHERE DATE >= "2020-{month}-1" and DATE <= "2020-{month}-31";
               """)
db.close()
data = cursor_now.fetchall()
# %%
# 股票按照 symbol 分組
'''
參數設置區
'''
# 漲幅參數
threshold_rate = 20

raw_data = pd.DataFrame(data)
symbol = list(set(raw_data[0].values))
group_df = raw_data.groupby(0)
x = group_df.get_group(symbol[0])

# 計算所有股票月漲跌幅
saved_list = list()
for each_stock in symbol:
    each_df = group_df.get_group(each_stock)
    # 計算月漲跌幅：(最後一筆收盤 - 第一筆開盤)/第一筆開盤 * 100
    mon_open = each_df.iloc[0][2]
    mon_close = each_df.iloc[-1][5]
    month_change = (mon_close - mon_open)/mon_open * 100
    # 月漲幅 > 20% 判斷式
    if month_change > threshold_rate:
        saved_list.append([each_stock, month_change, mon_open, mon_close])
    else:
        pass
# %%
change_df = pd.DataFrame(saved_list,
                         columns=['SYMBOL',
                                  'CHANGE',
                                  'OPEN',
                                  'CLOSE']).sort_values(by='CHANGE',
                                                        ascending=False)
change_df = change_df.reset_index(drop=True)
print(change_df)
# %%
# print(change_df.values)
# %%
# 敏感資訊 commit 前要刪掉
email_account = '********@gmail.com'
email_pwd = '********'
receivers = ['********@gmail.com', '********@gmail.com']
msg = MIMEMultipart('alternative')
if len(change_df.values) == 0:
    pass
else:
    # 為篩選股票建立表格
    table_create = list()
    for each_row in change_df.values:
        table_create.append('<tr><td>'+str(each_row[0])+'</td>' +
                            '<td>'+'%.3f' % each_row[1]+'</td>' +
                            '<td>'+str(each_row[2])+'</td>' +
                            '<td>'+str(each_row[3])+'</td></tr>')
    msg_html = f"""\
        <html>
            <body>
                <h2>【{month}月份漲幅 > {threshold_rate} % 股票清單】</h2>
                <h3> 本程式整理本月目前為止 漲幅 > {threshold_rate} % 的股票清單</h3>
                <table>
                    <tr>
                        <th>股票代號</th>
                        <th>漲跌幅</th>
                        <th>月開盤價</th>
                        <th>月收盤價</th>
                    </tr>
                    {''.join(table_create)}
                </table>
            </body>
        </html>
        """
    msg_table = MIMEText(msg_html, 'html')
    msg.attach(msg_table)
    # 信件標題、對象、寄送人
    msg['Subject'] = f'{month}月份漲幅 > {threshold_rate} % 股票清單--自動更新'
    msg['from'] = email_account
    msg['bcc'] = ','.join(receivers)
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.ehlo()
    server.login(email_account, email_pwd)
    server.send_message(msg)
    server.quit()
