# -*- coding: utf-8 -*-
# @Time    : 2020-01-08 10:01
# @Author  : GCY
# @FileName: sql_demo.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import pymysql
import utils.all_config as config

def conn_data():
    print('连接到mysql服务器...')
    try:
        conn = pymysql.connect(
            host=config.mysql_host,
            user=config.mysql_user,
            passwd=config.mysql_password,
            port=config.mysql_port,
            db='wyy_spider',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        print('连接上了!')
        return conn, cursor
    except:
        print('连接失败！')


conn, cursor = conn_data()

cursor.execute("select id, musicId from music_list")
music_list = cursor.fetchall()
for i in music_list:
    print(i)
