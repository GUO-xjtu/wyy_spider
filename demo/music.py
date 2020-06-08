# -*- coding: utf-8 -*-
# @Time     : 2020-03-19 09:28
# @Author   : GCY
# @FileName : music.py
# @SoftWare : PyCharm
# @Blog     : https://github.com/GUO-xjtu

from sql_save import MySQLCommand
import multiprocessing as mp
import csv


class DataToCSV():
    def __init__(self):
        self.music = False

    # 重连数据库
    def conn_music(self):
        self.mysqlMusic = MySQLCommand()
        self.mysqlMusic.connectdb()
        self.music = True

    def sql_music(self):
        if self.music is False:
            self.conn_music()
            self.music = True
        self.mysqlMusic.cursor.execute("select * from music")
        music_ids = self.mysqlMusic.cursor.fetchall()
        print("数据库中有歌曲：%d首" % len(music_ids))
        num = 0
        with open("music.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(['music_id', 'music_name', 'singer_id', 'album_id', 'contain_list', 'simple_music', 'song_lynic', 'lynic_user',
                     'comment_num'])
            # 写入多行用writerows
            for id in music_ids:
                num += 1
                if num % 1000 == 0:
                    print("正在保存第%d首歌曲的信息......" % num)
                music_id = id.get('music_id')
                singer_id = id.get('singer_id')
                music_name = id.get('music_name')
                album_id = id.get('album_id')
                contain_list = id.get('contain_list')
                simple_music = id.get('simple_music')
                song_lynic = id.get('song_lynic')
                lynic_user = id.get('lynic_user')
                comment_num = id.get('comment_num')
                writer.writerow(
                    [music_id, music_name, singer_id, album_id, contain_list, simple_music, song_lynic, lynic_user,
                     comment_num])


if __name__ == '__main__':
    DTC = DataToCSV()
    DTC.sql_music()
