# -*- coding: utf-8 -*-
# @Time     : 2020-03-18 23:32
# @Author   : GCY
# @FileName : music_list.py
# @SoftWare : PyCharm
# @Blog     : https://github.com/GUO-xjtu

from sql_save import MySQLCommand
import multiprocessing as mp
import csv


class DataToCSV():
    def __init__(self):
        self.list_queue = mp.Queue()
        self.list = False

    # 重连数据库
    def conn_list(self):
        self.mysqlList = MySQLCommand()
        self.mysqlList.connectdb()
        self.list = True

    def sql_list(self):
        if self.list is False:
            self.conn_list()
            self.list = True
        self.mysqlList.cursor.execute("select * from music_list")
        list_ids = self.mysqlList.cursor.fetchall()
        print("数据库中有歌单：%d个" % len(list_ids))
        num = 0
        with open("music_list.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(['list_id', 'list_name', 'user_id', 'tags', 'expertTags', 'createTime', 'updateTime', 'authority',
                                 'trackCount', 'playCount', 'specialType', 'subscribedCount', 'cloudTrackCount',
                                 'trackNumberUpdateTime', 'trackUpdateTime', 'highQuality', 'description', 'userLikeId', 'musicId',
                                 'hotlist'])
            # 写入多行用writerows
            for id in list_ids:
                num += 1
                if num % 1000 == 0:
                    print("正在分析第%d个歌单的信息......" % num)
                if id is None:
                    continue
                list_id = id.get('id')
                musicId = id.get('musicId')
                if musicId is None:
                    continue
                le_mu = len(musicId)
                if 50 > le_mu:
                    continue
                list_name = id.get('name')
                user_id = id.get('userId')
                createTime = id.get('createTime')
                updateTime = id.get('updateTime')
                description = id.get('description')
                trackCount = id.get('trackCount')
                authority = id.get('authority')
                playCount = id.get('playCount')
                specialType = id.get('specialType')
                expertTags = id.get('expertTags')
                tags = id.get('tags')

                if tags is None or len(tags) < 3:
                    continue
                subscribedCount = id.get('subscribedCount')
                cloudTrackCount = id.get('cloudTrackCount')
                trackUpdateTime = id.get('trackUpdateTime')
                trackNumberUpdateTime = id.get('trackNumverUpdateTime')
                highQuality = id.get('highQuality')
                userLikeId = id.get('userLikeId')
                hotlist = id.get('hotlist')
                writer.writerow([list_id, list_name, user_id, tags, expertTags, createTime, updateTime, authority,
                                 trackCount, playCount, specialType, subscribedCount, cloudTrackCount,
                                 trackNumberUpdateTime, trackUpdateTime, highQuality, description, userLikeId, musicId,
                                 hotlist])


if __name__ == '__main__':
    DTC = DataToCSV()
    DTC.sql_list()
