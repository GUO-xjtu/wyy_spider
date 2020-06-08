# -*- coding: utf-8 -*-
# @Time    : 2019-12-21 16:21
# @Author  : GCY
# @FileName: dateTocsv.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
from utils.sql_save import MySQLCommand
import multiprocessing as mp
from threading import Thread
import random
import csv


class DataToCSV():
    def __init__(self):
        self.music_queue = mp.Queue()
        self.user_queue = mp.Queue()
        self.list_queue = mp.Queue()
        self.singer_queue = mp.Queue()
        self.comment_queue = mp.Queue()
        self.music = False
        self.user = False
        self.list = False
        self.comm = False
        self.singer = False
        self.list_comm = False

    # 重连数据库
    def conn_music(self):
        self.mysqlMusic = MySQLCommand()
        self.mysqlMusic.connectdb()
        self.music = True

    def conn_list(self):
        self.mysqlList = MySQLCommand()
        self.mysqlList.connectdb()
        self.list = True

    def conn_user(self):
        self.mysqlUser = MySQLCommand()
        self.mysqlUser.connectdb()
        self.user = True

    def conn_comm(self):
        self.mysqlComment = MySQLCommand()
        self.mysqlComment.connectdb()
        self.comm = True

    def conn_list_comm(self):
        self.mysqlLcomm = MySQLCommand()
        self.mysqlLcomm.connectdb()
        self.list_comm = True

    def conn_singer(self):
        self.mysqlSinger = MySQLCommand()
        self.mysqlSinger.connectdb()
        self.singer = True

    def sql_music(self):
        while True:
            if self.music is False:
                self.conn_music()
                self.music = True
            try:
                self.mysqlMusic.cursor.execute("select * from music")
                music_ids = self.mysqlMusic.cursor.fetchall()
                break
            except:
                self.music = False
        with open("music.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(["歌曲ID", "歌名", "歌手ID", "专辑ID", "包含这首歌的歌单ID", "相似歌曲ID", "歌词", "歌词贡献者ID", "评论数"])
            # 写入多行用writerows
            for id in music_ids:

                music_id = id.get('music_id')
                singer_id = id.get('singer_id')
                music_name = id.get('music_name')
                album_id = id.get('album_id')
                contain_list = id.get('contain_list')
                simple_music = id.get('simple_music')
                song_lynic = id.get('song_lynic')
                lynic_user = id.get('lynic_user')
                if song_lynic == '' or song_lynic is None:
                    if random.randint(1, 10) >= 3:
                        continue
                comment_num = id.get('comment_num')
                writer.writerow([music_id, music_name, singer_id, album_id, contain_list, simple_music, song_lynic, lynic_user, comment_num])

    def sql_list(self):
        if self.list is False:
            self.conn_list()
            self.list = True
        self.mysqlList.cursor.execute("select * from music_list limit 300000, 500000")
        list_ids = self.mysqlList.cursor.fetchall()
        with open("music_list.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(["歌单ID", "歌单名", "歌单创建者ID", "歌单标签", "专家标签", "歌单创建时间", "歌单更新时间", "权威性",
                             "包含歌曲数", "歌单播放量", "特殊类别", "歌单订阅数", "云端曲目数", "曲目数更新时间", "曲目更新时间", "是否为高质量", "歌单描述信息", "喜欢此歌单的人", "包含的歌曲", "热门歌单"])
            # 写入多行用writerows
            for id in list_ids:
                if id is None:
                    continue
                list_id = id.get('id')
                musicId = id.get('musicId')
                if musicId is None:
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
                subscribedCount = id.get('subscribedCount')
                cloudTrackCount = id.get('cloudTrackCount')
                trackUpdateTime = id.get('trackUpdateTime')
                trackNumberUpdateTime = id.get('trackNumverUpdateTime')
                highQuality = id.get('highQuality')
                userLikeId = id.get('userLikeId')
                hotlist = id.get('hotlist')
                writer.writerow([list_id, list_name, user_id, tags, expertTags, createTime, updateTime, authority,
                                     trackCount, playCount, specialType, subscribedCount, cloudTrackCount,
                                     trackNumberUpdateTime, trackUpdateTime, highQuality, description, userLikeId, musicId, hotlist])

    def sql_singer(self):
        if self.singer is False:
            self.conn_singer()
            self.singer = True
        self.mysqlSinger.cursor.execute("select * from singer")
        singer_ids = self.mysqlSinger.cursor.fetchall()
        with open("singer.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(["歌手ID", "歌手名", "歌手主页ID", "歌手主页中的歌曲"])
            # 写入多行用writerows
            for id in singer_ids:
                artist_id = id.get('artist_id')
                artist_name = id.get('artist_name')
                homepage_id = id.get('homepage_id')
                top50 = id.get('top50_song_dict')
                if top50 == '' or top50 is None:
                    if random.randint(1, 10) >= 3:
                        continue
                writer.writerow([artist_id, artist_name, homepage_id, top50])

    def sql_user(self):
        if self.user is False:
            self.conn_user()
            self.user = True
        self.mysqlUser.cursor.execute("select * from user limit 0, 600000")
        user_ids = self.mysqlUser.cursor.fetchall()
        with open("user.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(['用户ID', '用户昵称', '性别', '省份', '城市', '生日', '描述信息', '详细描述信息', '专家标签', '个性签名',
                             '用户类型', 'vip类型', '关注量', '粉丝量', '动态量', '创建的歌单数', '用户创建时间', '所有歌单ID', '本周听过', '以前听过', '听过的歌曲数'])
            # 写入多行用writerows
            for id in user_ids:
                user_id = id.get('userId')
                nickname = id.get('nickname')
                province = id.get('province')
                city = id.get('city')
                birthday = id.get('birthday')
                detailDescription = id.get('detailDescription')
                description = id.get('description')
                expertTags = id.get('expertTags')
                signature = id.get('signature')
                userType = id.get('userType')
                vipType = id.get('vipType')
                list_id = id.get('list_id')
                eventCount = id.get('eventCount')
                followeds = id.get('followeds')
                follows = id.get('follows')
                gender = id.get('gender')
                playlistCount = id.get('playlistCount')
                time = id.get('time')
                week_music = id.get('week_music')
                all_music = id.get('all_music')
                listen_num = id.get('listen_num')
                if province == '' or province is None:
                    if random.randint(1, 10) >= 3:
                        continue
                if week_music == '' or week_music is None:
                    if random.randint(1, 10) >= 9:
                        continue
                writer.writerow([user_id, nickname, gender, province, city, birthday, description, detailDescription, expertTags,
                                       signature, userType, vipType, follows, followeds, eventCount, playlistCount, time, list_id, week_music, all_music, listen_num])

    def sql_comments(self):
        if self.comm is False:
            self.conn_comm()
            self.comm = True
        self.mysqlComment.cursor.execute("select * from comments limit 100000, 600000")
        comment_ids = self.mysqlComment.cursor.fetchall()
        with open("comment.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # 先写入columns_name
            writer.writerow(['歌曲ID', '用户ID', '歌手ID', '评论时间', '是否为热门评论', '点赞量', '评论', '回复'])
            # 写入多行用writerows
            for id in comment_ids:

                music_id = id.get('music_id')
                user_id = id.get('user_id')
                hot_comment = id.get('hot_comment')
                comment = id.get('comment')
                likedCount = id.get('likedCount')
                time = id.get('time')
                singer_id = id.get('singer_id')
                reply = id.get('reply')
                writer.writerow([music_id, user_id, singer_id, time, hot_comment, likedCount, comment, reply])

    def sql_list_comments(self):
        if self.list_comm is False:
            self.conn_list_comm()
            self.list_comm = True
        self.mysqlLcomm.cursor.execute("select * from list_comment limit 0, 600000")
        comment_ids = self.mysqlLcomm.cursor.fetchall()
        with open("list_comment.csv", 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['歌单ID', '创建者ID', '评论者ID', '评论时间', '是否为热门评论', '点赞量', '评论', '回复'])
            for id in comment_ids:
                list_id = id.get('list_id')
                user_id = id.get('user_id')
                hot_comment = id.get('hot_comment')
                likedCount = id.get('likedCount')
                time = id.get('time')
                comment = id.get('comment')
                creater_id = id.get('creater_id')
                reply = id.get('reply')
                writer.writerow([list_id, creater_id, user_id, time, hot_comment, likedCount, comment, reply])

    def execute_main(self):
        Thread(target=self.sql_singer, args=()).start()
        Thread(target=self.sql_comments, args=()).start()
        Thread(target=self.sql_list, args=()).start()
        Thread(target=self.sql_music, args=()).start()
        Thread(target=self.sql_user, args=()).start()
        Thread(target=self.sql_list_comments, args=()).start()
        

if __name__ == '__main__':
    DTC = DataToCSV()
    DTC.execute_main()