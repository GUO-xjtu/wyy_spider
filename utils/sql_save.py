# -*- coding: utf-8 -*-
# @Time    : 2019-11-10 15:59
# @Author  : GCY
# @FileName: sql_save.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import pymysql
import utils.all_config as config


class MySQLCommand(object):
    # 初始化类
    def __init__(self):
        self.host = config.mysql_host
        self.port = config.mysql_port  # 端口号
        self.user = config.mysql_user  # 用户名
        self.password = config.mysql_password
        self.db = config.mysql_db  # 存储库
        self.unum = 0
        self.mnum = 0
        self.pnum = 0
        self.snum = 0
        self.lnum = 0

    # 连接数据库
    def connectdb(self):
        print('连接到mysql服务器...')
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                port=self.port,
                db=self.db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
            print('连接上了!')
        except:
            print('连接失败！')

    # 更新歌单信息
    def update_list(self, music_list):
        cols = music_list.keys()
        values = music_list.values()
        if 'id' in cols:
            list_id = music_list['id']
        elif 'list_id' in cols:
            list_id = music_list['list_id']
        else:
            print('更新歌单信息失败，数据错误，此数据为%r' % music_list)
            return
        succeed = False
        for col, val in zip(cols, values):
            if col != 'list_id':
                try:
                    sql = ("update music_list set %s = '%s' where id='%s'" % (col, val, list_id))
                    self.cursor.execute(sql)
                    self.conn.commit()
                    succeed = True
                    print('更新歌单%s的%s成功' % (list_id, col))
                    # 判断是否执行成功
                except pymysql.Error as e:
                    # 发生错误回滚
                    # self.conn.rollback()
                    succeed = False
                    print("id为 %s 的歌单数据更新失败，原因 %s，数据为%r" % (list_id, e, music_list))
        if succeed:
            print('更新歌单%s成功' % list_id)

    # 插入歌单信息
    def insert_list(self, music_lists):
        # 插入数据
        try:
            cols = ', '.join(music_lists.keys())
            values = '"," '.join(music_lists.values())
            sql = "INSERT INTO music_list (%s) VALUES (%s)" % (cols, '"' + values + '"')

            try:
                self.cursor.execute(sql)
                self.conn.commit()
                self.mnum += 1
                try:
                    print("id为 %s 的歌单数据插入成功，第%d个歌单" % (music_lists['id'], self.mnum))
                except:
                    print("id为 %s 的歌单数据插入成功，第%d个歌单" % (music_lists['list_id'], self.mnum))

            except pymysql.Error as e:
                # 发生错误回滚
                # self.conn.rollback()
                print(e)
                self.mnum += 1
                self.update_list(music_lists)
        except pymysql.Error as e:
            print("歌单数据库错误，原因 %s, 数据为 %r" % (e, music_lists))

    # 更新用户数据
    def update_user(self, users):
        cols = users.keys()
        values = users.values()
        user_id = users['userId']
        succeed = False
        for col, val in zip(cols, values):
            if col != 'userId':
                try:
                    sql = ("update user set %s ='%s' where userId='%s'" % (col, val, user_id))
                    self.cursor.execute(sql)
                    self.conn.commit()
                    succeed = True
                    # 判断是否执行成功
                except pymysql.Error as e:
                    # 发生错误回滚
                    # self.conn.rollback()
                    succeed = False
                    print("id为 %s 的用户数据更新失败，原因 %s" % (user_id, e))
        if succeed:
            print("id为 %s 的用户数据更新成功" % user_id)

    # 插入用户数据， 插入之前先查询是否存在，若存在，就不再插入
    def insert_user(self, users):
        # 插入数据
        try:
            cols = ', '.join(users.keys())
            values = '"," '.join(users.values())
            sql = "INSERT INTO user (%s) VALUES (%s)" % (cols, '"' + values + '"')
            try:
                self.cursor.execute(sql)
                self.conn.commit()
                # 判断是否执行成功
                self.unum += 1
                print('id为%s的用户信息插入成功，第%d位用户' % (users['userId'], self.unum))
            except pymysql.Error:
                self.update_user(users)
        except pymysql.Error as e:
            print("用户数据库错误，原因 %s, 数据为：%r" % (e, users))

    # 插入评论用户数据， 插入之前先查询是否存在，若存在，就不再插入
    def insert_co_user(self, users):
        # 插入数据
        try:
            sql = "INSERT INTO comment_user (user_id) VALUES (%s)" % users
            try:
                self.cursor.execute(sql)
                self.conn.commit()
            except pymysql.Error:
                pass
        except pymysql.Error as e:
            print("用户数据库错误，原因 %s, 数据为：%r" % (e, users))

    # 插入评论数据
    def insert_comments(self, message):

        try:
            cols = ', '.join(message.keys())
            values = '"," '.join(message.values())
            sql = "INSERT INTO comments(%s) VALUES (%s)" % (cols, '"'+values+'"')

            result = self.cursor.execute(sql)
            self.conn.commit()
            # 判断是否执行成功
            if result:
                self.pnum += 1
                print("ID为%s的歌曲的第%d条评论插入成功" % (message['music_id'], self.pnum))
            else:
                print("ID为%s的歌曲的第%d条评论插入失败" % (message['music_id'], self.pnum))
                self.pnum += 1
        except pymysql.Error as e:
            # 发生错误回滚
            self.conn.rollback()
            print("第%d条评论插入失败，原因 %d:%s" % (self.pnum, e.args[0], e.args[1]))

    # 插入歌单评论数据
    def insert_list_comm(self, message):
        try:
            cols = ', '.join(message.keys())
            values = '"," '.join(message.values())
            sql = "INSERT INTO list_comment(%s) VALUES (%s)" % (cols, '"'+values+'"')

            result = self.cursor.execute(sql)
            self.conn.commit()
            # 判断是否执行成功
            if result:
                self.lnum += 1
                print("ID为%s的歌单的第%d条评论插入成功" % (message['list_id'], self.lnum))
            else:
                print("ID为%s的歌单的第%d条评论插入失败" % (message['list_id'], self.lnum))
                self.lnum += 1
        except pymysql.Error as e:
            # 发生错误回滚
            self.conn.rollback()
            print("第%d条评论插入失败，原因 %d:%s" % (self.lnum, e.args[0], e.args[1]))

    # 更新歌曲数据
    def update_music(self, music):
        cols = music.keys()
        values = music.values()
        id = music['music_id']
        for col, val in zip(cols, values):
            if col != 'music_id':
                try:
                    sql = ("update music set %s = '%s' where music_id='%s'" % (col, val, id))
                    self.cursor.execute(sql)
                    self.conn.commit()
                    print('更新成功，ID%s, 更新内容为%s' % (id, col))
                    # 判断是否执行成功
                except pymysql.Error as e:
                    # 发生错误回滚
                    # self.conn.rollback()
                    print("id为 %s 的歌曲数据更新失败，原因 %s" % (id, e))

    # 插入音乐数据
    def insert_music(self, music):
        id = music['music_id']
        try:
            sql = "INSERT INTO music (%s) VALUES (%s)" % ('music_id', id)
            try:
                self.cursor.execute(sql)
                self.conn.commit()
                # 判断是否执行成功
                self.mnum += 1
                print('id为%s的歌曲信息插入成功，第%d首歌曲' % (music['music_id'], self.mnum))
                self.update_music(music)
            except Exception as e:
                print(e)
                self.update_music(music)
        except Exception as e:
            print('插入ID为%s的歌曲信息失败，原因是%s. 歌曲内容为：%r' % (music['music_id'], e, music))

    def update_singer(self, artist_id, artist_name, homepage_id, top50_song_dict):
        self.snum += 1
        cols = ['artist_id', 'artist_name', 'homepage_id', 'top50_song_dict']
        values = [artist_id, artist_name, homepage_id, top50_song_dict]

        for col, val in zip(cols, values):
            if col != 'artist_id' and col != 'artist_name' and val != '':
                sql = ("update singer set {}={} where artist_id={}".format(col, val, artist_id))
                try:
                    self.cursor.execute(sql)
                    self.conn.commit()
                    print('更新歌手成功，ID-->%s, 更新内容为%s' % (artist_id, col))
                    # 判断是否执行成功
                except pymysql.Error as e:
                    # 发生错误回滚
                    print("id为 %s 的歌手数据更新失败，原因 %s" % (artist_id, e))
                    print(val)
                    print('错误：', sql)

    def insert_singer(self, artist_id, artist_name, homepage_id, top50_song_dict):
        self.snum += 1
        sql = "INSERT INTO singer(artist_id, artist_name, homepage_id, top50_song_dict) VALUES (%s, %s, %s, %s)"
        try:
            result = self.cursor.execute(sql, (artist_id, artist_name, homepage_id, top50_song_dict))
            if result:
                self.conn.commit()
                print('第%d位歌手信息插入成功, 歌手ID为%s' % (self.snum, artist_id))
        except:
            self.update_singer(artist_id, artist_name, homepage_id, top50_song_dict)

    def closeMysql(self):
        self.cursor.close()
        self.conn.close()  # 创建数据库操作类的实例
