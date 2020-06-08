# -*- coding: utf-8 -*-
# @Time    : 2019-11-12 13:23
# @Author  : GCY
# @FileName: all_config.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu

music_list_key = ['userId', 'name', 'id', 'createTime', 'updateTime', 'description', 'trackCount', 'playCount',
                  'authority', 'specialType', 'expertTags', 'tags', 'subscribedCount', 'cloudTrackCount',
                  'trackUpdateTime', 'trackNumberUpdateTime', 'highQuality']
music_list_csv = ('id', 'name', 'userId', 'createTime', 'updateTime', 'description', 'specialType', 'expertTags',
                  'tags', 'authority', 'trackCount', 'playCount', 'subscribedCount', 'cloudTrackCount',
                  'trackUpdateTime', 'trackNumberUpdateTime', 'highQuality')

fans_list_key = ['eventCount', 'expertTags', 'experts', 'followeds', 'follows', 'gender', 'nickname', 'playlistCount',
                'signature', 'time', 'userId', 'userType', 'vipType']

fans_list_csv = ('userId', 'nickname', 'gender', 'userType', 'vipType', 'followeds', 'follows', 'playlistCount',
                 'eventCount', 'expertTags', 'experts', 'signature', 'time')

music_user_key = ['city', 'province', 'gender', 'birthday', 'detailDescription', 'description', 'userId', 'expertTags',
                  'experts', 'nickname', 'signature', 'userType', 'vipType']

music_csv = ('music_id', 'music_name', 'singer_id', 'album_id', 'contain_list', 'simple_music', 'song_lynic', 'lynic_user')

music_key = ['music_id', 'music_name', 'singer_id', 'album_id', 'contain_list', 'simple_music', 'song_lynic', 'lynic_user']

mysql_port = 3306
mysql_host = 'localhost'
mysql_user = 'root'  # 用户名
mysql_password = '0321'
mysql_db = "wyy_spider"  # 存储库
