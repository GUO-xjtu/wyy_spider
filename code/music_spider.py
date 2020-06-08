# -*- coding: utf-8 -*-
# @Time    : 2019-11-13 17:00
# @Author  : GCY
# @FileName: music_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import re
import time
import requests
import math
import json
import codecs
import base64
import random
import pymysql
from threading import Thread
from Crypto.Cipher import AES
from utils.sql_save import MySQLCommand
from multiprocessing import Queue
from bs4 import BeautifulSoup


class MusicSpider(object):
    def __init__(self):
        self.file_path = '../data/'  # 用户信息保存位置
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': '_iuqxldmzr_=32; _ntes_nnid=0e6e1606eb78758c48c3fc823c6c57dd,1527314455632; '
                      '_ntes_nuid=0e6e1606eb78758c48c3fc823c6c57dd; __utmc=94650624; __utmz=94650624.1527314456.1.1.'
                      'utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); WM_TID=blBrSVohtue8%2B6VgDkxOkJ2G0VyAgyOY;'
                      ' JSESSIONID-WYYY=Du06y%5Csx0ddxxx8n6G6Dwk97Dhy2vuMzYDhQY8D%2BmW3vlbshKsMRxS%2BJYEnvCCh%5CKY'
                      'x2hJ5xhmAy8W%5CT%2BKqwjWnTDaOzhlQj19AuJwMttOIh5T%5C05uByqO%2FWM%2F1ZS9sqjslE2AC8YD7h7Tt0Shufi'
                      '2d077U9tlBepCx048eEImRkXDkr%3A1527321477141; __utma=94650624.1687343966.1527314456.1527314456'
                      '.1527319890.2; __utmb=94650624.3.10.1527319890',
            'Host': 'music.163.com',
            'Referer': 'http://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
        self.task_music_id = Queue()  # 歌曲信息任务队列
        self.lynic_task_id = Queue()  # 歌词任务队列
        self.lynic_result = Queue()  # 歌词结果队列
        self.save_sql = Queue()  # 存储sql队列

    @staticmethod
    def _generate_random_strs(length):
        string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        i = 0  # 控制次数参数i
        # 初始化随机字符串
        random_strs = ""
        while i < length:
            e = random.random() * len(string)
            # 向下取整
            e = math.floor(e)
            random_strs = random_strs + list(string)[e]
            i = i + 1
        return random_strs

    # AES加密
    @staticmethod
    def _aes_encrypt(msg, key):
        padding = 16 - len(msg) % 16  # 如果不是16的倍数则进行填充(padding)
        msg = msg + padding * chr(padding)  # 这里使用padding对应的单字符进行填充
        iv = '0102030405060708'  # 用来加密或者解密的初始向量(必须是16位)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        encrypted_bytes = cipher.encrypt(msg)  # 加密后得到的是bytes类型的数据
        encode_strs = base64.b64encode(encrypted_bytes)  # 使用Base64进行编码,返回byte字符串
        enc_text = encode_strs.decode('utf-8')  # 对byte字符串按utf-8进行解码
        return enc_text

    # RSA加密
    @staticmethod
    def _rsa_encrypt(random_strs, key, f):
        # 随机字符串逆序排列
        string = random_strs[::-1]
        # 将随机字符串转换成byte类型数据
        text = bytes(string, 'utf-8')
        seckey = int(codecs.encode(text, encoding='hex'), 16) ** int(key, 16) % int(f, 16)
        return format(seckey, 'x').zfill(256)

    # 获取参数
    def get_params(self, id_msg, comment):
        # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
        # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
        # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'
        # offset = (page-1) * 20
        # msg = '{offset":' + str(offset) + ',"limit":"20"}'
        # msg = '{"rid":"R_SO_4_1302938992","offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
        key = '0CoJUm6Qyw8W8jud'
        if comment:
            offset = (id_msg - 1) * 20
            # offset和limit是必选参数,其他参数是可选的,其他参数不影响data数据的生成
            msg = '{"offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
        else:
            msg = '{id: ' + id_msg + ', lv: -1, tv: -1}'
        f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
        e = '010001'
        enc_text = self._aes_encrypt(msg, key)
        # 生成长度为16的随机字符串
        i = self._generate_random_strs(16)

        # 两次AES加密之后得到params的值
        enc_text = self._aes_encrypt(enc_text, i)
        # RSA加密之后得到encSecKey的值
        enc_seckey = self._rsa_encrypt(i, e, f)
        return enc_text, enc_seckey

    # 数据正则处理
    def re_value(self, value):
        value = re.sub(r'\r|\n|\\|\'|\{|\}|\"', ' ', value)
        return value

    # 获取评论总数
    def page_spider(self, music_id):
        url = 'https://music.163.com/weapi/v1/resource/comments/R_SO_4_' + music_id + '?csrf_token='
        page = 1
        params, encSecKey = self.get_params(page, True)
        data = {'params': params, 'encSecKey': encSecKey}
        self.headers['Referer'] = 'https://music.163.com/song?id=%s' % music_id
        repeat = 0
        while repeat < 5:
            try:
                r = requests.post(url, headers=self.headers, data=data)
                time.sleep(repeat + 2)
                r.encoding = "utf-8"
                if r.status_code == 200:
                    # 返回json格式的数据
                    result = r.json()
                    if 'total' in result.keys():
                        total = result['total']
                        return total
                    else:
                        return 0
                else:
                    repeat += 1
            except Exception as e:
                print('ID为%s的歌曲评论总数获取失败, 原因是%s' % (music_id, e))
                repeat += 1

    def get_lynic(self):
        while True:
            song_id = self.lynic_task_id.get()
            # params的长度为108,不要拿浏览器控制面板中的数据进行测试,那里的params长度为128,不符合
            params, encSecKey = self.get_params(song_id, False)
            data = {'params': params, 'encSecKey': encSecKey}
            url = 'https://music.163.com/weapi/song/lyric?csrf_token='
            repeat = 1
            while repeat < 5:
                try:
                    r = requests.post(url, headers=self.headers, data=data)
                    time.sleep(repeat + 1)
                    song = r.json()
                    if 'uncollected' in song.keys() or 'lrc' in song.keys() or 'nolyric' in song.keys():
                        break
                    else:
                        if 'sgc' in song.keys():
                            if song['sgc']:
                                break
                        repeat += 1
                        print('第%d次获取ID为%s的歌曲歌词失败，请求太快！' % (repeat, song_id))
                except Exception as e:
                    print('第%d次获取ID为%s的歌曲歌词失败，原因%s' % (repeat, song_id, e))
                    repeat += 1
            try:
                song_lynic = song['lrc']['lyric']
                song_lynic = re.sub(r'\[(.*?)\]', '', song_lynic)
                song_lynic = re.sub(r'\n', ',', song_lynic)
                song_lynic = self.re_value(song_lynic)
            except Exception:
                # print(song_id)
                # print(song)
                song_lynic = ''
            try:
                id = song['lyricUser']['userid']
                uptime = song['lyricUser']['uptime']
                lynic_user = json.dumps({'user_id': id, 'uptime': uptime})
            except Exception:
                lynic_user = json.dumps({'user_id': '', 'uptime': ''})

            result = {'song_lynic': song_lynic, 'lynic_user': lynic_user}
            # print(result)
            self.lynic_result.put(result)

    # 获取歌曲详情
    def get_music_info(self):

        while True:
            music_dict = {}
            music = self.task_music_id.get()
            lynic_result = self.lynic_result.get()
            m_id = music
            music_dict['music_id'] = m_id
            simple_music = []
            contain_list = []
            url = 'https://music.163.com/song?id=%s' % m_id
            repeat = 1
            while repeat < 15:
                try:
                    response = requests.get(url, headers=self.headers)
                    time.sleep(repeat + 3)
                    response = response.text
                    soup = BeautifulSoup(response, 'html5lib')
                    try:
                        title = soup.find_all('div', attrs={'class': 'tit'})[0]
                        title = title.find('em').text
                        music_dict['music_name'] = title
                        break
                    except Exception as e:
                        music_dict['music_name'] = ''
                        print('未找到ID为%s歌曲的歌名，原因是%s' % (m_id, e))
                        repeat += 2
                except Exception as e:
                    print('第%d次次获取ID为%s歌曲详情失败！原因是%s ' % (repeat, m_id, e))
                    repeat += 1
                soup = None

            if soup is None:
                continue
            try:
                for index, info in enumerate(soup.find_all('p', attrs={'class': 'des s-fc4'})):
                    try:
                        singer_id = info.find_all('span')[0].find_all('a')[0]['href'].replace('/artist?id=', '').strip()
                        music_dict['singer_id'] = singer_id
                    except Exception:
                        try:
                            album_id = info.find_all('a')[0]['href'].replace('/album?id=', '').strip()
                            music_dict['album_id'] = album_id
                        except:
                            if index == 0:
                                music_dict['singer_id'] = ''
                            else:
                                music_dict['album_id'] = ''
            except Exception as e:
                music_dict['singer_id'] = ''
                music_dict['album_id'] = ''
                print('ID为%s的歌曲的歌手和专辑信息获取失败，使用默认空值！失败原因是%s' % (m_id, e))
            try:
                music_list = soup.find_all('ul', attrs={'class': 'm-rctlist f-cb'})[0]
                for info in music_list.find_all('li'):
                    try:
                        playlist = re.findall(r'playlist\?id=(.*?)" title', str(info))[0]
                        creator_id = re.findall(r'/user/home\?id=(.*?)" title', str(info))[0]
                        contain_list.append({'list': playlist, 'creator': creator_id})
                    except:
                        print('歌单ID和创建者ID爬取异常！此信息为：', str(info))
            except Exception as e:
                print('获取包含此歌的歌单信息失败！失败歌曲为: %s, 原因是%s' % (m_id, e))
            music_dict['contain_list'] = json.dumps({'contain_list': contain_list})
            try:
                simple_m = soup.find_all('ul', attrs={'class': 'm-sglist f-cb'})[0]
                for music in simple_m.find_all('li', attrs={'class': 'f-cb'}):
                    try:
                        song_id = re.findall(r'/song\?id=(.*?)" title', str(music))[0]
                        try:
                            singer_id = re.findall(r'/artist\?id=(.*?)">', str(music))[0]
                        except:
                            try:
                                singer_id = re.findall(r'title="(.*?)"><span', str(music))[0]
                            except:
                                singer_id = ''
                        simple_music.append({'song': song_id, 'singer': singer_id})
                    except:
                        print('歌曲ID和歌手ID爬取异常！此信息为：', str(music))
            except Exception as e:
                print('获取于此歌相似的歌曲失败！失败歌曲为: %s, 原因是%s' % (m_id, e))
            comment_num = self.page_spider(m_id)
            music_dict['comment_num'] = str(comment_num)
            music_dict['simple_music'] = json.dumps({'simple_music': simple_music})
            music_dict['song_lynic'] = lynic_result['song_lynic']
            music_dict['lynic_user'] = lynic_result['lynic_user']
            self.save_sql.put(music_dict)

    # 获取歌单中的歌曲ID
    def get_music_id(self, list_id):
        music_list = []
        spider_num = 0
        url = 'https://music.163.com/playlist?id=%s' % list_id
        while spider_num < 3:
            try:
                response = requests.get(url, headers=self.headers)
                time.sleep(spider_num + 5)
                response = response.text
                break
            except Exception as e:
                spider_num += 1
                print('第%d次爬取歌曲id失败，原因是%s，正在进行重试...' % (spider_num, e))
            response = None
        if response:
            soup = BeautifulSoup(response, 'html5lib')
            try:
                music_infos = soup.find_all('ul', attrs={'class': 'f-hide'})[0]
                music_infos = music_infos.find_all('li')
            except Exception as e:
                print('爬取歌单ID为：%s 失败，原因是%s, 重新请求！' % (list_id, e))
                try:
                    response = requests.get(url, headers=self.headers)
                    time.sleep(8)
                    response = response.text
                    soup = BeautifulSoup(response, 'html5lib')
                    music_infos = soup.find_all('ul', attrs={'class': 'f-hide'})[0]
                    music_infos = music_infos.find_all('li')
                except Exception as e:
                    print('尝试重新请求ID为: %s的歌单失败，原因是 %s' % (list_id, e))
                    return
            for music_info in music_infos:
                music_info = music_info.find_all('a')[0]
                # print(music_info)
                music_list.append(str(music_info))
            for info in music_list:
                try:
                    music_id = re.findall(r'/song\?id=(.*?)">', info)[0]
                    self.task_music_id.put(music_id)
                    self.lynic_task_id.put(music_id)
                except:
                    print('获取此歌单中的一首歌曲的ID失败，歌单为：%s' % info)
            while not self.task_music_id.empty() and not self.lynic_task_id.empty():
                time.sleep(1)

    # 保存歌曲信息
    def save_music(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.save_sql.get()
            mysql_command.insert_music(result)

    # 连接wyy_spider数据库
    def conn_data(self):
        while True:
            print('连接到mysql服务器...')
            try:
                conn = pymysql.connect(
                    host='localhost',
                    user='root',
                    passwd='0321',
                    port=3306,
                    db='wyy_spider',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                cursor = conn.cursor()
                print('wyy_spider连接上了!')
                return conn, cursor
            except:
                print('wyy_spider连接失败！')
                time.sleep(2)

    # 从数据库下发任务
    def get_list_id(self):
        conn, cursor = self.conn_data()
        cursor.execute("select id, musicId from music_list")
        music_list = cursor.fetchall()
        for list in music_list:
            list_id = list['id'].strip()
            music_ids = list['musicId']
            if music_ids is None:
                music_ids = ''
            print('开始爬取ID为 %s 的歌单...' % list_id)
            if len(music_ids) == 0:
                self.get_music_id(list_id)
            else:
                music_ids = music_ids.split(',')[:-1]
                for i in music_ids:
                    self.task_music_id.put(i)
                    self.lynic_task_id.put(i)
                    time.sleep(3)

    def main_spider(self):
        Thread(target=self.get_music_info, args=()).start()
        Thread(target=self.get_lynic, args=()).start()
        Thread(target=self.save_music, args=()).start()
        self.get_list_id()


if __name__ == '__main__':

    MS = MusicSpider()
    MS.main_spider()

