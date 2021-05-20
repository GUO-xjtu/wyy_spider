# -*- coding: utf-8 -*-
# @Time    : 2020-01-29 13:32
# @Author  : GCY
# @FileName: music_csv.py
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
import pandas as pd
from Crypto.Cipher import AES
from utils.sql_save import MySQLCommand
from bs4 import BeautifulSoup


class MusicSpider(object):
    def __init__(self):
        self.file_path = '../data/'  # 用户信息保存位置
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'JSESSIONID-WYYY=ByCd%2F1zHaA6%5CBqA%2BY6sxOkSFXycajAx3XuQyySu2buAYehwzXeZkRb1wscB8vUIg83pUvkMHO1SmtGIO3pKyySb%5CoxUpy9CUWWEo0hjRRszV%2FkqPsH%2B5PykExoVq9zQCZuwyQz4tQqCrvotiqb%5CO%5CA8cpWAqAQraI5NsvM5VY5KenvqS%3A1578052539036; _iuqxldmzr_=32; _ntes_nnid=6773350955c533de38f1625624ebe4f4,1578050739108; _ntes_nuid=6773350955c533de38f1625624ebe4f4; WM_NI=3NHJAjwsUDaG8r2TMyn128jA6fBbyickbyK%2FnunpTznOsK4Xk5AhevMS3EvW6tQsbNoSelxCjgnNNqWFyUEP%2B1e8SaaQ51OcjIxmvagcdyPMlC%2B%2BTwteRAImrcPzeEINM0U%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed2d14a9596ae94f067a88e8ba2d14a929a9aaabb21bab2aba9c240b19bfdb7db2af0fea7c3b92af19288abc462b5ad9ba5e44dfcaefeb5d073aeeffed9e94bf6ba8e83fc63a1b5ae9aca25aeaba291d772ae91bdacb754a9eb8f89e87e8f8dfda6f55df6ac9f94e146ad8dab8dfb49aab9a2afcd7b959ab7b6c85ce9efabd9d26ba38ffbd2ce69aa97b88ef56ba5bdac9ad347b09de5ccd77db8bb9ea2cc67b2bda09be84f8b9283d1d837e2a3; WM_TID=mCNsKkYK71tBAQBBRFNtqjmPHS4pFjUG',
            'Host': 'music.163.com',
            'Referer': 'http://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
        self.data_list = pd.read_csv('../data/csv_file/music_spider.csv')
        self.num = 0  # 从第0首歌曲开始爬取
        self.music = False
        self.cookie_path = '../data/cookie.txt'

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

    def check_headers(self):
        cookie_list = []
        with open(self.cookie_path, 'r') as fp:
            for i in fp.readlines():
                i = json.loads(i)
                cookie_list.append(i)
        self.headers['Cookie'] = random.choice(cookie_list)['cookie']

    # 获取评论总数
    def page_spider(self, music_id):
        url = 'https://music.163.com/weapi/v1/resource/comments/R_SO_4_' + music_id + '?csrf_token='
        page = 1
        params, encSecKey = self.get_params(page, True)
        data = {'params': params, 'encSecKey': encSecKey}
        self.headers['Referer'] = 'https://music.163.com/song?id=%s' % music_id
        repeat = 0
        while repeat < 8:
            try:
                if repeat > 5:
                    self.check_headers()
                r = requests.post(url, headers=self.headers, data=data)
                time.sleep(repeat)
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

    def get_lynic(self, song_id):
        # params的长度为108,不要拿浏览器控制面板中的数据进行测试,那里的params长度为128,不符合
        params, encSecKey = self.get_params(song_id, False)
        data = {'params': params, 'encSecKey': encSecKey}
        url = 'https://music.163.com/weapi/song/lyric?csrf_token='
        repeat = 1
        while repeat < 16:
            try:
                if repeat > 8:
                    self.check_headers()
                r = requests.post(url, headers=self.headers, data=data)
                time.sleep(repeat)
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
        return result

    # 获取歌曲详情
    def get_music_info(self, music):
        music_dict = {}
        # lynic_result = self.get_lynic(music)
        lynic_result = {'song_lynic': '', 'lynic_user': ''}
        m_id = music
        music_dict['music_id'] = m_id
        simple_music = []
        contain_list = []
        url = 'https://music.163.com/song?id=%s' % m_id
        repeat = 0
        while repeat < 5:
            try:
                response = requests.get(url, headers=self.headers)
                time.sleep(repeat)
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
                    repeat += 1
            except Exception as e:
                print('第%d次获取ID为%s歌曲详情失败！原因是%s ' % (repeat, m_id, e))
                repeat += 1
            break

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
        music_dict['contain_list'] = json.dumps(contain_list)
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
        music_dict['simple_music'] = json.dumps(simple_music)
        music_dict['song_lynic'] = lynic_result['song_lynic']
        music_dict['lynic_user'] = lynic_result['lynic_user']
        return music_dict

    # 重连数据库
    def conn_music(self):
        self.mysqlMusic = MySQLCommand()
        self.mysqlMusic.connectdb()
        self.music = True

    # 保存歌曲信息
    def save_music(self, num, result):
        try:
            self.mysqlMusic.insert_music(result)
            print(result)
            print('--->第%d首歌曲爬取完成<---' % num)

        except:
            self.music = False
            print('数据库异常，重新连接数据库...')

    # 从csv文件下发任务
    def get_list_id(self):
        if self.music is False:
            self.conn_music()
            time.sleep(1)
        data = self.data_list['music_id']
        num = 0
        for task in data.values:
            task = str(task)
            print('爬取ID为 %s 的歌曲...' % task)
            if num >= self.num:
                result = self.get_music_info(task)
                while True:
                    if self.music is False:
                        self.conn_music()
                        time.sleep(1)
                    else:
                        self.save_music(num, result)
                        break
            num += 1


if __name__ == '__main__':
    MS = MusicSpider()
    MS.get_list_id()

