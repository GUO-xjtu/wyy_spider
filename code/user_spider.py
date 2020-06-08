# -*- coding: utf-8 -*-
# @Time    : 2019-11-10 22:06
# @Author  : GCY
# @FileName: user_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import requests
import re
import time
import math
import codecs
import base64
import random
from threading import Thread
from Crypto.Cipher import AES
from bs4 import BeautifulSoup
from utils.sql_save import MySQLCommand
import multiprocessing as mp
import utils.all_config as config


class UserSpider(object):
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
        self.list_queue = mp.Queue()  # 歌单队列
        self.user_queue = mp.Queue()  # 用户队列
        self.task_follow_spider = mp.Queue()  # 粉丝爬取任务队列
        self.task_follows_spider = mp.Queue()  # 关注的人爬取任务队列
        self.task_list_spider = mp.Queue()  # 音乐列表爬取任务队列

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
    def get_params(self, id_msg):
        # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
        # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
        # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'
        # offset = (page-1) * 20
        # msg = '{offset":' + str(offset) + ',"limit":"20"}'
        # msg = '{"rid":"R_SO_4_1302938992","offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
        key = '0CoJUm6Qyw8W8jud'
        f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
        e = '010001'
        enc_text = self._aes_encrypt(id_msg, key)
        # 生成长度为16的随机字符串
        i = self._generate_random_strs(16)

        # 两次AES加密之后得到params的值
        enc_text = self._aes_encrypt(enc_text, i)
        # RSA加密之后得到encSecKey的值
        enc_seckey = self._rsa_encrypt(i, e, f)
        return enc_text, enc_seckey

    # 获取粉丝页的json数据
    def get_fans_json(self, url, data):
        repeat = 1
        while repeat < 5:
            try:
                r = requests.post(url, headers=self.headers, data=data)
                time.sleep(repeat)
                r.encoding = "utf-8"
                if r.status_code == 200:
                    # 返回json格式的数据
                    r = r.json()
                    print(r)
                    if 'follow' in r.keys():
                        if len(r['follow']) == 0:
                            print('抓取到的关注页为空，尝试重新抓取....')
                            repeat += 1
                        else:
                            return r
                    if 'followeds' in r.keys():
                        if len(r['followeds']) == 0:
                            print('抓取到的粉丝页为空，尝试重新抓取....')
                            repeat += 1
                        else:
                            return r
                    if 'playlist' in r.keys():
                        return r
            except Exception as e:
                print("第%d次获取url为%s的粉丝页信息失败，原因是%s" % (repeat, url, e))
                repeat += 1
        return None

    # 数据正则处理
    def re_value(self, value):
        value = re.sub(r'\r|\n|\\|\'|\{|\}|\"', ' ', value)
        return value

    # 歌单用户结果处理
    def user_music_list(self, user_info, list_id):
        result = {}
        for key in config.music_user_key:
            try:
                value = str(user_info[key])
                value = self.re_value(value)
                result[key] = value
            except:
                result.setdefault(key, '')
        result.setdefault('list_id', str(list_id))
        self.user_queue.put(result)
        return result

    # 粉丝用户结果处理
    def user_fans(self, user_info):
        result = {}
        for key in config.fans_list_key:
            try:
                value = str(user_info[key])
                value = self.re_value(value)
                result[key] = value
            except:
                result.setdefault(key, '')
        self.user_queue.put(result)
        # print('user: ', result)
        return result

    # 歌单结果处理
    def music_list(self, user_all_info):
        result = {}
        for key in config.music_list_key:
            try:
                value = str(user_all_info[key])
                value = self.re_value(value)
                result[key] = value
            except:
                result.setdefault(key, '')
        # print('music_list:', result)
        self.list_queue.put(result)
        return result

    # 对抓取数据进行处理
    def handle_info(self, user_json, uid):
        try:
            user_all_info = user_json['playlist']
            if len(user_all_info) > 0:
                music_list = []
                list_id = []
                result = {}
                user_info = user_all_info[0]['creator']

                for i in range(len(user_all_info)):
                    print('user_all_info: ', user_all_info[i])
                    lists = self.music_list(user_all_info[i])
                    music_list.append(lists)
                    list_id.append(user_all_info[i]['id'])
                if len(user_info) > 0:
                    result = self.user_music_list(user_info, list_id)
                result['list_id'] = str(list_id)
                return [result, music_list]
            else:
                print("id为{}用户没有创建歌单".format(uid))
                result = {}
                return [result, []]
        except:
            result = {}
            return [result, []]

    # 获取歌单信息
    def get_fans_info(self, task_id):
        print('爬取用户为%s的歌单任务...' % task_id)
        # 粉丝数据
        uri = 'https://music.163.com/weapi/user/playlist?csrf_token=cdee144903c5a32e6752f50180329fc9'
        # uid为粉丝id
        id_msg = '{uid: "' + str(task_id) + '", wordwrap: "7", offset: "0", ' \
                                        'total: "true", limit: "36", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
        params, encSecKey = self.get_params(id_msg)
        data = {'params': params, 'encSecKey': encSecKey}
        user_json = self.get_fans_json(uri, data)
        if user_json is not None:
            self.handle_info(user_json, task_id)

    # 获得关注的人信息
    def get_follow_info(self, task):
        print('关注的人爬取任务...', task)
        url = 'https://music.163.com/weapi/user/getfollows/%s?csrf_token=cdee144903c5a32e6752f50180329fc9' % task[0]
        page = 1
        pages = task[1] / 20
        while page:
            print('正在抓取第%d页, %s用户关注的人...' % (page, task[0]))
            offset = (page - 1) * 20
            id_msg = '{uid: "' + task[0] + '", wordwrap: "7", offset: ' + str(offset) + \
                     ', total: "true", limit: "36", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
            enc_text, enc_seckey = self.get_params(id_msg)
            data = {'params': enc_text, 'encSecKey': enc_seckey}
            user_json = self.get_fans_json(url, data)
            if user_json is None:
                break
            try:
                follows = user_json['follow']
                for follow in follows:
                    self.user_fans(follow)
                page += 1
            except Exception as e:
                print(user_json)
                print(e)
                page += 1
            if page > pages:
                return

    # 获取粉丝的人信息
    def get_follows_info(self, task):
        print('粉丝爬取任务：', task)
        url = 'https://music.163.com/weapi/user/getfolloweds?csrf_token=cdee144903c5a32e6752f50180329fc9'
        page = 1
        pages = task[1] / 20
        while page:
            print('正在抓取第%d页, %s用户的粉丝...' % (page, task[0]))
            offset = (page - 1) * 20
            id_msg = '{userId: "' + task[0] + '", offset: ' + str(
                offset) + ', total: "true", limit: "20", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
            enc_text, enc_seckey = self.get_params(id_msg)
            data = {'params': enc_text, 'encSecKey': enc_seckey}
            user_json = self.get_fans_json(url, data)
            if user_json is None:
                break
            try:
                follows = user_json['followeds']
                for follow in follows:
                    self.user_fans(follow)
                page += 1
            except Exception as e:
                print(user_json)
                print(e)
                page += 1
            if page > pages:
                return

    # 从数据库获取歌手id
    def get_singer_id(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        mysql_command.cursor.execute("select homepage_id from singer limit 0, 10")
        singer_list = mysql_command.cursor.fetchall()
        for singer_id in singer_list:
            singer_id = singer_id['homepage_id']
            if len(singer_id) > 0:
                print('开始获取歌手ID为：%s的信息。。。' % singer_id)
                url = 'https://music.163.com/user/home?id=%s' % singer_id
                replace = 0
                while replace < 3:
                    try:
                        res = requests.get(url, headers=self.headers)
                        time.sleep(2)
                        soup = BeautifulSoup(res.text, 'html5lib')
                        count = soup.find('ul', attrs={'class': 'data s-fc3 f-cb'})
                        try:
                            count.find_all('strong')
                            break
                        except Exception as e:
                            print('重新爬取歌手信息！失败原因是%s' % e)
                            replace += 1
                            time.sleep(2)
                    except Exception as e:
                        print(e)
                        print(url)
                user_num = []
                try:
                    for num in count.find_all('strong'):
                        num = re.findall(r'>(.*?)<', str(num))[0]
                        user_num.append(int(num))
                    # print(user_num)
                except Exception as e:
                    print(e)
                    print('count: ', count)
                    print('id: ', singer_id)
                    continue
                if user_num[1] > 1000:
                    user_num[1] = 1000
                if user_num[2] > 1000:
                    user_num[2] = 1000
                # self.get_follow_info([singer_id, user_num[1]])
                # self.get_follows_info([singer_id, user_num[2]])
                self.get_fans_info(singer_id)

                #self.task_follows_spider.put([singer_id, user_num[1]])
                #self.task_follow_spider.put([singer_id, user_num[2]])
                # self.task_list_spider.put(singer_id)

    # 保存歌单信息
    def save_music_list(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.list_queue.get()
            mysql_command.insert_list(result)

    # 保存用户信息
    def save_user_info(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.user_queue.get()
            mysql_command.insert_user(result)

    def spider_main(self):
        Thread(target=self.save_music_list, args=()).start()
        Thread(target=self.save_user_info, args=()).start()
        self.get_singer_id()


if __name__ == '__main__':
    US = UserSpider()
    US.spider_main()
