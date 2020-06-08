# -*- coding: utf-8 -*-
# @Time    : 2019-12-30 14:12
# @Author  : GCY
# @FileName: comm_user_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import requests
import re
import os
import time
import math
import json
import codecs
import base64
import random
from threading import Thread
from Crypto.Cipher import AES
from bs4 import BeautifulSoup
from utils.sql_save import MySQLCommand
import multiprocessing as mp
import utils.all_config as config
from selenium import webdriver
from pyvirtualdisplay import Display
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC


class CommUserSpider():
    def __init__(self):
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
            'origin': 'https://music.163.com',
            'Referer': 'https://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
        self.option_driver = False  # 实例化driver
        self.driver = None
        self.list_task = mp.Queue()  # 歌单队列
        self.music_task = mp.Queue()  # 歌曲任务
        self.list_result_queue = mp.Queue()  # 歌单结果
        self.user_result_queue = mp.Queue()  # 用户结果
        self.music_result_queue = mp.Queue()  # 歌曲结果

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

    # 获取json数据
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
        return result

    # 歌单用户结果处理
    def user_music_list(self, user_info):
        result = {}
        for key in config.music_user_key:
            try:
                value = str(user_info[key])
                value = self.re_value(value)
                result[key] = value
            except:
                result.setdefault(key, '')
        return result

    # 对抓取数据进行处理
    def handle_info(self, user_all_info, uid, user_id):
        try:
            if len(user_all_info) > 0:
                music_list = []
                list_id = []
                user_result = {}
                replace = False
                for i in range(len(user_all_info)):
                    music_user = {}
                    lists = self.music_list(user_all_info[i])
                    list_id.append(user_all_info[i]['id'])
                    if lists['userId'] == uid:
                        if replace is False:
                            user_result = self.user_music_list(user_all_info[i]['creator'])
                        replace = True
                    else:
                        user_result = self.user_music_list(user_all_info[i]['creator'])
                    music_user.setdefault('list', lists)
                    music_user.setdefault('user', user_result)
                    music_list.append(music_user)

                for index, i in enumerate(music_list):
                    if i['user']['userId'] == uid or i['user']['userId'] == user_id:
                        i['user']['userId'] = user_id
                        i['user'].setdefault('list_id', str(list_id))
                    else:
                        i['user'].setdefault('list_id', '')
                    self.list_result_queue.put(i['list'])
                    self.user_result_queue.put(i['user'])

            else:
                print("id为{}用户没有创建歌单".format(uid))
        except:
            result = {}
            return [result, []]

    def music_info(self, soup):
        song_list = ''
        listen_num = soup.find('div', attrs={'class': 'u-title u-title-1 f-cb m-record-title'}).find('h4').string
        listen_num = re.findall(r'累积听歌(.*?)首', listen_num)[0].strip()
        song_info = soup.find_all('span', attrs={'class': 'txt'})
        song_width = soup.find_all('div', attrs={'class': 'tops'})
        for i, j in zip(song_info, song_width):
            song = i.find_all('a')
            song_id, singer_id = '', ''
            if len(song) >= 2:
                song_id = song[0]['href'].replace('/song?id=', '')
                for i in song[1:]:
                    singer_id += i['href'].replace('/artist?id=', '') + ','
            else:
                try:
                    song_id = song[0]['href'].replace('/song?id=', '')
                    singer_id = ''
                except:
                    singer_id = song[1]['href'].replace('/artist?id=', '')
                    song_id = ''
            try:
                width = j.find('span')['style'].replace('width:', '').replace('%;', '')
            except:
                width = ''
            song = json.dumps({'song_id': song_id, 'singer_id': singer_id, 'width': width})
            song_list += song + ','
        return listen_num, song_list

    def driver_firefox(self):
        try:
            # 实例化一个启动参数对象
            firefox_options = Options()
            # 设置浏览器窗口大小 无界面运行
            firefox_options.add_argument('--headless')
            self.driver = webdriver.Firefox(firefox_options=firefox_options)
        except Exception as e:
            print('启动Firefox浏览器失败，原因是：', e, '....重新启动....')
            try:
                self.driver.close()
                os.system('killall firefox')
            except:
                pass
            self.driver = None

    def get_music(self):
        while True:
            if self.option_driver is False:
                self.driver_firefox()
                self.option_driver = True
                if self.driver is None:
                    self.option_driver = False
                    continue
            user_id = self.music_task.get()
            userId = user_id.strip()
            print('开始获取用户ID为：%s的歌曲。。。' % userId)
            try:
                self.driver.get("http://music.163.com/user/songs/rank?id=%s" % userId)  # 需要抓取的用户链接，这里注意的是这里的id不是用户的id，而是用户听歌形成的所有时间排行的排行版的id
                self.driver.switch_to.frame('g_iframe')  # 从windows切换到frame，切换到歌曲列表所在的frame
            except:
                self.option_driver = False
                continue
            try:
                time.sleep(1)
                wait = ui.WebDriverWait(self.driver, 60)
                # 找到歌曲列表所在的父标签
                if wait.until(lambda driver: driver.find_element_by_class_name('g-bd')):
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "z-sel")))
                    if self.driver.find_element_by_class_name('z-sel').text == '所有时间':
                        soup = BeautifulSoup(self.driver.page_source, features='lxml')
                        listen_num, all_time = self.music_info(soup)
                        week_time = ''
                    else:
                        soup = BeautifulSoup(self.driver.page_source, features='lxml')
                        listen_num, week_time = self.music_info(soup)
                        self.driver.find_element_by_id('songsall').click()
                        time.sleep(1)
                        soup = BeautifulSoup(self.driver.page_source, features='lxml')
                        listen_num, all_time = self.music_info(soup)
                else:
                    all_time, week_time, listen_num = '', '', '0'

                result = {'userId': user_id, 'all_music': all_time, 'week_music': week_time, 'listen_num': listen_num}
                self.user_result_queue.put(result)
            except Exception as e:
                print(e)
                try:
                    soup = BeautifulSoup(self.driver.page_source, features='lxml')
                    listen_num = soup.find('div', attrs={'class': 'u-title u-title-1 f-cb m-record-title'}).find(
                        'h4').string
                    listen_num = re.findall(r'累积听歌(.*?)首', listen_num)[0].strip()
                except:
                    listen_num = '0'
                result = {'userId': user_id, 'all_music': '', 'week_music': '', 'listen_num': listen_num}
                self.user_result_queue.put(result)

    def get_list(self):
        while True:
            user_id, playlistCount = self.list_task.get()
            userId = user_id.strip()
            print('开始获取用户ID为：%s的歌单。。。' % userId)
            url = 'https://music.163.com/weapi/user/playlist?csrf_token='
            id_msg = '{uid: "' + str(userId) + '", wordwrap: "7", offset: "0", ' \
                                                'total: "true", limit: "100", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
            enc_text, enc_seckey = self.get_params(id_msg)
            data = {'params': enc_text, 'encSecKey': enc_seckey}
            user_json = self.get_fans_json(url, data)
            if user_json is not None:
                user_all_info = user_json['playlist']
                self.handle_info(user_all_info, userId, user_id)
                if len(user_all_info) >= 100:
                    url = 'https://music.163.com/weapi/user/playlist?csrf_token='
                    id_msg = '{uid: "' + str(userId) + '", wordwrap: "7", offset: "0", ' \
                                                       'total: "true", limit: "1000", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
                    enc_text, enc_seckey = self.get_params(id_msg)
                    data = {'params': enc_text, 'encSecKey': enc_seckey}
                    user_json = self.get_fans_json(url, data)
                    user_all_info = user_json['playlist'][100:]
                    self.handle_info(user_all_info, userId, user_id)
                time.sleep(3)

    # 从数据库获取用户id
    def get_user_id(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        mysql_command.cursor.execute("select userId, playlistCount from user")
        user_list = mysql_command.cursor.fetchall()
        for userinfo in user_list:
            user_id = userinfo['userId']
            playlistCount = userinfo['playlistCount']
            if playlistCount is not None:
                playlistCount = playlistCount.strip()
            else:
                playlistCount = 0
            if len(user_id) > 0:
                self.music_task.put(user_id)
                self.list_task.put([user_id, int(playlistCount)])
                time.sleep(2)

    # 保存歌单信息
    def save_music_list(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.list_result_queue.get()
            mysql_command.insert_list(result)

    # 保存用户信息
    def save_user_info(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.user_result_queue.get()
            mysql_command.insert_user(result)

    # spider主函数
    def spider_main(self):
        Thread(target=self.get_music, args=()).start()
        # Thread(target=self.get_list, args=()).start()
        # Thread(target=self.save_music_list, args=()).start()
        Thread(target=self.save_user_info, args=()).start()
        self.get_user_id()


if __name__ == '__main__':
    CUS = CommUserSpider()
    CUS.spider_main()



