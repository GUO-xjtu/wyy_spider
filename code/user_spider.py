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
import json
import random
from threading import Thread
from Crypto.Cipher import AES
from bs4 import BeautifulSoup, ResultSet
from utils.sql_save import MySQLCommand
import multiprocessing as mp
import utils.all_config as config
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler


class UserSpider(object):
    def __init__(self):
        self.file_path = '../data/'  # 用户信息保存位置
        self.not_exist_user_path = '/home/cyguo/wyy_spider/data/not_exist_user.csv'
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
        self.host_path = '../data/host.txt'
        self.cookie_path = '../data/cookie.txt'
        self.ip_queue = mp.Queue()
        self.prosiex_start = False  # 是否开始使用代理爬取任务
        self.prosiex_time = None  # 开始使用IP代理的时间
        # self.ip_pool = []

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

    def check_headers(self):
        cookie_list = []
        with open(self.cookie_path, 'r') as fp:
            for i in fp.readlines():
                i = json.loads(i)
                cookie_list.append(i)
        self.headers['Cookie'] = random.choice(cookie_list)['cookie']

    # 检查代理IP是否可用
    def check_ip(self, proxies):
        try:
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                                    'Chrome/64.0.3282.186 Safari/537.36'}
            ip = '://' + proxies['ip'] + ':' + proxies['port']
            proxies = {'https': 'https' + ip}
            url = 'https://www.ipip.net/'
            r = requests.get(url, headers=header, proxies=proxies, timeout=5)
            r.raise_for_status()
        except:
            return False
        else:
            print('爬取到的:', proxies, '代理IP--->检查通过！')
            return True

    # 生成IP代理
    def ip_proxies(self):
        tsp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        print("%r 开启爬取IP代理的定时任务....." % tsp)
        api = 'http://www.xicidaili.com/wn/{}'
        header = {
            'Cookie': '_free_proxy_session=BAh7B0kiD3Nlc3Npb25faWQGOgZFVEkiJTZlOTVjNGQ1MmUxMDlmNzhlNjkwMDU3MDUxMTQ4YTUwBjsAVEkiEF9jc3JmX3Rva2VuBjsARkkiMUpRcU9ySVRNcmlOTytuNm9ZWm53RUFDYzhzTnZCbGlNa0ZIaHJzancvZEU9BjsARg%3D%3D--742b1937a06cc747483cd594752ef2ae80fc4d91; Hm_lvt_0cf76c77469e965d2957f0553e6ecf59=1577952296; Hm_lpvt_0cf76c77469e965d2957f0553e6ecf59=1578016572',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/'
                          '537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'Host': 'www.xicidaili.com',
            'Connection': 'keep-alive',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Cache-Control': 'no-cache'}

        ip_num = 0
        replace = 1
        ip_port_time = time.time()
        while True:
            api = api.format(1)
            try:
                respones = requests.get(url=api, headers=header, timeout=10)
                time.sleep(replace)
                soup = BeautifulSoup(respones.text, 'html.parser')
                container = soup.find_all(name='tr', attrs={'class': 'odd'})
            except:
                replace += 1
                continue
            if self.ip_queue.qsize() == 0 and time.time() - ip_port_time > 20:
                self.ip_txt()
            for tag in container:
                try:
                    con_soup = BeautifulSoup(str(tag), 'html.parser')
                    td_list = con_soup.find_all('td')
                    ip = str(td_list[1])[4:-5]
                    port = str(td_list[2])[4:-5]
                    _type = td_list[5].text
                    IPport = {'ip': ip, 'port': port, 'type': _type.lower()}
                    if self.check_ip(IPport):
                        IPport = json.dumps(IPport)
                        # self.ip_pool.append(IPport)
                        try:
                            fp = open(self.host_path, 'a+', encoding=('utf-8'))
                            fp.write(IPport)
                            fp.write('\n')
                            fp.close()
                        except:
                            pass
                        ip_port_time = time.time()
                        self.ip_queue.put(IPport)
                        ip_num += 1
                except Exception as e:
                    print('No IP！')
            if ip_num > 30:
                break

    # 从host.txt中读取代理
    def ip_txt(self):
        print('IP代理爬取不够，从host.txt中添加...')
        ip_list = []
        with open(self.host_path, 'r') as fp:
            ip_port = fp.readlines()
            for i in ip_port:
                # self.ip_pool.append(i)
                ip_list.append(i)
        ips = random.choices(ip_list, k=2)
        for i in ips:
            self.ip_queue.put(i)

    # 使用代理爬取
    def ip_spider(self, url, data, task):
        repeat = 0
        while repeat < 50:
            proxies = self.ip_queue.get()
            proxies = json.loads(proxies)
            ip = '://' + proxies['ip'] + ':' + proxies['port']
            proxies = {'https': 'https' + ip}
            print('使用的代理IP为：', proxies, '使用此ip爬取%s任务....' % task)
            try:
                r = requests.post(url, headers=self.headers, data=data, proxies=proxies, timeout=10)
                time.sleep(1)
                try:
                    r.encoding = 'utf-8'
                    result = r.json()
                except Exception as e:
                    print('使用IP代理爬取后转换json失败，错误：', e)
                    return r
                if 'code' in result.keys():
                    if result['code'] == -460:
                        repeat += 1
                        print('%r的IP代理不可用, 访问URL为%s的网页失败！原因是%s, 重试第%d次' % (proxies, url, result, repeat + 1))
                try:
                    return r
                except Exception as e:
                    print('使用代理成功，但为转成json，返回的是：', r)
                    print('以上错误的原因是：', e)
                    return None
            except Exception as e:
                print('IP代理为%r, 访问URL为%s的网页失败！原因是%s, 重试第%d次' % (proxies, url, e, repeat+1))
                repeat += 1
        print('IP代理---->返回的是none')
        return None

    # 获取粉丝页的json数据
    def get_fans_json(self, url, data, task):
        repeat = 1
        while repeat < 15:
            try:
                if not self.prosiex_start or time.time() - self.prosiex_time > 43200:
                    r = requests.post(url, headers=self.headers, data=data)
                    time.sleep(repeat)
                    r.encoding = "utf-8"
                    self.prosiex_start = False
                else:
                    print('----->由于主机ip已被封，尝试使用代理IP抓取%s任务<-----' % task)
                    r = self.ip_spider(url, data, task)

                if r.status_code == 200:
                    # 返回json格式的数据
                    r = r.json()
                    # print(r.keys())
                    if 'follow' in r.keys():
                        if len(r['follow']) == 0:
                            print('抓取到的关注页为空，尝试重新抓取....')
                            repeat += 1
                            if 5 < repeat < 10:
                                print('尝试更换headers重新抓去....')
                                self.check_headers()
                            elif repeat > 10:
                                print('尝试使用代理IP抓取%s任务....' % task)
                                self.prosiex_start = True
                                self.prosiex_time = time.time()
                                try:
                                    return self.ip_spider(url, data, task).json()
                                except:
                                    return None
                        else:
                            return r
                    if 'followeds' in r.keys():
                        if len(r['followeds']) == 0:
                            print('抓取到的粉丝页为空，尝试重新抓取....')
                            repeat += 1
                            if 5 < repeat < 10:
                                self.check_headers()
                            elif repeat > 10:
                                print('尝试使用代理IP抓取%s任务....' % task)
                                self.prosiex_start = True
                                self.prosiex_time = time.time()
                                try:
                                    return self.ip_spider(url, data, task).json()
                                except:
                                    return None
                        else:
                            return r
                    if 'playlist' in r.keys():
                        return r
            except Exception as e:
                print("第%d次获取url为%s的信息失败，原因是%s" % (repeat, url, e))
                print('r是：', r)
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
        print('爬取用户为%s的歌单任务....' % task_id)
        # 粉丝数据
        url = 'https://music.163.com/weapi/user/playlist?csrf_token=cdee144903c5a32e6752f50180329fc9'
        # uid为粉丝id
        id_msg = '{uid: "' + str(task_id) + '", wordwrap: "7", offset: "0", ' \
                                        'total: "true", limit: "36", csrf_token: "cdee144903c5a32e6752f50180329fc9"}'
        params, encSecKey = self.get_params(id_msg)
        data = {'params': params, 'encSecKey': encSecKey}
        user_json = self.get_fans_json(url, data, '歌单')
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
            user_json = self.get_fans_json(url, data, '关注的人')
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
            user_json = self.get_fans_json(url, data, '粉丝')
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

    # 从not_exist_user.csv获取用户id
    def get_user_id(self):
        spider_data = pd.read_csv(self.not_exist_user_path)

        for user_id in spider_data['user_id'].to_list():
            user_id = str(user_id).strip()
            if len(user_id) > 6:
                print('开始获取ID为：%s的用户信息....' % user_id)
                url = 'https://music.163.com/user/home?id=%s' % user_id
                replace = 0
                while replace < 3:
                    try:
                        res = requests.get(url, headers=self.headers)
                        time.sleep(replace)
                        soup = BeautifulSoup(res.text, 'html5lib')
                        count = soup.find('ul', attrs={'class': 'data s-fc3 f-cb'})
                        try:
                            count.find_all('strong')
                            break
                        except Exception as e:
                            print('重新爬取用户信息！失败原因是%s' % e)
                            replace += 1
                            time.sleep(1)
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
                    print('id: ', user_id)
                    continue
                if user_num[1] > 1000:
                    user_num[1] = 1000
                if user_num[2] > 1000:
                    user_num[2] = 1000

                if user_num[1] != 0:
                    self.get_follow_info([user_id, user_num[1]])
                if user_num[2] != 0:
                    self.get_follows_info([user_id, user_num[2]])

                self.get_fans_info(user_id)
                # self.task_follows_spider.put([user_id, user_num[1]])
                # self.task_follow_spider.put([user_id, user_num[2]])
                # self.task_list_spider.put(user_id)
            print('ID为：%s的用户信息爬去完成！' % user_id)

    # 开启爬取代理ip定时任务
    def init_scheduler(self):
        # BackgroundScheduler: 适合于要求任何在程序后台运行的情况，当希望调度器在应用后台执行时使用
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.ip_proxies(), 'interval', seconds=120, id='my_heartbeat')
        scheduler.start()

    # 保存歌单信息
    def save_music_list(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.list_queue.get()
            # print('爬去的歌单结果: ', result)
            mysql_command.insert_list(result)

    # 保存用户信息
    def save_user_info(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            result = self.user_queue.get()
            # print('爬去的用户结果: ', result)
            mysql_command.insert_user(result)

    def spider_main(self):
        Thread(target=self.save_music_list, args=()).start()
        time.sleep(0.3)
        Thread(target=self.save_user_info, args=()).start()
        time.sleep(0.3)
        Thread(target=self.init_scheduler, args=()).start()
        time.sleep(0.3)
        self.get_user_id()


if __name__ == '__main__':
    US = UserSpider()
    US.spider_main()
