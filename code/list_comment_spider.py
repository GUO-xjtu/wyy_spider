# -*- coding: utf-8 -*-
# @Time    : 2019-12-26 13:53
# @Author  : GCY
# @FileName: list_comment_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import requests
import json
import re
import math
import random
import time
import pymysql
from Crypto.Cipher import AES
import codecs
import base64
from bs4 import BeautifulSoup
from threading import Thread
from multiprocessing import Queue
from utils.sql_save import MySQLCommand


class ListCommSpider():
    def __init__(self):
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'WM_TID=36fj4OhQ7NdU9DhsEbdKFbVmy9tNk1KM; _iuqxldmzr_=32; _ntes_nnid=26fc3120577a92f179a3743269d8d0d9,1536048184013; _ntes_nuid=26fc3120577a92f179a3743269d8d0d9; __utmc=94650624; __utmz=94650624.1536199016.26.8.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); WM_NI=2Uy%2FbtqzhAuF6WR544z5u96yPa%2BfNHlrtTBCGhkg7oAHeZje7SJiXAoA5YNCbyP6gcJ5NYTs5IAJHQBjiFt561sfsS5Xg%2BvZx1OW9mPzJ49pU7Voono9gXq9H0RpP5HTclE%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed5cb8085b2ab83ee7b87ac8c87cb60f78da2dac5439b9ca4b1d621f3e900b4b82af0fea7c3b92af28bb7d0e180b3a6a8a2f84ef6899ed6b740baebbbdab57394bfe587cd44b0aebcb5c14985b8a588b6658398abbbe96ff58d868adb4bad9ffbbacd49a2a7a0d7e6698aeb82bad779f7978fabcb5b82b6a7a7f73ff6efbd87f259f788a9ccf552bcef81b8bc6794a686d5bc7c97e99a90ee66ade7a9b9f4338cf09e91d33f8c8cad8dc837e2a3; JSESSIONID-WYYY=G%5CSvabx1X1F0JTg8HK5Z%2BIATVQdgwh77oo%2BDOXuG2CpwvoKPnNTKOGH91AkCHVdm0t6XKQEEnAFP%2BQ35cF49Y%2BAviwQKVN04%2B6ZbeKc2tNOeeC5vfTZ4Cme%2BwZVk7zGkwHJbfjgp1J9Y30o1fMKHOE5rxyhwQw%2B%5CDH6Md%5CpJZAAh2xkZ%3A1536204296617; __utma=94650624.1052021654.1536048185.1536199016.1536203113.27; __utmb=94650624.12.10.1536203113',
            'Host': 'music.163.com',
            'Referer': 'http://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'}
        self.host_path = '../data/host.txt'
        self.cookie_path = '../data/cookie.txt'
        self.ip_queue = Queue()
        self.save_queue = Queue()  # 结果队列
        self.task_queue = Queue()  # 任务队列
        self.save_user_queue = Queue()  # 评论人队列
        self.conn_task = False
        self.conn_result = False
        self.conn_user = False
        self.prosiex_start = True  # 是否启动代理IP爬取线程

    # 重连数据库
    def task_conn(self):
        self.mysqlCommand = MySQLCommand()
        self.mysqlCommand.connectdb()
        self.conn_task = True

    def result_conn(self):
        self.mysqlResult = MySQLCommand()
        self.mysqlResult.connectdb()
        self.conn_result = True

    def user_conn(self):
        self.mysqlUser = MySQLCommand()
        self.mysqlUser.connectdb()
        self.conn_user = True

    # 生成16个随机字符
    def generate_random_strs(self, length):
        string = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        # 控制次数参数i
        i = 0
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
    def AESencrypt(self, msg, key):
        # 如果不是16的倍数则进行填充(paddiing)
        padding = 16 - len(msg) % 16
        # 这里使用padding对应的单字符进行填充
        msg = msg + padding * chr(padding)
        # 用来加密或者解密的初始向量(必须是16位)
        iv = '0102030405060708'

        cipher = AES.new(key, AES.MODE_CBC, iv)

        # 加密后得到的是bytes类型的数据
        encryptedbytes = cipher.encrypt(msg)
        # 使用Base64进行编码,返回byte字符串
        encodestrs = base64.b64encode(encryptedbytes)
        # 对byte字符串按utf-8进行解码
        enctext = encodestrs.decode('utf-8')

        return enctext

    # RSA加密
    def RSAencrypt(self, randomstrs, key, f):
        # 随机字符串逆序排列
        string = randomstrs[::-1]
        # 将随机字符串转换成byte类型数据
        text = bytes(string, 'utf-8')
        seckey = int(codecs.encode(text, encoding='hex'), 16) ** int(key, 16) % int(f, 16)
        return format(seckey, 'x').zfill(256)

    # 获取参数
    def get_params(self, page):
        # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
        # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
        # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'
        # 偏移量
        offset = (page - 1) * 20
        # offset和limit是必选参数,其他参数是可选的,其他参数不影响data数据的生成
        msg = '{"offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
        key = '0CoJUm6Qyw8W8jud'
        f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
        e = '010001'
        enctext = self.AESencrypt(msg, key)
        # 生成长度为16的随机字符串
        i = self.generate_random_strs(16)

        # 两次AES加密之后得到params的值
        encText = self.AESencrypt(enctext, i)
        # RSA加密之后得到encSecKey的值
        encSecKey = self.RSAencrypt(i, e, f)
        return encText, encSecKey

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
            print(proxies, '检查通过！')
            return True

    # 生成IP代理
    def ip_proxies(self):
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

        fp = open(self.host_path, 'a+', encoding=('utf-8'))
        self.ip_pool = []
        for i in range(20):
            api = api.format(1)
            respones = requests.get(url=api, headers=header)
            time.sleep(3)
            soup = BeautifulSoup(respones.text, 'html.parser')
            container = soup.find_all(name='tr', attrs={'class': 'odd'})
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
                        self.ip_pool.append(IPport)
                        fp.write(IPport)
                        fp.write('\n')
                        self.ip_queue.put(IPport)
                except Exception as e:
                    print('No IP！')
            if self.prosiex_start is False:
                break
        fp.close()

    # 从host.txt中读取代理
    def ip_txt(self):
        print('IP代理爬取不够，从host.txt中添加...')
        with open(self.host_path, 'r') as fp:
            ip_port = fp.readlines()
            for i in ip_port:
                self.ip_pool.append(i)
                self.ip_queue.put(i)

    def get_comments_json(self, url, data):
        repeat = 0
        while repeat < 4:
            try:
                r = requests.post(url, headers=self.headers, data=data)
                time.sleep(repeat+2)
                r.encoding = "utf-8"
                if r.status_code == 200:
                    # 返回json格式的数据
                    result = r.json()
                    if 'total' in result.keys():
                        total = result['total']
                        repeat = 0
                        self.ip_pool = []
                        return result, total
                    elif 'code' in result.keys():
                        if result['code'] == -460:
                            if repeat < 3:
                                self.check_headers()
                            else:
                                if len(self.ip_pool) < 10:
                                    Thread(target=self.ip_proxies, args=()).start()
                                if len(self.ip_pool) < 10:
                                    self.ip_txt()
                                result, total = self.ip_spider(url, data)
                                if result is None:
                                    self.prosiex_start = False
                                    for i in range(90000):
                                        print('\r IP可能被封，代理IP不可用！需要等待' + str(90000 - i) + '秒...', sep=' ', end='', flush=True)
                                        time.sleep(1)
                                    self.prosiex_start = True
                                else:
                                    self.prosiex_start = True
                                    return result, total
                            repeat += 1

            except:
                time.sleep(1)
                repeat += 1
                print("第%d次爬取url为%s 的页面失败!正重新尝试..." % (repeat, url))
        return None, None

    # 使用代理爬取
    def ip_spider(self, url, data):
        repeat = 0
        while repeat < 50:
            proxies = self.ip_queue.get()
            proxies = json.loads(proxies)
            ip = '://' + proxies['ip'] + ':' + proxies['port']
            proxies = {'https': 'https' + ip}
            print('使用的代理IP为：', proxies)
            try:
                r = requests.post(url, headers=self.headers, data=data, proxies=proxies)
                time.sleep(2)
                try:
                    r.encoding = 'utf-8'
                    result = r.json()
                except Exception as e:
                    print('错误：', e)
                    return r, None
                if 'code' in result.keys():
                    if result['code'] == -460:
                        repeat += 1
                        print('%r的IP代理不可用, 访问URL为%s的网页失败！原因是%s, 重试第%d次' % (proxies, url, result, repeat + 1))
                if 'total' in result.keys():
                    total = result['total']
                    print('result: ', result)
                    return result, total
            except Exception as e:
                print('IP代理为%r, 访问URL为%s的网页失败！原因是%s, 重试第%d次' % (proxies, url, e, repeat+1))
                repeat += 1
        print('返回的是none')
        return None, None

    # 数据正则处理
    def re_value(self, value):
        value = re.sub(r'\r|\n|\\|\'|\{|\}|\"', ' ', value)
        return value

    # 获取热门评论
    def hot_comments(self, html, list_id, pages, total, creater_id):
        try:
            print("正在获取歌单{}的热门评论,总共有{}页{}条评论!".format(list_id, pages, total))
            if 'hotComments' in html:
                for item in html['hotComments']:
                    # 提取发表热门评论的用户名
                    user = item['user']
                    if item['content'] is not None:
                        comment = self.re_value(item['content'])
                    else:
                        comment = ''
                    # 写入文件
                    hot_comment = {'hot_comment': '1', 'user_id': str(user['userId']), 'comment': comment,
                                   'likedCount': str(item['likedCount']), 'time': str(item['time']), 'list_id': list_id,
                                   'creater_id': creater_id}
                    self.save_user_queue.put(str(user['userId']))
                    # 回复评论
                    reply_comment = []
                    if len(item['beReplied']) != 0:
                        for reply in item['beReplied']:
                            # 提取发表回复评论的用户名
                            reply_user = reply['user']
                            if reply['content'] is not None:
                                content = self.re_value(reply['content'])
                            else:
                                content = ''
                            reply_comment.append({'user_id': reply_user['userId'], 'content': content})
                            self.save_user_queue.put(str(reply_user['userId']))
                    hot_comment['reply'] = str(reply_comment)
                    self.save_queue.put(hot_comment)
        except Exception as e:
            print('获取歌单{}的评论失败，原因是{}'.format(list_id, e))
            return False

    # 获取普通评论
    def comments(self, html, list_id, i, pages, total, creater_id):
        # try:
        print("正在获取歌单{}的第{}页评论,总共有{}页{}条评论!".format(list_id, i, pages, total))
        # 全部评论
        for item in html['comments']:
            # 提取发表评论的用户名
            user = item['user']
            if item['content'] is not None:
                comment = self.re_value(item['content'])
            else:
                comment = ''
            comment = {'hot_comment': '0', 'user_id': str(user['userId']), 'comment': comment,
                       'likedCount': str(item['likedCount']), 'time': str(item['time']), 'list_id': list_id,
                       'creater_id': creater_id}
            self.save_user_queue.put(str(user['userId']))
            # 回复评论
            reply_comment = []
            if len(item['beReplied']) != 0:
                for reply in item['beReplied']:
                    # 提取发表回复评论的用户名
                    reply_user = reply['user']
                    if reply['content'] is not None:
                        content = self.re_value(reply['content'])
                    else:
                        content = ''
                    reply_comment.append({'user_id': reply_user['userId'], 'content': content})
                    self.save_user_queue.put(str(reply_user['userId']))
            comment['reply'] = str(reply_comment)
            self.save_queue.put(comment)
        return True
        # except Exception as e:
        #     print('获取歌单{}的第{}页评论失败，原因是{}'.format(list_id, i, e))
        #     return False

    def page_spider(self):
        while True:
            list_id, creater_id = self.task_queue.get()
            print('开始爬取ID为%s歌单的所有评论！！！！！' % list_id)
            url1 = 'https://music.163.com/playlist?id=' + list_id
            url = 'https://music.163.com/weapi/v1/resource/comments/A_PL_0_' + list_id + '?csrf_token='
            page = 1
            params, encSecKey = self.get_params(page)
            data = {'params': params, 'encSecKey': encSecKey}
            # 获取第一页评论
            try:
                html, total = self.get_comments_json(url, data)
                # 评论总数
                if html is None:
                    continue
                if 'comments' in html.keys():
                    if html['comments'] is None:
                        try:
                            requests.get(url1, headers=self.headers)
                        except:
                            pass
                        html, total = self.get_comments_json(url, data)
                        if html is None:
                            continue
            except Exception as e:
                print('此歌单: %s, 评论爬取失败！原因：%s' % (list_id, e))
                continue
            # 总页数
            pages = math.ceil(total / 20)
            try:
                self.hot_comments(html, list_id, pages, total, creater_id)
            except Exception as e:
                print('此歌单: %s, 热门评论爬取失败！原因：%s' % (list_id, e))
            try:
                self.comments(html, list_id, page, pages, total, creater_id)
            except Exception as e:
                print('此歌单: %s, 第一页普通评论爬取失败！原因：%s' % (list_id, e))

            # 开始获取歌曲的全部评论
            page = 2
            reverse = False  # 若请求的评论结果为空，则从最后评论页向前爬取
            while True:
                if page == 0:
                    break
                params, encSecKey = self.get_params(page)
                data = {'params': params, 'encSecKey': encSecKey}
                html, total = self.get_comments_json(url, data)
                # 从后向前已经把可请求的评论页请求完成，则跳出循环
                if reverse is True and len(html['comments']) == 0:
                    break

                if len(html['comments']) == 0:
                    reverse = True
                    page = pages
                    print('开始倒序爬取！')
                    continue
                # 从第二页开始获取评论
                try:
                    self.comments(html, list_id, page, pages, total, creater_id)
                except Exception as e:
                    print('此歌单: %s, 第%d页普通评论爬取失败！原因：%s' % (list_id, page, e))
                    print('重新爬取！')
                    if 'total' in str(e):
                        for i in range(90000):
                            print('\r IP可能被封，需要等待' + str(90000 - i) + '秒...', sep=' ', end='', flush=True)
                            time.sleep(1)
                    elif 'comments' in str(e):
                        for i in range(10000):
                            print('\r IP可能被封，需要等待' + str(10000 - i) + '秒...', sep=' ', end='', flush=True)
                            time.sleep(1)
                    else:
                        continue
                if reverse is False:
                    page += 1
                else:
                    page -= 1
                # 如果爬取完成，则跳出循环
                if page > pages:
                    break
            print('=='*20, '%s====歌====单====爬====取====完====成' % list_id, '=='*20)

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

    # 从数据库获取任务
    def sql_task(self):
        conn, cursor = self.conn_data()
        cursor.execute("select id, userId from music_list")
        music_ids = cursor.fetchall()

        for id in music_ids:
            if id is None:
                continue
            list_id = id.get('id').strip()
            creater_id = id.get('userId').strip()
            print(list_id)
            self.task_queue.put([list_id, creater_id])
            time.sleep(10)

    # 评论保存至数据库
    def save_result(self):
        while True:
            comment = self.save_queue.get()
            if self.conn_result is False:
                self.result_conn()
            try:
                self.mysqlResult.insert_list_comm(comment)
            except:
                self.conn_result = False

    # 评论人保存至数据库
    def save_user(self):
        while True:
            comment_user = self.save_user_queue.get()
            if self.conn_user is False:
                self.user_conn()
            try:
                self.mysqlUser.insert_co_user(comment_user)
            except:
                self.conn_user = False

    def spider_main(self):
        # Thread(target=self.page_spider, args=()).start()
        # Thread(target=self.page_spider, args=()).start()
        # Thread(target=self.page_spider, args=()).start()
        Thread(target=self.page_spider, args=()).start()
        Thread(target=self.save_result, args=()).start()
        Thread(target=self.save_user, args=()).start()
        self.sql_task()


if __name__ == '__main__':
    LCS = ListCommSpider()
    LCS.spider_main()

