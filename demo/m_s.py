# -*- coding: utf-8 -*-
# @Time    : 2020-01-29 13:32
# @Author  : GCY
# @FileName: music_csv.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import re
import time
import requests
import json
import random
from tqdm import tqdm
from bs4 import BeautifulSoup


class MusicSpider(object):
    def __init__(self):
        self.save_path = '../demo/result.txt'  # 信息保存位置
        self.file_path = '../demo/music.txt'
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
        self.num = 0  # 从第0首歌曲开始爬取
        self.music = False
        self.cookie_path = '../data/cookie.txt'

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

    # 获取歌曲详情
    def get_music_info(self, m_id):
        result = ''
        url = 'https://music.163.com/song?id=%s' % m_id
        repeat = 0
        while repeat < 5:
            try:
                response = requests.get(url, headers=self.headers)
                time.sleep(repeat)
                response = response.text
                soup = BeautifulSoup(response, 'html5lib')
                temp = soup.find_all('p', attrs={'class': 'des s-fc4'})[0]
                temp = temp.find_all('a', attrs={'class': 's-fc7'})
                for info in temp:
                    try:
                        singer_id = info['href'].replace('/artist?id=', '').strip()
                        result += (m_id+'|'+singer_id+'*')
                    except Exception:
                        pass
                break
            except Exception as e:
                result = ''
                repeat += 1
                print('ID为%s的歌曲的歌手和专辑信息获取失败，使用默认空值！失败原因是%s' % (m_id, e))
        res = result[:-1]
        return res

    # 从csv文件下发任务
    def get_list_id(self):
        save_fp = open(self.save_path, 'w')
        save_fp.write('tag\tms\n')
        with open(self.file_path, 'r') as fp:
            for index, i in tqdm(enumerate(fp.readlines())):
                if index == 0:
                    continue
                tag, music = i.split('\t')
                music = music.split('|')
                for m in music:
                    result = self.get_music_info(m)
                    res = tag + '\t' + result + '\n'
                    save_fp.write(res)
        save_fp.close()


if __name__ == '__main__':
    MS = MusicSpider()
    MS.get_list_id()

