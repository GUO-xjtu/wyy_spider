# -*- coding: utf-8 -*-
# @Time    : 2019-12-25 19:13
# @Author  : GCY
# @FileName: list_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import re
import time
import json
import requests
import multiprocessing as mp
from bs4 import BeautifulSoup
from threading import Thread
from utils.sql_save import MySQLCommand


class ListMusicIDs():
    def __init__(self):
        self.file_path = '../data/'  # 用户信息保存位置
        self.headers = {
            'accept-encoding': 'gzip, deflate, br',
            'cookie': '_iuqxldmzr_=32; _ntes_nnid=1ea4c0a3bf7ef4709cbd1cc9344aac39,1552463572427; _ntes_nuid=1ea4c0a3bf7ef4709cbd1cc9344aac39; WM_TID=0sJ%2Br5BotwpEERVAARN89S2kZb9zjMn5; ntes_kaola_ad=1; WM_NI=VdwnwgpqLzjKaXxywyfT%2BpbMp0h4a7mPlXF%2FIMEPmTkfdB%2FOfXvCgYpaJ%2FD7hCYKNSZtJuMDzO2DfcqcQUwdSx1jraMcuOeY2UZexUHFqxYFTOeAy5%2B07TOBtOJ1HpvadVA%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eeadea4a90e9b88ed95df18a8fb7c54e839f8faeb87abcaba894e240ad978883d82af0fea7c3b92a96bcb98ff66db4af849be941a388b89af673a68ce591e147b4f0a8d5e43daf90a4b9c5258b938bb1f060a9baaca3ae45a688baa4e653b4bf86d0c73dac9d97accd599aac8696d050ba91fea5e569b3bd9fb6f67091a8ff8bf16b8fbdbbb0e853f28b9686fb5f8c9abf99dc4e9891aba2fc3fe9f0f9b5ce4ab89186d2cb7cb6bfacb7c437e2a3; JSESSIONID-WYYY=UcRlS8UIuN%5C45yS73geuzr1%5C3ZCp2Q9ZU9nAcbhSuPAFpa3h9o%2BHDM3QeGhoxqT5R0xAOF0IJXTFMMwMt8vHjDk1e0%2BOR5CrysQpRJodDlPrjTDxIwEIs2H2cddBrM%2B3VsC22BQJvcrUgUByjDh6%2FJrqeO3J%2B58db7juzAGfQ15oRB1f%3A1577276498310',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'no-cache',
            'sec-fetch-mode': 'nested-navigate',
            'sec-fetch-site': 'same-origin',
            'referer': 'https://music.163.com/',
            'upgrade-insecure-requests': '1',
            'sec-fetch-user': '?1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/66.0.3359.181 Safari/537.36'
        }
        self.id_queue = mp.Queue()  # 歌单队列

    def extract_id(self, list_id, music_id, user_id, simple_list):
        mid, uid, lid = '', '', ''
        m_num, u_num, l_num = 0, 0, 0
        for id in music_id:
            id = id.find_all('a')[0]['href'].replace('/song?id=', '').strip()
            mid += id
            mid += ','
            m_num += 1
        for id in user_id:
            id = id.find_all('a')[0]['href'].replace('/user/home?id=', '').strip()
            uid += id
            uid += ','
            u_num += 1
        for id in simple_list:
            ids = id.find_all('a')
            try:
                listId = ids[0]['href'].replace('/playlist?id=', '').strip()
                l_num += 1
            except:
                listId = ''
            try:
                user_id = ids[1]['href'].replace('/user/home?id=', '').strip()
            except:
                user_id = ''
            list_str = listId + ':' + user_id
            lid += list_str
            lid += ','
        print('歌单%s有歌曲%d首，喜欢的人%d个，热门歌单%d个' % (list_id, m_num, u_num, l_num))
        self.id_queue.put({'id': list_id, 'musicId': mid, 'userLikeId': uid, 'hotlist': lid})

    def list_id(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        mysql_command.cursor.execute("select id, userLikeId from music_list")
        list_ids = mysql_command.cursor.fetchall()

        for id in list_ids:
            userids = id.get('userLikeId')
            list_id = id.get('id')

            if userids is not None and len(userids) > 10:
                print('ID为%s的歌单已经更新' % list_id)
                continue
            if len(list_id) > 0:
                replace = 1
                print('正在爬取ID为%s 的歌单' % list_id)
                id = list_id.strip()
                url = 'https://music.163.com/playlist?id=%s' % id
                print('爬取的歌单url为：%s' % url)
                while replace < 10:
                    msg = 0
                    try:
                        headers = self.headers
                        res = requests.get(url, headers=headers)
                        time.sleep(replace + 5)
                        soup = BeautifulSoup(res.text, 'html5lib')
                        try:
                            music = soup.find('ul', attrs={'class': 'f-hide'})
                            music_id = music.find_all('li')
                        except Exception as e:
                            music_id = []
                            msg += 1
                            print('ID为%s的歌单没有歌曲！原因是%s' % (list_id, e))
                        try:
                            user = soup.find('ul', attrs={'class': 'm-piclist f-cb'})
                            user_id = user.find_all('li')
                        except Exception as e:
                            user_id = []
                            msg += 1
                            print('ID为%s的歌单没有喜欢的用户！原因是%s' % (list_id,  e))
                        try:
                            simple_list = soup.findAll('div', attrs={'class': 'info'})
                        except Exception as e:
                            simple_list = []
                            msg += 1
                            print('ID为%s的歌单没有相关推荐或热门歌单！原因是%s' % (list_id, e))
                        if msg < 2:
                            try:
                                self.extract_id(list_id, music_id, user_id, simple_list)
                                break
                            except Exception as e:
                                print('失败！原因是%r' % e)
                                replace += 1
                        else:
                            replace += 2
                    except Exception as e:
                        print('重试！ %r' % e)
                        replace += 1
                        time.sleep(2)

    def save_sql(self):
        mysql_command = MySQLCommand()
        mysql_command.connectdb()
        while True:
            ids = self.id_queue.get()
            print('数据为：\n', ids)
            mysql_command.update_list(ids)

    def start_main(self):
        Thread(target=self.list_id, args=()).start()
        Thread(target=self.save_sql, args=()).start()


if __name__ == '__main__':
    LMI = ListMusicIDs()
    LMI.start_main()
