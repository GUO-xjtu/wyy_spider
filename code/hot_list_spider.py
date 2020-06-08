# -*- coding: utf-8 -*-
# @Time     : 2020-03-23 11:46
# @Author   : GCY
# @FileName : hot_list_spider.py
# @SoftWare : PyCharm
# @Blog     : https://github.com/GUO-xjtu

import re
import time
import csv
import random
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup


class ListCsv(object):
    def __init__(self):
        self.listid = pd.read_csv('../music_data/hot_list.csv', dtype={'list_id': str})
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
        self.cookie_path = '../data/cookie.txt'
        self.csv_result = []
        self.num = 6
        self.save_num = 0

    def check_headers(self):
        cookie_list = []
        with open(self.cookie_path, 'r') as fp:
            for i in fp.readlines():
                i = json.loads(i)
                cookie_list.append(i)
        self.headers['Cookie'] = random.choice(cookie_list)['cookie']

    def spider_task(self):
        num = 0
        for i in self.listid['list_id']:
            if num >= self.num:
                print('开始爬取第%d个歌单' % num)
                self.list_id(i)
            num += 1
        pd.DataFrame(self.csv_result,
                     columns=['user_id', 'list_name', 'list_id', 'createTime', 'updateTime', 'description',
                              'trackCount', 'playCount',
                              'authority', 'specialType', 'expertTags', 'tags', 'subscribedCount', 'cloudTrackCount',
                              'trackUpdateTime', 'trackNumberUpdateTime', 'highQuality', 'musicId', 'userLikeId',
                              'hotlist']).to_csv('list_csv/hot_list_end.csv', index=None)
        self.save_num = 0
        print('保存hot_list文件成功....')
        self.csv_result = []

    def extract_id(self, list_id, music_id, user_id, simple_list, list_dict):
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
        result = {'id': list_id, 'musicId': mid, 'userLikeId': uid, 'hotlist': lid}
        result_all = {**list_dict, **result}
        self.save_csv(result_all)
        print('==='*30)

    def details_id(self, soup):
        gedan_name=soup.find_all('h2', class_='f-ff2 f-brk')
        list_name=gedan_name[0].string
        print('listName:', list_name)
        gedan_creater=soup.find_all('a', class_='s-fc7')
        user_id=gedan_creater[0]['href'].replace('/user/home?id=', '').strip()
        print('user_id:', user_id)
        gedan_create_time = soup.find_all('span', class_='time s-fc4')
        create_time = gedan_create_time[0].string.encode('utf-8').decode('gbk')
        createTime = create_time[0:10]
        timeArray = time.strptime(createTime, "%Y-%m-%d")
        createTime = int(time.mktime(timeArray))
        print('createtime:', createTime)
        favorite = soup.find_all('a', class_='u-btni u-btni-fav')
        subscribedCount = dict(favorite[0].attrs)['data-count']
        print('subscribedCount:', subscribedCount)
        comment = soup.find_all('span', id='cnt_comment_count')
        comment_count = comment[0].string
        if comment_count == '评论':
            comment_count = '0'
        print('comment_count:', comment_count)
        play = soup.find_all('strong', id='play-count')
        playCount = play[0].string
        print('plauCount:', playCount)
        playlist = soup.find_all('span', id='playlist-track-count')
        trackCount = playlist[0].string
        print('trackCount:', trackCount)
        tag = soup.find_all('div', class_='tags f-cb')
        tag = tag[0].find_all('a', class_='u-tag')
        tags = []
        for i in tag:
            tags.append(i.string)
        print('tags:', tags)
        try:
            description = soup.find_all('p', class_='intr f-brk')
            description = description[0].string
        except:
            description = ''
        print('description:', description)
        return {'list_name': list_name, 'user_id': user_id, 'createTime': createTime, 'subscribedCount': subscribedCount,
                'comment_count': comment_count, 'playCount': playCount, 'trackCount': trackCount, 'tags': tags,
                'description': description, 'expertTags': '', 'updateTime': '', 'authority': '',
                'specialType': '', 'cloudTrackCount': '0', 'trackNumberUpdateTime': '', 'trackUpdateTime': '',
                'highQuality': True}

    def list_id(self, list_id):
        replace = 1
        print('正在爬取ID为%s的歌单' % list_id)
        id = list_id.strip()
        url = 'https://music.163.com/playlist?id=%s' % id
        print('爬取的歌单url为：%s' % url)
        while replace < 15:
            msg = 0
            try:
                if replace > 10:
                    self.check_headers()
                headers = self.headers
                res = requests.get(url, headers=headers)
                time.sleep(replace + 2)
                soup = BeautifulSoup(res.text, 'html5lib')
                list_dict = self.details_id(soup)

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
                        self.extract_id(list_id, music_id, user_id, simple_list, list_dict)
                        break
                    except Exception as e:
                        print('失败！原因是%r' % e)
                        replace += 1
                else:
                    replace += 1

            except Exception as e:
                print('重试！ %r' % e)
                replace += 1
                time.sleep(2)

    def save_csv(self, result_dict):
        result = []
        for key in ['user_id', 'list_name', 'id', 'createTime', 'updateTime', 'description', 'trackCount', 'playCount',
          'authority', 'specialType', 'expertTags', 'tags', 'subscribedCount', 'cloudTrackCount',
          'trackUpdateTime', 'trackNumberUpdateTime', 'highQuality', 'musicId', 'userLikeId', 'hotlist']:
            result.append(result_dict[key])
        self.csv_result.append(result)
        self.save_num += 1
        if self.save_num > 5000:
            pd.DataFrame(self.csv_result, columns=['user_id', 'list_name', 'list_id', 'createTime', 'updateTime', 'description', 'trackCount', 'playCount',
          'authority', 'specialType', 'expertTags', 'tags', 'subscribedCount', 'cloudTrackCount',
          'trackUpdateTime', 'trackNumberUpdateTime', 'highQuality', 'musicId', 'userLikeId', 'hotlist']).to_csv('list_csv/hot_list_%s.csv' % str(result_dict['id']), index=None)
            self.save_num = 0
            print('保存hot_list文件成功....')
            self.csv_result = []


if __name__ == '__main__':
    LC = ListCsv()
    LC.spider_task()
