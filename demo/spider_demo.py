# -*- coding: utf-8 -*-
# @Time    : 2019-11-13 18:45
# @Author  : GCY
# @FileName: spider_demo.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import requests
import random
import math
import re
import codecs
from Crypto.Cipher import AES
import base64
from bs4 import BeautifulSoup



# 获取歌曲详情
def get_music_info(music_id):
    music_dict = {}
    music_dict['music_id'] = music_id
    simple_music = []
    contain_list = []
    headers = {
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
    url = 'https://music.163.com/song?id=%s' % music_id
    response = requests.get(url, headers=headers).text
    soup = BeautifulSoup(response, 'html5lib')
    for info in soup.find_all('p', attrs={'class': 'des s-fc4'}):
        try:
            singer_id = info.find_all('span')[0].find_all('a')[0]['href'].replace('/artist?id=', '').strip()
            music_dict['singer_id'] = singer_id
        except Exception:
            album_id = info.find_all('a')[0]['href'].replace('/album?id=', '').strip()
            music_dict['album_id'] = album_id

    music_list = soup.find_all('ul', attrs={'class': 'm-rctlist f-cb'})[0]
    for info in music_list.find_all('li'):
        playlist = re.findall(r'playlist\?id=(.*?)" title', str(info))[0]
        creator_id = re.findall(r'/user/home\?id=(.*?)" title', str(info))[0]
        contain_list.append({'list': playlist, 'creator': creator_id})
    music_dict['contain_list'] = str(contain_list)
    simple_m = soup.find_all('ul', attrs={'class': 'm-sglist f-cb'})[0]
    for music in simple_m.find_all('li', attrs={'class': 'f-cb'}):
        song_id = re.findall(r'/song\?id=(.*?)" title', str(music))[0]
        singer_id = re.findall(r'/artist\?id=(.*?)">', str(music))[0]
        simple_music.append({'song': song_id, 'singer': singer_id})
    music_dict['simple_music'] = str(simple_music)
    print(music_dict)

get_music_info('1393176534')