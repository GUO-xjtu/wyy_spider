# -*- coding: utf-8 -*-
# @Time    : 2019-11-09 23:58
# @Author  : GCY
# @FileName: singer_spider.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import requests
import json
from bs4 import BeautifulSoup
from utils.sql_save import MySQLCommand


class SingerSpider(object):
    def __init__(self):
        self.list1 = [1001, 1002, 1003, 2001, 2002, 2003, 6001, 6002, 6003, 7001, 7002, 7003, 4001, 4002, 4003]
        self.list2 = [-1, 0, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]    # initial的值
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
        self.mysqlCommand = MySQLCommand()
        self.mysqlCommand.connectdb()

    # 获取歌手信息
    def get_singer_info(self, artist_id):
        song_dict = dict()  # 歌手热门歌曲字典
        try:
            url = 'https://music.163.com/artist?id=' + artist_id
            r = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(r.text, 'html5lib')
            try:
                singer_homepage = soup.find('a', attrs={'class': 'btn-rz f-tid'})
                singer_homepage = singer_homepage['href'].replace('/user/home?id=', '').strip()
            except:
                singer_homepage = ''
            try:
                song_list = str(soup.find_all('ul', attrs={'class': 'f-hide'}))
                song_list = BeautifulSoup(song_list, 'html5lib')
                song_list = song_list.find_all('a')
            except:
                song_list = []
            for song in song_list:
                song_name = song.string
                song_id = song['href'].replace('/song?id=', '').strip()
                song_dict[song_id] = song_name
            song_dict = str(song_dict)
            song_dict = json.dumps(song_dict)
            return singer_homepage, song_dict
        except:
            return '', json.dumps({})

    # 获取所有歌手
    def get_all_singer(self, url):
        r = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(r.text, 'html5lib')

        for artist in soup.find_all('a', attrs={'class': 'nm nm-icn f-thide s-fc0'}):

            artist_name = artist.string
            artist_id = artist['href'].replace('/artist?id=', '').strip()
            singer_homepage, song_dict = self.get_singer_info(artist_id)
            print(artist_id, artist_name, singer_homepage)
            try:
                self.mysqlCommand.insert_singer(artist_id, artist_name, singer_homepage, song_dict)
            except Exception as msg:
                print(msg)

    # spider主函数
    def spider_main(self):
        print('开始爬取歌手信息...')
        for index, i in enumerate(self.list1):
            for j in self.list2:
                url = 'http://music.163.com/discover/artist/cat?id=' + str(i) + '&initial=' + str(j)
                self.get_all_singer(url)


if __name__ == '__main__':
    SS = SingerSpider()
    SS.spider_main()


