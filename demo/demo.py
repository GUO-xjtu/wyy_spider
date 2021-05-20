# -*- coding: utf-8 -*-
# @Time    : 2020-01-06 10:31
# @Author  : GCY
# @FileName: demo.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import pandas as pd
from tqdm import tqdm


class SingerSpider(object):
    def __init__(self):
        train_data = pd.read_csv('/home/cyguo/CF_Tag/data/train.csv')
        test_data = pd.read_csv('/home/cyguo/CF_Tag/data/test.csv')

        self.all_data = pd.concat([train_data, test_data])

        self.music_data = pd.read_csv('/home/cyguo/CF_Tag/data/music.csv')
        self.save_data = '/home/cyguo/CF_Tag/data/music_spider.csv'

    def no_singer(self):
        music_ids = self.all_data['musicId']

        no_singer_list = set()
        has_id = self.music_data['歌曲ID'].agg({'music_id': lambda k: str(k).strip()}).values.tolist()
        print(has_id[1], type(has_id[1]))
        m_list = list()
        for m_ids in tqdm(music_ids):
            mid = m_ids.split(',')[:-1]
            m_list.extend(mid)
        print('m_list:', len(m_list))
        m_set =set(m_list)
        print('m_set:', len(m_set))

        for i in tqdm(m_set):
            if i not in has_id:
                no_singer_list.add(i)

        print('需要爬取的歌曲：', len(no_singer_list))


        pd.DataFrame(list(no_singer_list), columns=['music_id']).to_csv(self.save_data)




if __name__ == '__main__':
    ss = SingerSpider()
    ss.no_singer()
