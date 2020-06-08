# -*- coding: utf-8 -*-
# @Time    : 2020-01-06 10:31
# @Author  : GCY
# @FileName: demo.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import pandas as pd

list_data = pd.read_csv('music_list.csv')
l_m_id = list_data['musicId']
lmid = set()
for i in l_m_id.values:
    i = i.split(',')
    for j in i[:-1]:
        j = str(j)
        lmid.add(j)

print('歌单中有歌曲%d首' % len(lmid))

music_data = pd.read_csv('music.csv')
mid = set()
m_id = music_data['music_id']
for i in m_id.values:
    i = str(i)
    mid.add(i)

print('已有歌曲%d首' % len(mid))

new_mid = list(lmid.difference(set(mid)))# b中有而a中没有的

print('还需要爬去的歌曲%d首' % len(new_mid))

pd.DataFrame(new_mid, columns=['music_id']).to_csv('new_m_id.csv')
