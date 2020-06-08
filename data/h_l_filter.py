# -*- coding: utf-8 -*-
# @Time     : 2020-03-23 13:48
# @Author   : GCY
# @FileName : h_l_filter.py
# @SoftWare : PyCharm
# @Blog     : https://github.com/GUO-xjtu

import pandas as pd
import os
import gc

list_data = pd.read_csv('../music_list.csv')
old_list_ids = set()
for list_id in list_data['list_id'].values:
    # print(list_id)
    id = str(list_id).strip()
    old_list_ids.add(id)
print(old_list_ids)
print('现有：', len(old_list_ids))
del list_data
gc.collect()

listids = set()
for _, _, file in os.walk('../data/hot_list'):
    for i in file:
        # print('开始合并：', i)
        with open('../data/hot_list/'+i) as fp:
            rows = fp.readlines()
            for row in rows:
                # print(row)
                kind = row.split('=')[1]
                kind = kind.replace('\n', '')
                listids.add(kind)
print(listids)
print('热门未重复歌单长度：', len(listids))
ret = listids-old_list_ids
print('需新增：', len(ret))
print('交集：', old_list_ids & ret)
pd.DataFrame(ret, columns=['list_id']).to_csv('../music_data/hot_list.csv', index=None)

