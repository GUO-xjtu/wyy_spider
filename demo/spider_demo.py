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
import json
from Crypto.Cipher import AES
import base64
from bs4 import BeautifulSoup

def reviewdata_insert():
    keys = ['overall', 'vote', 'verified', 'reviewTime', 'reviewerID', 'asin', 'style', 'reviewerName',
            'reviewText', 'summary', 'unixReviewTime', 'image']
    with open(r"AMAZON_FASHION_5.json", encoding='utf-8') as f:
        all_data = f.readlines()
        for index, line in enumerate(all_data):
            print(u'正在载入第%s行......' % index)
            try:
                review_text = json.loads(line)  # 解析每一行数据
                temp = set(keys) - set(review_text.keys())
                for key in temp:
                    if key == 'vote':
                        review_text[key] = -1
                    else:
                        review_text[key] = ''
                print(type(review_text['style']))
                exit()
                print(review_text['overall'], review_text['vote'], review_text['verified'],
                               review_text['reviewTime'], review_text['reviewerID'], review_text['asin'],
                               str(review_text['style']), review_text['reviewerName'],review_text['reviewText'],
                               review_text['summary'],review_text['unixReviewTime'],review_text['image'])
                inesrt_re = "insert into reviews_test2(id,overall,vote,verified,reviewTime,reviewerID,asin,style,reviewerName,reviewText,summary,unixReviewTime,image) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                # cursor = db.cursor()
                # cursor.executemany(inesrt_re, (index,review_text['overall'], review_text['vote'], review_text['verified'],
                #                review_text['reviewTime'], review_text['reviewerID'], review_text['asin'],
                #                review_text['style'], review_text['reviewerName'],review_text['reviewText'],
                #                review_text['summary'],review_text['unixReviewTime'],review_text['image']))
                # db.commit()
                print('==='*20)
            except Exception as e:
                #db.rollback()
                print('报错:', str(e))
reviewdata_insert()