# -*- coding: utf-8 -*-
# @Time    : 2020-01-09 17:10
# @Author  : GCY
# @FileName: music_analysis.py
# @Software: PyCharm
# @Blog    ：https://github.com/GUO-xjtu
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


class MusicAnalysis(object):
    def __init__(self):
        self.listid = pd.read_csv('/Users/apple/Desktop/wangyiyun3/music_list.csv', dtype={'music_list': str})
        # lg: 语种, sy: 风格, sc: 场景, em: 情感, tm: 主题
        self.style_dict = {'lg':{'cy': '华语', 'om': '欧美', 'ry': '日语', 'hy': '韩语', 'yy': '粤语', 'xyz': '小语种'},
                           'sy':{'lx':'流行', 'yg': '摇滚', 'my': '民谣', 'dz': '电子', 'wq': '舞曲', 'sc': '说唱',
                                 'qyy': '轻音乐', 'jus': '爵士', 'xc': '乡村', 'soul': 'R&B/Soul', 'gd': '古典', 'mz': '民族',
                                 'yl': '英伦', 'jis': '金属', 'pk': '朋克', 'bd': '蓝调', 'lg': '雷鬼', 'sjyy': '世界音乐',
                                 'ld': '拉丁', 'll': '另类', 'na': 'New Age', 'gf': '古风', 'hy': '后摇', 'bn': 'Bossa Nova'},
                           'sc': {'qc': '清晨', 'yw': '夜晚', 'xx': '学习', 'gz': '工作', 'wx': '午休', 'xwc': '下午茶',
                                  'dt': '地铁', 'jc': '驾车', 'yd': '运动', 'lx': '旅行', 'sb': '散步', 'jb': '酒吧'},
                           'em': {'hj': '怀旧', 'qx': '清新', 'lm': '浪漫', 'xg': '性感', 'sg': '伤感', 'zy': '治愈',
                                  'fs': '放松', 'gd': '孤独', 'qg': '感动', 'xf': '兴奋', 'kl': '快乐', 'aj': '安静', 'sn': '思念'},
                           'tm': {'ysys': '影视原声', 'acg': 'ACG', 'xy': '校园', 'yx': '游戏', '70': '70后', '80': '80后',
                                  '90': '90后', 'wlgq': '网络歌曲', 'ktv': 'KTV', 'jd': '经典', 'fc': '翻唱', 'jt': '吉他',
                                  'gq': '钢琴', 'qy': '器乐', 'et': '儿童', 'bd': '榜单', '00': '00后'}}

    def list_classification(self, frame, save=False):
        # 语种保存
        lg = '|'.join(self.style_dict['lg'].values())
        lg_list = frame[frame['tags'].str.contains(lg)]

        # 风格保存
        sy = '|'.join(self.style_dict['sy'].values())
        sy_list = frame[frame['tags'].str.contains(sy)]

        # 场景保存
        sc = '|'.join(self.style_dict['sc'].values())
        sc_list = frame[frame['tags'].str.contains(sc)]

        # 主题保存
        tm = '|'.join(self.style_dict['tm'].values())
        tm_list = frame[frame['tags'].str.contains(tm)]

        # 情感保存
        em = '|'.join(self.style_dict['em'].values())
        em_list = frame[frame['tags'].str.contains(em)]

        if save:
            lg_list.to_csv('../data/csv_file/lg.csv')
            sy_list.to_csv('../data/csv_file/sy.csv')
            sc_list.to_csv('../data/csv_file/sc.csv')
            tm_list.to_csv('../data/csv_file/tm.csv')
            em_list.to_csv('../data/csv_file/em.csv')
        return lg_list, sy_list, sc_list, tm_list, em_list

    def data_classification(self):
        # 按月份分组计算歌单创建量
        self.listid['createtime'] = self.listid['createtime'].apply(lambda x: pd.to_datetime(time.strftime('%Y/%m/%d', time.localtime(float(x/1000)))))
        #month_group = self.listid.loc[:, ['music_list', 'createtime']]
        #month_group = month_group.groupby(self.listid['createtime'].apply(lambda x:x.month)).agg('count')
        #self.bar_plot(month_group.index, month_group['music_list'], '每月的歌单创建量', 'month_create', '月份', '歌单量')
        # 按月份分组计算播放量、收藏量和歌曲数
        month_group = self.listid.groupby(self.listid['createtime'].apply(lambda x: x.year)).agg({'playcount': 'sum', 'trackcount': 'sum', 'subscribedcount': 'sum'})
        print(month_group)
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 设置中文字体
        fig = plt.figure()
        ax1 = fig.add_subplot(111)
        ax1.plot(month_group.index, month_group['trackcount'], 'or-', color='b', label='收藏量')
        ax1.plot(month_group.index, month_group['subscribedcount'], 'or-', color='r', label='歌曲数')
        ax1.legend(loc=2)
        ax1.set_ylabel('收藏/歌曲数')
        ax2 = ax1.twinx()
        plt.bar(month_group.index, month_group['playcount'], alpha=0.7, label='播放量')
        ax2.set_ylabel('播放数')
        ax2.legend(loc=1)
        plt.xticks(month_group.index)
        plt.title('按年份分组的歌单播放、收藏与歌曲数', fontsize=15, color='black')
        plt.xlabel('年份', fontsize=10, color='black')
        plt.savefig('../data/image/' + 'year_group.png')
        plt.show()
        exit()
        # 按歌曲数分组计算歌单数
        track_group = self.listid.groupby(['trackcount']).agg({'music_list': lambda x: len(x)})
        track_group = track_group.drop(labels=[0])  # 删除没有歌曲的歌单
        track_group = track_group[track_group['music_list'] > 100]
        self.bar_plot(track_group.index, track_group['music_list'], '歌单单曲存放量分布', 'track_group', '存放量', '歌单数', text=False)
        track_group = track_group[['music_list']].nlargest(50, 'music_list')
        self.bar_plot(track_group.index, track_group['music_list'], '歌单单曲存放量分布(top50)', 'track_group_top50', '存放量', '歌单数', text=False)
        labels = ['语种', '风格', '场景', '主题', '情感']
        # 按收藏量排序取前100
        sub_list = self.listid.sort_values('subscribedcount', ascending=False).head(100)
        lg_sub, sy_sub, sc_sub, tm_sub, em_sub = self.list_classification(sub_list)
        # 饼状图分布
        sizes = [len(lg_sub), len(sy_sub), len(sc_sub), len(tm_sub), len(em_sub)]
        self.pie_plot(labels, sizes, '歌单收藏量前100导航类数量分布', 'sub_navigation.jpg')
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for frame in [lg_sub, sy_sub, sc_sub, tm_sub, em_sub]:
            trackcount.append(frame['trackcount'].agg('sum'))
            playcount.append(frame['playcount'].agg('sum'))
            subcount.append(frame['subscribedcount'].agg('sum'))
        print('歌单导航数量: ', sizes)
        print('歌曲数: ', trackcount)
        print('播放量：', playcount)
        print('收藏量：', subcount)
        self.bar_plot(labels, trackcount, '收藏量前100导航类 歌曲数', 'sub_track.png')
        self.bar_plot(labels, playcount, '收藏量前100类 播放量', 'sub_play.png')
        self.bar_plot(labels, subcount, '收藏量前100类 收藏数', 'sub_sub.png')

        # 按播放量排序取前100
        play_list = self.listid.sort_values('playcount', ascending=False).head(100)
        lg_play, sy_play, sc_play, tm_play, em_play = self.list_classification(play_list)
        # 饼状图分布
        sizes = [len(lg_play), len(sy_play), len(sc_play), len(tm_play), len(em_play)]
        self.pie_plot(labels, sizes, '歌单播放量前100导航类数量分布', 'play_navigation.jpg')
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for frame in [lg_play, sy_play, sc_play, tm_play, em_play]:
            trackcount.append(frame['trackcount'].agg('sum'))
            playcount.append(frame['playcount'].agg('sum'))
            subcount.append(frame['subscribedcount'].agg('sum'))
        print('歌单导航数量: ', sizes)
        print('歌曲数: ', trackcount)
        print('播放量：', playcount)
        print('收藏量：', subcount)
        self.bar_plot(labels, trackcount, '播放量前100导航类 歌曲数', 'play_track.png')
        self.bar_plot(labels, playcount, '播放量前100类 播放量', 'play_play.png')
        self.bar_plot(labels, subcount, '播放量前100类 收藏数', 'play_sub.png')

        play_sub = pd.merge(play_list, sub_list, how='inner', on='music_list').loc[:, ['music_list', 'name_y', 'playcount_y', 'trackcount_y', 'subscribedcount_y']]
        print('播放量and收藏量\n', play_sub)
        # 按歌曲数排序取前100
        track_list = self.listid.sort_values('trackcount', ascending=False).head(100)
        lg_track, sy_track, sc_track, tm_track, em_track = self.list_classification(track_list)
        # 饼状图分布
        sizes = [len(lg_track), len(sy_track), len(sc_track), len(tm_track), len(em_track)]
        self.pie_plot(labels, sizes, '歌单歌曲数前100导航类数量分布', 'track_navigation.jpg')
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for frame in [lg_track, sy_track, sc_track, tm_track, em_track]:
            trackcount.append(frame['trackcount'].agg('sum'))
            playcount.append(frame['playcount'].agg('sum'))
            subcount.append(frame['subscribedcount'].agg('sum'))
        print('歌单导航数量: ', sizes)
        print('歌曲数: ', trackcount)
        print('播放量：', playcount)
        print('收藏量：', subcount)
        self.bar_plot(labels, trackcount, '歌曲数前100导航类 歌曲数', 'track_track.png')
        self.bar_plot(labels, playcount, '歌曲数前100类 播放量', 'track_play.png')
        self.bar_plot(labels, subcount, '歌曲数前100类 收藏数', 'track_sub.png')

        lg_list, sy_list, sc_list, tm_list, em_list = self.list_classification(self.listid, True)
        # 饼状图分布
        sizes = [len(lg_list), len(sy_list), len(sc_list), len(tm_list), len(em_list)]
        self.pie_plot(labels, sizes, '网易云音乐歌单导航类数量分布', 'list_navigation.jpg')
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for frame in [lg_list, sy_list, sc_list, tm_list, em_list]:
            trackcount.append(frame['trackcount'].agg('sum'))
            playcount.append(frame['playcount'].agg('sum'))
            subcount.append(frame['subscribedcount'].agg('sum'))
        print('歌单导航数量: ', sizes)
        print('歌曲数: ', trackcount)
        print('播放量：', playcount)
        print('收藏量：', subcount)
        self.bar_plot(labels, trackcount, '歌单导航类 歌曲数', 'all_track.png')
        self.bar_plot(labels, playcount, '歌单导航类 播放量', 'all_play.png')
        self.bar_plot(labels, subcount, '歌单导航类 收藏数', 'all_sub.png')

    def data_detail(self):
        lg_frame = pd.read_csv('../data/csv_file/lg.csv')
        sy_frame = pd.read_csv('../data/csv_file/sy.csv')
        sc_frame = pd.read_csv('../data/csv_file/sc.csv')
        tm_frame = pd.read_csv('../data/csv_file/tm.csv')
        em_frame = pd.read_csv('../data/csv_file/em.csv')

        # 语种
        labels = self.style_dict['lg'].values()
        sizes = []  # 每个类别的数量
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量

        for i in labels:
            lg_i_frame = lg_frame[lg_frame['tags'].str.contains(i)]
            trackcount.append(lg_i_frame['trackcount'].agg('sum'))
            playcount.append(lg_i_frame['playcount'].agg('sum'))
            subcount.append(lg_i_frame['subscribedcount'].agg('sum'))
            sizes.append(len(lg_i_frame))
        print('语种: ', sizes)
        print('歌曲数: ', trackcount)
        print('播放量：', playcount)
        print('收藏量：', subcount)
        print('==='*30)
        self.pie_plot(labels, sizes, '歌单语种类数量分布', 'language.jpg')
        self.bar_plot(labels, trackcount, '语种类 歌曲数', 'lg_track.png')
        self.bar_plot(labels, playcount, '语种类 播放量', 'lg_play.png')
        self.bar_plot(labels, subcount, '语种类 收藏数', 'lg_sub.png')

        # 风格
        labels = self.style_dict['sy'].values()
        sizes = []
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for i in labels:
            sy_i_frame = sy_frame[sy_frame['tags'].str.contains(i)]
            trackcount.append(sy_i_frame['trackcount'].agg('sum'))
            playcount.append(sy_i_frame['playcount'].agg('sum'))
            subcount.append(sy_i_frame['subscribedcount'].agg('sum'))
            sizes.append(len(sy_i_frame))
        print('风格: ', sizes)
        self.pie_plot(labels, sizes, '歌单风格类数量分布', 'style.jpg')
        self.bar_plot(labels, trackcount, '风格类 歌曲数', 'sy_track.png')
        self.bar_plot(labels, playcount, '风格类 播放量', 'sy_play.png')
        self.bar_plot(labels, subcount, '风格类 收藏数', 'sy_sub.png')

        # 场景
        labels = self.style_dict['sc'].values()
        sizes = []
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for i in labels:
            sc_i_frame = sc_frame[sc_frame['tags'].str.contains(i)]
            trackcount.append(sc_i_frame['trackcount'].agg('sum'))
            playcount.append(sc_i_frame['playcount'].agg('sum'))
            subcount.append(sc_i_frame['subscribedcount'].agg('sum'))
            sizes.append(len(sc_i_frame))
        print('场景: ', sizes)
        self.pie_plot(labels, sizes, '歌单场景类数量分布', 'scenes.jpg')
        self.bar_plot(labels, trackcount, '场景类 歌曲数', 'sc_track.png')
        self.bar_plot(labels, playcount, '场景类 播放量', 'sc_play.png')
        self.bar_plot(labels, subcount, '场景类 收藏数', 'sc_sub.png')

        # 主题
        labels = self.style_dict['tm'].values()
        sizes = []
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for i in labels:
            tm_i_frame = tm_frame[tm_frame['tags'].str.contains(i)]
            trackcount.append(tm_i_frame['trackcount'].agg('sum'))
            playcount.append(tm_i_frame['playcount'].agg('sum'))
            subcount.append(tm_i_frame['subscribedcount'].agg('sum'))
            sizes.append(len(tm_i_frame))
        print('主题: ', sizes)
        self.pie_plot(labels, sizes, '歌单主题类数量分布', 'theme.jpg')
        self.bar_plot(labels, trackcount, '主题类 歌曲数', 'tm_track.png')
        self.bar_plot(labels, playcount, '主题类 播放量', 'tm_play.png')
        self.bar_plot(labels, subcount, '主题类 收藏数', 'tm_sub.png')

        # 情感
        labels = self.style_dict['em'].values()
        sizes = []
        trackcount = []  # 歌曲数
        playcount = []  # 播放量
        subcount = []  # 收藏量
        for i in labels:
            em_i_frame = em_frame[em_frame['tags'].str.contains(i)]
            trackcount.append(em_i_frame['trackcount'].agg('sum'))
            playcount.append(em_i_frame['playcount'].agg('sum'))
            subcount.append(em_i_frame['subscribedcount'].agg('sum'))
            sizes.append(len(em_i_frame))
        print('情感: ', sizes)
        self.pie_plot(labels, sizes, '歌单情感类数量分布', 'emotion.jpg')
        self.bar_plot(labels, trackcount, '情感类 歌曲数', 'em_track.png')
        self.bar_plot(labels, playcount, '情感类 播放量', 'em_play.png')
        self.bar_plot(labels, subcount, '情感类 收藏数', 'em_sub.png')

    def pie_plot(self, labels, sizes, title, name):
        ln = len(sizes)
        explode = [0.01] * ln  # 设置分离的距离
        print(ln//2)
        size = sorted(sizes)[:ln//2]
        for index, i in enumerate(sizes):
            if i in size:
                explode[index] = 0.3
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 设置中文字体
        plt.title(title, fontsize=18, color='black')
        plt.pie(sizes, labels=labels, explode=explode, shadow=False, pctdistance=0.8, startangle=90, autopct='%1.1f%%')
        plt.axis('equal')  # 保证为正圆形
        plt.savefig('../data/image/' + name)
        plt.show()

    def bar_plot(self, lables, count, title, name, xlabel='类别', ylable='数量', text=True):
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 设置中文字体
        params = {'figure.figsize': '11, 6'}
        plt.rcParams.update(params)
        plt.bar(lables, count, color=['r', 'g', 'y', 'b', 'pink', 'cyan', 'brown', 'lightgrey', 'steelblue', 'teal'])
        if text:
            for a, b in zip(lables, count):
                plt.text(a, b + 0.05, '%.0f' % b, ha='center', va='bottom', fontsize=8)
        plt.xticks(rotation=60, fontsize=12)
        plt.title(title, fontsize=20, color='black')
        plt.xlabel(xlabel, fontsize=18, color='black')
        plt.ylabel(ylable, fontsize=18, color='black')
        plt.savefig('../data/image/' + name)
        plt.show()



if __name__ == '__main__':
    MA = MusicAnalysis()
    MA.data_classification()
    # MA.data_detail()
