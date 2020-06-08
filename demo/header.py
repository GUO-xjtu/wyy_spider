    provinces = marge_data.groupby(['provinceName', 'updateTime']).aggregate(
        {'cityAdd': np.sum, 'cityCount': np.sum, 'meanTemp': np.mean})
    provinces['diffusion_rate'] = provinces['cityAdd'] / (provinces['cityCount'] - provinces['cityAdd'])
    # 换这部分
    for province_name in marge_data['provinceName'].unique():
        print('---------------------', province_name, '---------------------')
        add_corr = []
        difu_corr = []
        corr = stats.spearmanr(provinces.loc[province_name]['meanTemp'], provinces.loc[province_name]['cityAdd'])
        print(province_name, '新增感染数：', corr[0], corr[1])
        add_corr.append([province_name, corr[0], corr[1]])
        corr = stats.spearmanr(provinces.loc[province_name]['meanTemp'][1:],
                               provinces.loc[province_name]['diffusion_rate'][1:])
        print(province_name, '扩散率：', corr[0], corr[1])
        difu_corr.append([province_name, corr[0], corr[1]])
        province = marge_data[marge_data['provinceName'] == province_name]

        for city in province['cityName'].unique():
            city_data = province[province['cityName'] == city]
            max_add_corr, max_diff_corr = self.city_shift(city, city_data)
            corr = stats.spearmanr(city_data['meanTemp'], city_data['cityAdd'])
            print(city, '新增感染数：', corr[0], corr[1], f'偏移{max_add_corr[0]}天：', max_add_corr[1], max_add_corr[2])
            add_corr.append([city, corr[0], corr[1], max_add_corr[0], max_add_corr[1], max_add_corr[2]])

            corr = stats.spearmanr(city_data['meanTemp'][1:], city_data['diffusion_rate'][1:])
            print(city, '扩散率：', corr[0], corr[1], f'偏移{max_diff_corr[0]}天：', max_diff_corr[1], max_diff_corr[2])
            difu_corr.append([city, corr[0], corr[1], max_diff_corr[0], max_diff_corr[1], max_diff_corr[2]])

        pd.DataFrame(add_corr, columns=['地点', '相关系数', 'p-value', '偏移天数', '偏移的相关系数', '偏移的p-value']).to_csv(
            '/Users/apple/PycharmProjects/WYY_sprider/weather/data/新增感染率/' + province_name + '_新增感染数.csv',
            encoding='utf_8_sig')
        pd.DataFrame(difu_corr, columns=['地点', '相关系数', 'p-value', '偏移天数', '偏移的相关系数', '偏移的p-value']).to_csv(
            '/Users/apple/PycharmProjects/WYY_sprider/weather/data/扩散率/' + province_name + '_扩散率.csv',
            encoding='utf_8_sig')

def city_shift(self, city, city_data):
    add_corr = -5
    diff_corr = -5
    for i in range(7):
        city_data.diffusion_rate = city_data.diffusion_rate.shift(-1)
        city_data = city_data.dropna()
        addcorr = stats.spearmanr(city_data['meanTemp'], city_data['cityAdd'])
        print(f'{city}城市偏移{i}天的Add:', addcorr)
        if add_corr < addcorr[0]:
            add_corr = addcorr[0]
            max_add_corr = [i+1, addcorr[0], addcorr[1]]
        diffcorr = stats.spearmanr(city_data['meanTemp'], city_data['diffusion_rate'])
        print(f'{city}城市偏移{i}天的diff:', addcorr)

        if diff_corr < diffcorr[0]:
            add_corr = diffcorr[0]
            max_diff_corr = [i+1, diffcorr[0], diffcorr[1]]
    print(f'{city}城市偏移{max_add_corr[0]}天的Add：', max_add_corr)
    print(f'{city}城市偏移{max_diff_corr[0]}天的diffusion_rate：', max_diff_corr)
    return max_add_corr, max_diff_corr