
####################################
import sys
import os
import platform
#################################
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import csv
import geohash2
import json
import datetime
from geopy.distance import geodesic
# from scipy import stats
from scipy import sparse
from tqdm import tqdm
from random import shuffle
from .util import dir_check, write_list_list


# 随机选取k个相邻的运营区
def get_adjacent_blocks(fin='', k=10):
    df = pd.read_csv(fin, sep=',', encoding='utf-8', header=None, names=['日期', '运营区id', '城市', '快递员id',
                                                                         '接单时间', '预约时间1', '预约时间2', '订单经度', '订单纬度',
                                                                         '订单所属区块id', '区块类型id', '区块类型', '订单揽收时间',
                                                                         '揽收最近时间', '揽收最近经度', '揽收最近纬度',
                                                                         '揽收轨迹精度', '接单最近时间', '接单最近经度', '接单最近纬度',
                                                                         '接单轨迹精度'])
    t = df[['运营区id', '订单经度', '订单纬度']].groupby(df['运营区id']).mean()
    dis = np.zeros((t.shape[0], 4))
    dis[:, :3] = t.values
    idx = np.random.randint(0, t.shape[0])
    lat = dis[idx, 2]
    lon = dis[idx, 1]
    for nn in range(t.shape[0]):
        dis[nn, 3] = geodesic((lat, lon), (dis[nn, 2], dis[nn, 1])).meters
    dis = dis[np.argsort(dis[:, 3])]
    blocks = [int(x) for x in dis[:k, 0]]
    return blocks

# 字符串时间转分钟


def time2min(t):
    y, M, d = t.split(' ')[0].split('-')
    h, m, s = t.split(' ')[1].split(':')
    return d, 60*int(h)+int(m)+int(s)/60

# 获取对应列名的 index


def idx(df, col_name):
    _idx_ = list(df.columns).index(col_name)
    return _idx_

# 将数据划分为若干快递员的轨迹


def split_trace(df):
    courier_l = []  #
    temp1 = df.values[0]
    id_idx = list(df.columns).index('快递员id')
    f = 0
    t = 0
    for i in df.values:
        if i[id_idx] != temp1[id_idx]:
            courier_l.append(df[f:t])
            f = t
        t = t + 1
        temp1 = i
    courier_l.append(df[f:t])
    return courier_l

# 剔除离群值
# def drop_unnormal(df):
#     keep=[]
#     out_cnt, short_cnt, similar_cnt= 0, 0, 0
#
#     courier_l=split_trace(df)
#     for c in courier_l:
#         c_v=c.values
#         for n in range(c.shape[0]):
#             if c.shape[0]<=5: ##轨迹内不多于五单直接舍弃
#                 keep.append(False)
#                 short_cnt+=1
#                 continue
#             if n!=0 and c_v[n][-2]==0 and c_v[n][-7]<1:##一分钟内不移动则认为是一单，其余舍弃
#                 keep.append(False)
#                 similar_cnt+=1
#                 continue
#             if n<c.shape[0]-1:
#                 if c_v[n][-7]!=0 and c_v[n+1][-7]!=0:
#                     if c_v[n][-2]/c_v[n][-7]>500 and c_v[n+1][-2]/c_v[n+1][-7]>500:##与前后两单都有较大偏差，舍弃
#                         keep.append(False)
#                         out_cnt+=1
#                         continue
#             keep.append(True)
#     print('轨迹过短剔除{}单 数据冗余剔除{}单 gps漂移剔除{}单'.format(short_cnt,similar_cnt,out_cnt))
#     return df[keep]


def drop_unnormal(df, fout):
    # 删除gps漂移点
    keep = []
    out_cnt = 0
    courier_l = split_trace(df)
    for c in courier_l:
        c_v = c.values
        for n in range(c.shape[0]):
            if n < c.shape[0] - 1:
                if c_v[n][-7] != 0 and c_v[n + 1][-7] != 0:
                    # 与前后两单都有较大偏差，舍弃
                    if c_v[n][-2] / c_v[n][-7] > 500 and c_v[n + 1][-2] / c_v[n + 1][-7] > 500:
                        keep.append(False)
                        out_cnt += 1
                        continue
            keep.append(True)
    df = df[keep]

    # 统计快递员信息
    couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order = courier_info(
        df)

    # 确定需要剔除的快递员
    # 工作少于5天的快递员数据不要
    rmv_wd = set(filter(lambda c: work_days[c] < 5, couriers))
    # 每日平均移动距离少于50m的不要
    rmv_dmd = set(filter(lambda c: dis_mean_day[c] < 50, couriers))
    # 每日平均单数少于3单的不要
    rmv_omd = set(filter(lambda c: order_mean_day[c] < 5, couriers))
    # 每两单之间的时间小于3min的不要
    rmv_tmo = set(filter(lambda c: time_mean_order[c] < 5, couriers))
    # 每两单之间的平均距离少于20m的不要
    rmv_dmo = set(filter(lambda c: dis_mean_order[c] < 50, couriers))
    remove_c = rmv_wd & rmv_dmd & rmv_omd & rmv_tmo & rmv_dmo

    # 剔除快递员数据
    keep = []
    similar_cnt, remove_cnt = 0, 0
    courier_l = split_trace(df)
    cid_idx = idx(df, '快递员id')
    for c in courier_l:
        c_v = c.values
        for n in range(c.shape[0]):
            if c_v[n][cid_idx] in remove_c:  # 去除对应快递员数据
                keep.append(False)
                remove_cnt += 1
                continue
            if n != 0 and c_v[n][-2] == 0 and c_v[n][-7] < 1:  # 一分钟内不移动则认为是一单，其余舍弃
                keep.append(False)
                similar_cnt += 1
                continue
            keep.append(True)

    str_ = f'去除经纬度漂移单{out_cnt}, 过滤快递员数/总快递员数:{len(remove_c)}/{len(couriers)}, 过滤单数{remove_cnt}, 数据冗余剔除{similar_cnt}单'
    print(str_)
    write_list_list(fout + '/data_info.txt', [str_], 'w')

    report_df = pd.DataFrame({'快递员过滤条件': ['工作少于5天', '每日平均移动距离少于50m', '每日平均单数少于3单', '每两单之间的时间小于3min', '每两单之间的平均距离少于20m'],
                              '过滤数量': [len(x & remove_c) for x in [rmv_wd, rmv_dmd, rmv_omd, rmv_tmo, rmv_dmo]],
                              '比例': [len(x & remove_c) / len(remove_c) for x in [rmv_wd, rmv_dmd, rmv_omd, rmv_tmo, rmv_dmo]]
                              })
    print('剔除快递员具体情况：\n', report_df)

    return df[keep], (couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order)


# 列表转字符串
def list2str(l):
    s = ''
    if l == []:
        return s
    for ll in l:
        s += str(ll)+'.'
    return s[:-1]

# 字符串转列表


def str2list(s):
    if s == '':
        return []
    l = []
    sl = s.split('.')
    for i in sl:
        l.append(int(i))
    return l

# 统计每一时刻待揽收的订单编号及数量


def get_unpick(df):
    rem = []
    rem_cnt = []
    courier_l = split_trace(df)
    for c in courier_l:
        c_v = c.values
        for n in range(c.shape[0]):
            now_time = c_v[n][2]  # @ 这里的2，太依赖前面的代码了，如果揽件时间的index不是2呢？
            now_id = c_v[n][0]
            got = c_v[:, 0][c_v[:, 3] < now_time]
            got_unpick = got[got > now_id]
            lst_unpick = list(got_unpick)
            # shuffle(lst_unpick)
            rem.append(list2str(lst_unpick))
            rem_cnt.append(got_unpick.shape[0])
    return rem, rem_cnt

# 统计快递员信息
# 统计快递员信息


def courier_info(df):
    couriers = list(set(df['快递员id']))
    order_sum = {}  # 快递员总单数
    dis_sum = {}  # 快递员总距离
    work_days = {}  # 快递员工作天数
    order_mean_day = {}  # 快递员平均每日单数
    dis_mean_day = {}  # 快递员平均每日总直线距离
    time_mean_order = {}  # 快递员平均两单间隔时间
    dis_mean_order = {}  # 快递员平均两单间隔距离
    for courier in couriers:
        work_days[courier] = len(set(df[df['快递员id'] == courier]['日期']))
        dis_sum[courier] = sum(df[df['快递员id'] == courier]['与前一单直线距离'])
        dis_mean_day[courier] = dis_sum[courier]/work_days[courier]
        order_sum[courier] = df[df['快递员id'] == courier].shape[0]
        order_mean_day[courier] = order_sum[courier]/work_days[courier]
        time_mean_order[courier] = np.mean(
            df[df['快递员id'] == courier]['时间差单位化'])
        dis_mean_order[courier] = np.mean(
            df[df['快递员id'] == courier]['与前一单直线距离'])
    return couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order


# 全局量，空间换时间，加快距离计算速度
dic_dis_cal = {}


def distance(fro, to, df_v):
    global dic_dis_cal
    if (str(fro)+'-'+str(to)) in dic_dis_cal:
        return dic_dis_cal[str(fro)+'-'+str(to)]
    else:
        dis_temp = int(geodesic(
            (df_v[fro-1][11], df_v[fro-1][10]), (df_v[to-1][11], df_v[to-1][10])).meters)
        dic_dis_cal[str(fro)+'-'+str(to)] = dis_temp
        return dic_dis_cal[str(fro)+'-'+str(to)]

# 获取距离贪心的待揽收集合顺序


def greedy_sort(sta, points, df_v):
    ans = ''
    while len(points) > 0:
        temp_id = points[0]
        temp_dis_min = distance(sta, int(points[0]), df_v)
        for nn in range(1, len(points)):
            d = distance(sta, int(points[nn]), df_v)
            if d < temp_dis_min:
                temp_id = points[nn]
                temp_dis_min = d
        sta = temp_id
        ans += str(sta)+'.'
        points.remove(temp_id)
    return ans[:-1]


def pre_process(fin, fout, is_test=False):
    print('原始数据：' + fin)
    print('中继文件输出目录：' + fout)
    nrows = 50000 if is_test else None
    df = pd.read_csv(fin, sep=',', encoding='utf-8', header=None, names=['日期', '运营区id', '城市', '快递员id',
                                                                         '接单时间', '预约时间1', '预约时间2', '订单经度', '订单纬度',
                                                                         '订单所属区块id', '区块类型id', '区块类型', '订单揽收时间',
                                                                         '揽收最近时间', '揽收最近经度', '揽收最近纬度',
                                                                         '揽收轨迹精度', '接单最近时间', '接单最近经度', '接单最近纬度',
                                                                         '接单轨迹精度'], nrows=nrows)
    print('初始文件读入完成')
    df = df.drop_duplicates()  # 原始数据中有重复的行，需要去掉
    df = df.reset_index(drop=True)

    # 排序
    df = df.sort_values(by=['日期', '快递员id', '订单揽收时间'])

    # 剔除离群值
    # df = df[df['城市'] == '上海市']

    # 扩充基本信息
    print('开始数据转化及扩充')
    pbar = tqdm(total=df.shape[0])
    courier_l = split_trace(df)
    got_time = []
    got_time_shift = []
    book_time = []
    lon_shift = []
    lat_shift = []
    dis_dir = []
    early_time = []
    for c in courier_l:
        c_v = c.values
        for n in range(c.shape[0]):
            pbar.update(1)
            d1, t1 = time2min(c_v[n][12])
            d2, t2 = time2min(c_v[n][4])
            if (str(c_v[n][5])) == 'nan':
                t3 = 1440
            else:
                d3, t3 = time2min(c_v[n][6])
            if n == 0:
                gts = t1
                lon_ = c_v[n][7]
                lat_ = c_v[n][8]
            else:
                gts = got_time[-1]
                lon_ = c_v[n - 1][7]
                lat_ = c_v[n - 1][8]

            if d2 != d1:  # 前一天下的单
                t2 = t2 - 60 * 24
            lon_shift.append(lon_)
            lat_shift.append(lat_)
            dis_dir.append(
                int(geodesic((lat_, lon_), (c_v[n][8], c_v[n][7])).meters))
            got_time.append(t1)
            got_time_shift.append(gts)
            book_time.append(t2)
            early_time.append(t3)
    pbar.close()
    pad = [0 for n in range(df.shape[0])]
    df.insert(1, '揽收时间1', got_time)
    df.insert(2, '接单时间1', book_time)
    df.insert(df.shape[1], '上一经度', lon_shift)
    df.insert(df.shape[1], '上一纬度', lat_shift)
    df['经度差'] = df['订单经度'].astype(float) - df['上一经度'].astype(float)
    df['纬度差'] = df['订单纬度'].astype(float) - df['上一纬度'].astype(float)
    df.insert(df.shape[1], '高德距离（未启用）', pad)
    df.insert(df.shape[1], '高德预计时间（未启用）', pad)
    df.insert(df.shape[1], '上一揽收时间', got_time_shift)
    df.insert(df.shape[1], '上一时间差（未启用）', pad)
    df['时间差单位化'] = df['揽收时间1'] - df['上一揽收时间']
    df.insert(df.shape[1], '高德距离当天累计（未启用）', pad)
    df.insert(df.shape[1], '高德距离/总距离（未启用）', pad)
    df.insert(df.shape[1], '高德距离平均总距离（未启用）', pad)
    df.insert(df.shape[1], '高德距离/平均总距离（未启用）', pad)
    df.insert(df.shape[1], '与前一单直线距离', dis_dir)
    df.insert(df.shape[1], '最晚预约时间', early_time)
    print('基本信息扩充完成')

    print('开始剔除数据')
    # 剔除过短的轨迹，重复冗余订单、剔除gps漂移点
    # 对快递员做过滤，剔除重复冗余订单、删除gps漂移点
    # df = drop_unnormal(df)
    df, courier_information = drop_unnormal(df, fout)
    couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order = courier_information
    #np.save('./courier_feature_dic.npy', [couriers, dic_work_days, dic_dis_mean, dic_order_mean, dic_time_mean, dis_mean_order])

    print('数据剔除完成')

    # 添加编号信息
    df.insert(0, 'order_id', [n for n in range(1, df.shape[0] + 1)])

    # 统计未揽收订单，并贪心排序
    print('开始统计待揽收订单,并按距离贪心排序')

    df_v = df.values
    rem, rem_cnt = get_unpick(df)
    rem_greedy = []
    pbar = tqdm(total=df.shape[0])
    for n in range(df.shape[0]):
        pbar.update(1)
        rem_greedy.append(greedy_sort(n + 1, str2list(rem[n]), df_v))
    pbar.close()
    df.insert(df.shape[1] - 2, '待揽收订单', rem)
    df.insert(df.shape[1] - 2, '待揽收订单数量', rem_cnt)
    df.insert(df.shape[1] - 1, 'new_greedy', rem_greedy)
    print('待揽收订单贪心排序完成')

    # 获取快递员统计信息
    # couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order = courier_info(df)
    df.insert(df.shape[1] - 2, '快递员平均距离', [dis_mean_day[c]
              for c in df['快递员id']])
    df.insert(df.shape[1] - 2, '与前一单相对距离', [0 for n in range(df.shape[0])])
    df.insert(df.shape[1] - 2, '快递员平均间隔时间', [time_mean_order[c]
              for c in df['快递员id']])

    # 更新相邻订单信息
    print('更新相邻订单信息')
    courier_l = split_trace(df)

    got_time_shift = []
    lon_shift = []
    lat_shift = []
    dis_dir = []
    dis_rel = []  # 相对距离
    for c in courier_l:
        c_v = c.values
        for n in range(c.shape[0]):
            if n == 0:
                gts = c_v[n][2]
                lon_ = c_v[n][10]
                lat_ = c_v[n][11]
            else:
                gts = c_v[n - 1][2]
                lon_ = c_v[n - 1][10]
                lat_ = c_v[n - 1][11]

            lon_shift.append(lon_)
            lat_shift.append(lat_)
            dis = int(geodesic((lat_, lon_), (c_v[n][11], c_v[n][10])).meters)
            dis_dir.append(dis)
            if c_v[n][-5] == 0:
                dis_rel.append(0)
            else:
                dis_rel.append(dis / c_v[n][-5] * 100)
            got_time_shift.append(gts)

    df['与前一单直线距离'] = dis_dir
    df['与前一单相对距离'] = dis_rel
    df['上一经度'] = lon_shift
    df['上一纬度'] = lat_shift
    df['上一揽收时间'] = got_time_shift
    df['经度差'] = df['订单经度'].astype(float) - df['上一经度'].astype(float)
    df['纬度差'] = df['订单纬度'].astype(float) - df['上一纬度'].astype(float)
    df['时间差单位化'] = df['揽收时间1'] - df['上一揽收时间']
    geo = [geohash2.encode(lat, lon, 8)
           for lat, lon in zip(df['订单纬度'], df['订单经度'])]
    df.insert(12, 'geohash', geo)
    print('相邻订单信息更新完成')
    #     print(df.columns)

    # 生成简化订单数据
    df_sim = df[['order_id', '日期', '揽收时间1', '接单时间1',
                 '订单经度', '订单纬度', '时间差单位化', '与前一单相对距离']]
    # 重新编码，避免非数字
    block = list(set(df['区块类型id']))
    block_id = [block.index(b) for b in df['区块类型id']]
    df_sim.insert(6, 'block_id', block_id)
    pad = [0 for n in range(df_sim.shape[0])]
    df_sim.insert(7, 'weather', pad)
    days = sorted(list(set(df['日期'])))
    today_days = [days.index(d) for d in df['日期']]
    df_sim.insert(2, 'days', today_days)

    # 生成快递员数据
    id_ = [n for n in range(len(couriers))]
    work_days = [work_days[c] for c in couriers]
    dis_mean = [dis_mean_day[c] for c in couriers]
    time_mean = [time_mean_order[c] for c in couriers]
    order_mean = [order_mean_day[c] for c in couriers]
    cou_dic = {
        'id_': id_,
        'id': couriers,
        'work_days': work_days,
        'dis_mean': dis_mean,
        'order_mean': order_mean
    }
    cou_df = pd.DataFrame(cou_dic)
    if fout != '':
        dir_check(fout)
        df.to_csv(fout+'total.csv', index=False)
        df_sim.to_csv(fout+'simplified.csv', index=False)
        cou_df.to_csv(fout+'courier_feature.csv', index=False)
        print('中继文件存储完成')
    print('数据预处理完成')
    return df, df_sim, cou_df

# if __name__ == "__main__":

#     fin=ws + '/data/raw_data/shanghai_50blocks.csv'
#     temp_fout=ws + '/data/temp/shanghai_50blocks/'

#     pre_process(fin=fin,fout=temp_fout, is_test=False)#

    # 可视化快递员特征
    #couriers, work_days, dis_mean_day, order_mean_day, time_mean_order, dis_mean_order = np.load('./courier_feature_dic.npy', allow_pickle=True)
    # vis_distribution_auto(dis_mean_day.values(),'./distance_mean_day.png')
    # vis_distribution_auto(time_mean_order.values(), './time_mean_order.png')
    # vis_distribution_auto(dis_mean_order.values(), './distance_mean_order')
    # print(vis_distribution(dis_mean_order.values(), sections=[50 * i for i in range(13)]))
    # vis_distribution_auto(order_mean_day.values())
