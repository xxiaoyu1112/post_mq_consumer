import sys, os, platform
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
import random
from scipy import sparse
from tqdm import tqdm
from .util import dir_check,write_list_list
from .pro_process import pre_process,idx


def week2vec(week_id):
    # 周 one-hot 9
    vec = [0] * (60 // 7 + 1)
    vec[week_id // 7] = 1
    return vec


# week2vec(21) #[0, 0, 0, 1, 0, 0, 0, 0, 0]

def day2vec(day_id):
    # 周内 one-hot 7
    vec = [0] * 7
    vec[int(day_id) % 7] = 1
    return vec


# day2vec(2) # [0, 0, 1, 0, 0, 0, 0]

def got_time2vec(time_id):
    # 时间 one-hot
    vec = [0] * 12
    if time_id < 8.5 * 60:
        vec[0] = 1
        return vec
    if time_id > 18.5 * 60:
        vec[-1] = 1
        return vec
    vec[int(time_id - 8.5 * 60) // 60 + 1] = 1
    return vec


# got_time2vec(860) #[0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0]

def book_time2vec(time_id):
    vec = [0] * 12
    if time_id < 8 * 60:
        vec[0] = 1
        return vec
    if time_id > 18 * 60:
        vec[-1] = 1
        return vec
    vec[int(time_id - 8 * 60) // 60 + 1] = 1
    return vec


# book_time2vec(860) #[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]

def get_trans_graph(block):
    temp = []
    sps = np.load( '/data/graph/trans_mean_time.npy', allow_pickle=True)
    for nn in range(10):
        temp.append(sps[nn].toarray())
    return np.array(temp)


def get_neighbor_graph(block):
    sp = np.load('/data/graph/neighbor_graph.npy', allow_pickle=True)
    return sp[0].toarray()


def load_node_info(fin):
    return np.load(fin)


def json2dic(name_json):
    with open( '/data/' + name_json + '.json', 'r') as fr:
        return json.load(fr)





# block=22146 {22146，13603,0} 运营区id ,0返回两个运营区
# graph_ret=True, 为假时不返回G_transfers, G_neighbor, G_X
def create_sample(fin_original='',fin_temp='',fout_temp='',day_window=(0, 10), block={22146}, label_range=(0,120),len_range=(1,25), graph_ret=True):

    def dis_cal(fro, to, a, b, c, d):  ##计算距离(样本计算距离)
        if (str(fro) + '-' + str(to)) in dic_dis_cal:
            return dic_dis_cal[str(fro) + '-' + str(to)]
        else:
            dic_dis_cal[str(fro) + '-' + str(to)] = int(geodesic((a, b), (c, d)).meters)
            return dic_dis_cal[str(fro) + '-' + str(to)]
    remove_cnt=0
    # 读取数据
    if fin_temp!='':
        df = pd.read_csv(fin_temp + "/total.csv", sep=',', encoding='utf-8')  # 总订单数据
        df_sim = pd.read_csv(fin_temp+ "/simplified.csv", sep=',', encoding='utf-8')  # 简化订单数据
        df_cou = pd.read_csv(fin_temp + "/courier_feature.csv", sep=',', encoding='utf-8')# 快递员信息
    else:
        if fin_original=='':
            print('请指定原始文件路径')
            return
        else:
            df,df_sim,df_cou=pre_process(fin=fin_original, fout=fout_temp)

    geo = [geohash2.encode(lat, lon, 8) for lat, lon in zip(df['订单纬度'], df['订单经度'])]
    dic_geo2index = {}  # 字典 geohash到点的索引的映射 如dic['wtw31evn']=3
    ver = sorted(list(set(geo)))
    geo_cnt = 0
    for geo_ in ver:
        dic_geo2index[geo_] = geo_cnt
        geo_cnt += 1
    sim_value = df_sim.values
    cou_value = df_cou.values

    last_route = []
    unpicked_set = []
    global_fea = []
    unpicked_geos = []
    real_order = []
    real_index = []
    eta = []
    last_seq_len = []
    seq_len = []
    days = []

    #字段索引

    #主表字段索引
    idx_order_id=idx(df,'order_id')#0订单编号
    idx_block_id=idx(df, '运营区id')#4运营区编号
    idx_courier_id=idx(df, '快递员id')#6快递员编号
    idx_order_num=idx(df, '待揽收订单数量')#39剩余订单数量
    idx_unpicked_set_greedy=idx(df, 'new_greedy')#44待揽收订单集合（距离贪心排列）
    idx_got_time=idx(df, '揽收时间1')#2订单揽收时间
    idx_accept_time=idx(df, '接单时间1')#3接单时间
    idx_longitude=idx(df, '订单经度')#10订单经度
    idx_latitude=idx(df, '订单纬度')#11订单纬度
    idx_courier_distance=idx(df, '快递员平均距离')#41快递员历史距离信息
    idx_promised_time=idx(df, '最晚预约时间')#45预约时间

    #简表字段索引
    simidx_day=idx(df_sim, 'days')#2当前天数，从0开始
    simidx_weather=idx(df_sim, 'weather')#8天气

    #快递员表字段索引
    couidx_id=idx(df_cou, 'id_')#0快递员编号，从0开始

    ## 拆分轨迹
    courier_l = []  #
    temp1 = df.values[0]
    f = 0
    t = 0
    for i in df.values:
        if i[idx_courier_id] != temp1[idx_courier_id]:
            courier_l.append(df[f:t])
            f = t
        t = t + 1
        temp1 = i
    courier_l.append(df[f:t])

    cnt = 0  # 统计样本数
    # if before:
    #     total = df_sim[df_sim['days'] <= split_day].shape[0]
    # else:
    #     total = df_sim[df_sim['days'] >= split_day].shape[0]
    start_day, end_day = day_window
    day_window = list(range(start_day, end_day))
    if 0 in block:
        total = df_sim[df_sim['days'].isin(day_window)].shape[0]
    else:
        total = df_sim[(df_sim['days'].isin(day_window)) & (df['运营区id'].isin(block))].shape[0]

    # if block != 0: total = total // 2

    # 显示进度条
    pbar = tqdm(total=total)
    for cou in courier_l:
        dic_dis_cal = {}
        cou_v = cou.values
        today_day = sim_value[cou_v[0][idx_order_id] - 1][simidx_day]

        if today_day not in day_window: continue

        if int(cou_v[0][idx_block_id]) not in block and 0 not in block:
            continue
        id_first = cou_v[0][idx_order_id]
        ##当天不变量
        today_temp = day2vec(today_day)
        # today_temp.append(sim_value[cou_v[0][idx_order_id] - 1][simidx_weather])  # 天气
        #
        # # 是否临近双十一、双十二，此条只针对上海数据集
        # if (4 >= today_day >= 0) or (34 >= today_day >= 31):
        #     today_temp.append(1)
        # else:
        #     today_temp.append(0)

        ##快递员个性特征
        cou_id = cou_v[0][idx_courier_id]
        cou_index = list(df_cou['id']).index(int(cou_id))
        cou_fea = cou_value[cou_index][2:] #@ 因为前两列是ID_，和ID信息
        global_temp = today_temp + list(cou_fea)
        global_temp.append(cou_value[cou_index][couidx_id])
        global_temp.append(0)#后续用快递员位置填充
        global_temp.append(0)#后续用快递员位置填充

        # 遍历当前轨迹
        for start in range(len(cou_v)):
            pbar.update(1)
            if cou_v[start][idx_order_num] > 0:
                if str(cou_v[start][idx_unpicked_set_greedy]) != 'nan':
                    rem_greedy_list = str(cou_v[start][idx_unpicked_set_greedy]).split('.')
                else:
                    rem_greedy_list = []
            else:
                continue
            ##序列二 待揽收集合
            ##剔除
            need_remove=[]
            for rem in rem_greedy_list:
                rem_id = int(rem)
                pre_end = rem_id - id_first
                if pre_end <= start:#@ 这里的剔除规则是什么
                    need_remove.append(rem)
                    continue
                    ## 按label剔除
                if cou_v[pre_end][idx_got_time] - cou_v[start][idx_got_time] <= label_range[0] or cou_v[pre_end][idx_got_time] - cou_v[start][idx_got_time] > label_range[1]:
                    remove_cnt+=1
                    need_remove.append(rem)
                    continue
                    ##受影响剔除
                drop_flag = 0
                for check_p in range(start + 1, pre_end):
                    if cou_v[check_p][idx_accept_time] > cou_v[start][idx_got_time]:
                        drop_flag = 1
                        break
                if drop_flag == 1:
                    need_remove.append(rem)
            #@ 去掉订单
            for nr in need_remove:
                rem_greedy_list.remove(nr)

            if len(rem_greedy_list) <= max(len_range[0], 0):  #剔除后样本长度过短，整体剔除
                continue

            if len(rem_greedy_list) > len_range[1]:
                continue #超过的都不要了
                #rem_greedy_list = rem_greedy_list[:len_range[1]] 只保留按贪心顺序的前len_range[1]单


            rem_sorted_list = list(map(int, rem_greedy_list))  # 按揽件时间排序得到真实揽收序列
            rem_sorted_list.sort()
            rem_cnt = 0
            rem_temp = []
            order_temp = []
            index_temp = []
            eta_temp = []
            geos_temp = []
            cnt_rem = 0
            for rem in rem_sorted_list:
                index_temp.append(rem_greedy_list.index(str(rem)))
                # geos_temp.append(dic_geo2index[geo[int(rem) - 1]])
                if cnt_rem >= len_range[1]-1:
                    break
                cnt_rem += 1
            for nnn in range(len_range[1] - len(rem_sorted_list)):
                index_temp.append(-1)
                # geos_temp.append(0)
            cnt_rem = 0


            for rem in rem_greedy_list:
                rem_id = int(rem)
                pre_end = rem_id - id_first
                abs_temp = dis_cal(start, pre_end, cou_v[start][idx_latitude], cou_v[start][idx_longitude], cou_v[pre_end][idx_latitude],
                                   cou_v[pre_end][idx_longitude])
                if cou_v[pre_end][idx_courier_distance] == 0:  # 防止除的属性距离为0

                    dis_temp = 0.0
                else:
                    dis_temp = abs_temp / cou_v[pre_end][idx_courier_distance] * 100

                rem_temp_sub = list(sim_value[int(rem) - 1][3:-3])
                rem_temp_sub.remove(rem_temp_sub[0])  # 移除揽件时间
                rem_temp_sub.append(dis_temp)
                rem_temp_sub.append(abs_temp)
                rem_temp_sub.append(cou_v[pre_end][idx_promised_time] - cou_v[pre_end][idx_accept_time])
                rem_temp_sub.append(cou_v[pre_end][idx_promised_time] - cou_v[start][idx_got_time])
                # rem_temp_sub = rem_temp_sub + book_time2vec(rem_temp_sub[1])
                rem_temp.append(rem_temp_sub)

                geos_temp.append(dic_geo2index[geo[int(rem) - 1]])
                order_temp.append(rem_sorted_list.index(int(rem)) + 1)
                # if cou_v[pre_end][2] - cou_v[start][2]>label_range[1]:
                #     print(cou_v[pre_end][2],cou_v[start][2])
                eta_temp.append(cou_v[pre_end][idx_got_time] - cou_v[start][idx_got_time])

                cnt_rem += 1
                if cnt_rem >= len_range[1]:
                    break
            if cnt_rem < len_range[1]:
                padding = [0.0] * 8
                for nn in range(len_range[1] - cnt_rem):
                    rem_temp.append(padding)
                    order_temp.append(0)
                    eta_temp.append(0)
                    geos_temp.append(-1)
            ##已完成订单序列
            padding1 = [0.0] * 9
            last_temp = []
            if start < 4:
                for nn in range(start + 1):
                    last_temp_sub = list(sim_value[cou_v[nn][idx_order_id] - 1][3:])
                    last_temp_sub.remove(last_temp_sub[-3])  # 移除天气
                    last_temp_sub.append(cou_v[nn][-6])
                    last_temp_sub.append(cou_v[nn][idx_promised_time] - cou_v[nn][idx_accept_time])
                    # last_temp_sub = last_temp_sub + got_time2vec(last_temp_sub[1]) + book_time2vec(last_temp_sub[2])
                    last_temp.append(last_temp_sub)
                for nn in range(4 - start):
                    last_temp.append(padding1)
            else:
                for nn in range(start - 4, start + 1):
                    last_temp_sub = list(sim_value[cou_v[nn][0] - 1][3:])
                    last_temp_sub.remove(last_temp_sub[-3])  # 移除天气
                    last_temp_sub.append(cou_v[nn][-6])
                    last_temp_sub.append(cou_v[nn][idx_promised_time] - cou_v[nn][idx_accept_time])
                    # last_temp_sub = last_temp_sub + got_time2vec(last_temp_sub[1]) + book_time2vec(last_temp_sub[2])
                    last_temp.append(last_temp_sub)
            last_seq_len.append(min(5, start + 1))
            last_route.append(last_temp)
            global_temp[-2] = cou_v[start][idx_longitude]
            global_temp[-1] = cou_v[start][idx_latitude]
            global_fea.append(global_temp)
            unpicked_set.append(rem_temp)
            # 有效长度
            seq_len.append(min(25, len(rem_greedy_list)))
            unpicked_geos.append(geos_temp)
            days.append(today_day)  # 第几天，用于提图
            real_order.append(order_temp)
            real_index.append(index_temp)
            # if len(index_temp) != 25:
            #     print(len(index_temp))
            eta.append(eta_temp)
            cnt += 1
            # if cnt % 1000 == 0:
            #     print(cnt)
    pbar.close()

    ##转存numpy

    # n为样本数
    last_x = np.array(
        last_route)  ##(n, 5, 9) 11:  揽收时间 下单时间 经度 纬度 区块类型 与前一单的时间差 与前一单的直线相对距离 与前一单的直线绝对距离 预约-接单 揽收时间onehot12 下单时间onehot12
    last_route = []  # 释放内存
    last_len = np.array(last_seq_len)  # (n,) #当前样本已揽收揽收序列长度最长为5
    last_seq_len = []
    global_x = np.array(
        global_fea)  # (n, 7(oh)+2+5)  周几（one-hot7）   快递员工作天数 每天移动距离 平均两单间时间 平均每日单数 快递员编号从0开始 快递员当前经度 快递员当前纬度
    global_fea = []
    unpick_x = np.array(
        unpicked_set)  # (n, 25, 8) 8:  下单时间 经度 纬度 区块类型 与所在订单的直线相对距离 与前一单的直线绝对距离 预约-接单 预约-当前 下单时间onehot12
    unpicked_set = []
    unpick_len = np.array(seq_len)  # (n,) #当前样本待揽收序列长度
    seq_len = []
    unpick_geo = np.array(unpicked_geos)  # (n, 25) 25个订单geohash，不足补' '
    unpicked_geos = []
    days_np = np.array(days)  # (n,) #当前样本为第几天
    days = []
    order_np = np.array(real_order)  # (n, 25) 25个订单实际揽收顺序，不足补零
    index_np = np.array(real_index)  # (n, 25)实际位次递增的订单索引，不足补零
    real_index = []
    real_order = []
    eta_np = np.array(eta)  # (n, 25) 25个订单是实际eta，不足补-1
    eta = []

    return last_x, last_len, global_x, unpick_x, unpick_len, unpick_geo, days_np, order_np, index_np, eta_np, dic_geo2index

#len_range[1]=25


# last_x,last_len, global_x, unpick_x, unpick_len,unpick_geo,days_np, order_np, index_np, \
# eta_np, G_transfers, G_neighbor, G_X, dic_geo2index = create_sample( before=True, split_day=2, block=0, LIMITEDTIME=0,graph_ret=True)
# # last_x,last_len, global_x, unpick_x, unpick_len,unpick_geo,days_np, order_np, index_np, \
# # eta_np, dic_geo2index = create_sample( before=True, split_day=2, block=0, LIMITEDTIME=0,graph_ret=False)
# # oh代表one-hot,n为样本数
# print(last_x.shape)  # (n, 5, 9) 11: 揽收时间 下单时间 经度 纬度 区块类型 天气 与前一单的时间差 与前一单的直线相对距离 与前一单的直线绝对距离 预约-接单
# print(last_len.shape) # (n,) #当前样本已揽收揽收序列长度最长为5
# print(global_x.shape)  # (n, 7(oh)+2+5)  周几（one-hot7）  天气 是否临近双十一双十二 + 快递员工作天数 每天移动距离 平均两单间时间 平均每日单数 (快递员编号从0开始)
# print(unpick_x.shape)  # (n, 25, 8) 8:  下单时间 经度 纬度 区块类型 与所在订单的直线相对距离 与前一单的直线绝对距离 预约-接单 预约-当前
# print(unpick_len.shape)  # (n,) #当前样本待揽收序列长度
# print(unpick_geo.shape) # (n, 25) 25个订单geohash，不足补' '
# print(days_np.shape)  # (n,) #当前样本为第几天
# print(order_np.shape)  # (n, 25) 25个订单是实际揽收顺序，不足补零
# print(index_np.shape)  # (n, 25)实际位次递增的订单索引(0-n-1)，不足补-1
# print(eta_np.shape)  # (n, 25) 25个订单是实际eta，不足补零
# print(G_transfers.shape)  # (9*点数*点数) #每周一张图，点的个数取决于运营区
# print(G_neighbor.shape)  # (点数*点数) #点的个数取决于运营区
# print(G_X.shape)# (点数*3) 纬度 经度 累计单数

if __name__ == "__main__":
    if 0:
        ## 用法二： 基于中继文件（预处理后的数据）生成样本，除样本参数外需设置中继文件路径fin_temp
        ##注意：fin_temp应对应目录，而非文件，目录下应有 total simplified courier_feature 三个csv文件
        fin_temp =  '/data/temp/shanghai_10blocks/'
        fout =  '/data/dataset/shanghai-b10_0-120_1-25/test_order.npy'
        dir_check(fout)
        print(fout)
        np.save(fout, create_sample(fin_temp=fin_temp, day_window=(50,60), block={0},
                                    label_range=(0, 120), len_range=(1, 25), graph_ret=False))
        last_x, last_len, global_x, unpick_x, unpick_len, unpick_geo, days_np, order_np, index_np, eta_np, dic_geo2index = np.load(fout, allow_pickle=True)
        print(np.max(index_np))
        print(len(unpick_x))




