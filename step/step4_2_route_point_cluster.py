# -*- coding: utf-8 -*-
"""
Created on 2020/1/30 17:23

@author: Xiao Yijia
"""

import datetime

import csv
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN

# 输入输出文件
input_route_point_csv_name = r'D:\ShipProgram\DoctorPaper\MSRData\TestData\step4\RoutePoint.csv'
output_route_point_csv_name = r'D:\ShipProgram\DoctorPaper\MSRData\TestData\step4\RoutePointClusterResult.csv'
output_route_point_header = ['sp_index', 'rp_index', 'mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length',
                             'width', 'longitude', 'latitude', 'draft', 'speed', 'utc', 'label']

# cluster parameter
eps = 0.1
min_samples = 5


def point_cluster(input_csv_name, output_csv_name, header):
    """
    1. 将生成的航路点进行聚类
    2. 输出一个原始文件夹，增加一个属性
    3. 输出另外一个文件夹，保存聚类中心
    :return:
    """
    # input
    df = pd.read_csv(input_csv_name)
    coordinates = df.values[:, 9:11]

    # cluster
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(coordinates)

    # output
    result_arr = np.insert(df.values, len(header) - 1, values=clustering.labels_, axis=1)
    result_pd = pd.DataFrame(result_arr, columns=header)
    result = result_pd[result_pd['label'] != -1]
    result.to_csv(output_csv_name, index=False)

    return


if __name__ == '__main__':
    point_cluster(input_route_point_csv_name, output_route_point_csv_name, output_route_point_header)
