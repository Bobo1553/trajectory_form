# -*- coding: utf-8 -*-
"""
Created on 2018/11/14 14:18

@author: Xiao Yijia
"""

import datetime
import arcpy
import time

from dao.data import DetailData

import csv
import math

# 公式参数
a_list = [999.842597, 6.793953 * 10 ** -2, -9.095290 * 10 ** -3, 1.001685 * 10 ** -4, -1.120083 * 10 ** -6,
          6.536332 * 10 ** -9]
b_list = [0.82449, -4.0899 * 10 ** -3, 7.6438 * 10 ** -5, -8.2467 * 10 ** -7, 5.3875 * 10 ** -9]
c_list = [-5.7246 * 10 ** -3, 1.0227 * 10 ** -4, -1.6546 * 10 ** -6]
d_list = [4.8314 * 10 ** -4]
e_list = [19652.21, 148.4206, -2.327105, 1.360477 * 10 ** -2, -5.155288 * 10 ** -5]
f_list = [54.6746, -0.603459, 1.099870 * 10 ** -2, -6.167 * 10 ** -5]
g_list = [7.944 * 10 ** -2, 1.6483 * 10 ** -2, -5.3009 * 10 ** -4]
h_list = [3.23990, 1.43713 * 10 ** -3, 1.16092 * 10 ** -4, -5.77905 * 10 ** -7]
i_list = [2.28380 * 10 ** -3, -1.09810 * 10 ** -5, -1.60780 * 10 ** -6]
j_list = [1.91075 * 10 ** -4]
k_list = [8.50935 * 10 ** -5, -6.12293 * 10 ** -6, 5.27870 * 10 ** -8]
m_list = [-9.9348 * 10 ** -7, 2.0816 * 10 ** -8, 9.1697 * 10 ** -10]
cb_list = [0.79, 0.8, 0.817, 0.832, 0.843, 0.845, 0.85]
n_list = [1.004, 1.01]
dead_weight_list = [40000, 60000, 80000, 120000, 200000, 300000]
# 输入文件的所在位置
source_csv_name = r'D:\ShipProgram\NewTankerMainRouteExtraction\TestData\File\Result1224\TestChangePoint1224.csv'
output_csv_name = r'D:\ShipProgram\NewTankerMainRouteExtraction\TestData\File\Result1224\Test.csv'
# 海水盐度、温度、压力数据所在的文件夹
day_salinity_raster_path = r'D:\地理数据\全球盐度数据_天'
hour_temperature_raster_path = r'D:\地理数据\全球海表温度_小时'
day_temperature_raster_path = r'D:\地理数据\全球海表温度_天'
hour_press_raster_path = r'D:\地理数据\全球压力数据_小时'
# 为真的时候使用小时数据，为假的时候使用天数据；对于只有小时或者只有天的数据，则取那个唯一的数据
use_hour_data = True
# 默认的海水温度单位为吨每立方米
default_density = 1.023


def get_data(ship_data, data_raster_name, cell_size):
    if ship_data.ship_position.X < 0:
        position = str(ship_data.ship_position.X + 360) + " " + str(ship_data.ship_position.Y)
    else:
        position = str(ship_data.ship_position.X) + " " + str(ship_data.ship_position.Y)
    data = arcpy.GetCellValue_management(data_raster_name, position).getOutput(0)
    if data == 'NoData':
        avg_data = 0
        count = 0
        i = -2
        while i < 3:
            j = -2
            while j < 3:
                if math.fabs(i) + math.fabs(j) < 3:
                    if ship_data.ship_position.X + i * cell_size < 0:
                        position = str(ship_data.ship_position.X + 360 + i * cell_size) + " " + \
                                   str(ship_data.ship_position.Y + j * cell_size)
                    else:
                        position = str(ship_data.ship_position.X + i * cell_size) + " " + \
                                   str(ship_data.ship_position.Y + j * cell_size)
                    near_data = arcpy.GetCellValue_management(data_raster_name, position).getOutput(0)
                    if near_data != 'NoData':
                        avg_data += float(near_data)
                        count += 1
                j += 1
            i += 1
        if count > 0:
            return avg_data/count
        else:
            return 'NoData'
    else:
        return float(data)


def get_day_salinity(ship_data):
    """
    返回船舶轨迹点所在位置的盐度，其中盐度的单位为PSU
    :param ship_data:船舶轨迹点
    :return:如果没有数据的话，返回NoData
    """
    time_array = time.localtime(ship_data.utc)
    day_salinity_raster_name = day_salinity_raster_path + '\\{}\\{}_{}_{}.tif'.format(
        time_array.tm_year, time_array.tm_year, time_array.tm_mon, time_array.tm_mday)
    return get_data(ship_data, day_salinity_raster_name, 1)


def get_hour_temperature(ship_data):
    """
    返回船舶轨迹点所在位置的海面温度，时间分辨率为小时级别，单位为摄氏度
    :param ship_data: 船舶轨迹点
    :return: 如果没有数据的话，返回NoData
    """
    time_array = time.localtime(ship_data.utc)
    hour_temperature_raster_name = hour_temperature_raster_path + '\\{}\\{}_{}_{}_{}.tif'.format(
        time_array.tm_year, time_array.tm_year, time_array.tm_mon, time_array.tm_mday, time_array.tm_hour)
    result = get_data(ship_data, hour_temperature_raster_name, 0.25)
    if result == 'NoData':
        return result
    else:
        return result - 273.15


def get_day_temperature(ship_data):
    """
    返回船舶轨迹点所在位置的海面温度，时间分辨率为天级别，单位为摄氏度
    :param ship_data: 船舶轨迹点
    :return: 如果没有数据的话，返回NoData
    """
    time_array = time.localtime(ship_data.utc)
    day_temperature_raster_name = day_temperature_raster_path + '\\{}\\{}_{}_ {}.tif'.format(
        time_array.tm_year, time_array.tm_year, time_array.tm_mon, time_array.tm_mday)
    return get_data(ship_data, day_temperature_raster_name, 0.25)


def get_hour_press(ship_data):
    """
    返回船舶轨迹点所在位置的压力，时间分辨率为小时级别，单位为bar
    :param ship_data: 船舶轨迹点
    :return: 如果没有数据的话，返回NoData
    """
    time_array = time.localtime(ship_data.utc)
    hour_press_raster_name = hour_press_raster_path + '\\{}\\{}_{}_{}_{}.tif'.format(
        time_array.tm_year, time_array.tm_year, time_array.tm_mon, time_array.tm_mday, time_array.tm_hour)
    result = get_data(ship_data, hour_press_raster_name, 0.25)
    if result == 'NoData':
        return result
    else:
        return result/100000


def calculate_density_upper(ship_data):
    salinity = get_day_salinity(ship_data)
    if use_hour_data:
        temperature = get_hour_temperature(ship_data)
    else:
        temperature = get_day_temperature(ship_data)
    if salinity == 'NoData' or temperature == 'NoData':
        return 'NoData'
    density_smow = (a_list[0] + a_list[1] * temperature + a_list[2] * temperature ** 2 + a_list[3] * temperature ** 3
                    + a_list[4] * temperature ** 4 + a_list[5] * temperature ** 5)
    b = b_list[0] + b_list[1] * temperature + b_list[2] * temperature ** 2 + b_list[3] * temperature ** 3 \
        + b_list[4] * temperature ** 4
    c = c_list[0] + c_list[1] * temperature + c_list[2] * temperature ** 2
    density = density_smow + b * salinity + c * salinity ** 1.5 + d_list[0] * salinity ** 2
    return density


def calculate_k(ship_data):
    # 计算，当press为0的时候
    salinity = get_day_salinity(ship_data)
    press = get_hour_press(ship_data)
    if use_hour_data:
        temperature = get_hour_temperature(ship_data)
    else:
        temperature = get_day_temperature(ship_data)
    if salinity == 'NoData' or temperature == 'NoData' or press == 'NoData':
        return 'NoData'
    kw = (e_list[0] + e_list[1] * temperature + e_list[2] * temperature ** 2 + e_list[3] * temperature ** 3
          + e_list[4] * temperature ** 4)
    f = f_list[0] + f_list[1] * temperature + f_list[2] * temperature ** 2 + f_list[3] * temperature ** 3
    g = g_list[0] + g_list[1] * temperature + g_list[2] * temperature ** 2
    k_s_t_0 = kw + f * salinity + g * salinity ** 1.5
    # 当press不为0的时候的调整值
    aw = h_list[0] + h_list[1] * temperature + h_list[2] * temperature ** 2 + h_list[3] * temperature ** 3
    a1 = (aw + (i_list[0] + i_list[1] * temperature + i_list[2] * temperature ** 2) * salinity +
          j_list[0] * salinity ** 1.5)
    bw = k_list[0] + k_list[1] * temperature + k_list[2] * temperature ** 2
    b2 = bw + (m_list[0] + m_list[1] * temperature + m_list[2] * temperature ** 2) * salinity
    k_s_t_p = k_s_t_0 + a1 * press + b2 * press ** 2
    return k_s_t_p


def calculate_density(ship_data):
    press = get_hour_press(ship_data)
    if press == 'NoData' or calculate_density_upper(ship_data) == 'NoData' or calculate_k(ship_data) == 'NoData':
        return default_density
    else:
        return calculate_density_upper(ship_data) / (1 - press / calculate_k(ship_data)) / 1000


def calculate_displace_volume(ship_data):
    """
    获得船舶排水重量，其中如果压力、盐度、温度返回为NoData的话，密度取海水默认密度，单位为吨每立方米
    :param ship_data:船舶位置点
    :return:返回的是船舶排水重量，单位为吨
    """
    density = calculate_density(ship_data)
    if ship_data.dead_weight < dead_weight_list[0]:
        cb = cb_list[0]
    elif ship_data.dead_weight < dead_weight_list[1]:
        cb = cb_list[1]
    elif ship_data.dead_weight < dead_weight_list[2]:
        cb = cb_list[2]
    elif ship_data.dead_weight < dead_weight_list[3]:
        cb = cb_list[3]
    elif ship_data.dead_weight < dead_weight_list[4]:
        cb = cb_list[4]
    elif ship_data.dead_weight < dead_weight_list[5]:
        cb = cb_list[5]
    else:
        cb = cb_list[6]
    displace_volume = density * ship_data.length * ship_data.width * ship_data.draught * cb
    return displace_volume


def calculate_change_dead_weight(before_ship_data, after_ship_data):
    """
    计算前后点之间的船舶载重值变化，其中考虑不同区域的差异进行弥补
    :param before_ship_data: 前点船舶
    :param after_ship_data: 后点船舶
    :return: 修正后的船舶载重情况变化，单位为吨，其中载重值为正为装货，反之为卸货
    """
    before_displace_volume = calculate_displace_volume(before_ship_data)
    after_displace_volume = calculate_displace_volume(after_ship_data)
    # 根据calculate_density来获得前后船舶位置点的密度
    after_density = calculate_density(after_ship_data)
    before_density = calculate_density(before_ship_data)
    adjust_amount = (after_density - before_density) / before_density * before_displace_volume
    if before_ship_data.dead_weight < dead_weight_list[0]:
        k = n_list[1]
    elif before_ship_data.dead_weight >= dead_weight_list[5]:
        k = n_list[0]
    else:
        k = (n_list[1] - n_list[0]) * (before_ship_data.dead_weight - dead_weight_list[0]) / \
            (dead_weight_list[5] - dead_weight_list[0]) + n_list[0]
    change_dead_weight = (after_displace_volume - before_displace_volume) * k - adjust_amount

    return change_dead_weight


def calculate_weight():
    """
    输入为吃水变化点的.csv文件
    输出为包含有载重变化结果的吃水变化点的.csv文件
    :return:
    """
    with open(source_csv_name, 'r') as source_csv:
        source_csv_reader = csv.reader(source_csv)
        source_csv_reader.next()

        with open(output_csv_name, 'wb') as output_csv:
            output_csv_writer = csv.writer(output_csv)
            output_csv_writer.writerow(['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width',
                                        'dead_weight', 'gross_weight', 'longitude', 'latitude', 'draught', 'speed',
                                        'utc', 'line_num', 'change_weight'])

        for row in source_csv_reader:
            before_ship_data = DetailData(int(row[0]), row[1], row[2], row[3], row[4], int(row[5]), int(row[6]),
                                          int(row[7]), int(row[8]), float(row[9]), float(row[10]), float(row[11]),
                                          float(row[12]), int(row[13]))
            row = source_csv_reader.next()
            after_ship_data = DetailData(int(row[0]), row[1], row[2], row[3], row[4], int(row[5]), int(row[6]),
                                         int(row[7]), int(row[8]), float(row[9]), float(row[10]), float(row[11]),
                                         float(row[12]), int(row[13]))
            change_dead_weight = calculate_change_dead_weight(before_ship_data, after_ship_data)
            output_csv_writer.writerow(before_ship_data.export_to_csv() + [row[14], change_dead_weight])
            output_csv_writer.writerow(after_ship_data.export_to_csv() + [row[14], change_dead_weight])

    return


def main():
    """
    
    :return:
    """
    start_time = datetime.datetime.now()
    calculate_weight()
    # change_utc_to_datetime()
    end_time = datetime.datetime.now()
    print '运行所用时间为', (end_time - start_time).seconds, 's'
    return


if __name__ == '__main__':
    main()
