# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 14:18:16 2018

@author: Xiao Yijia
"""

import csv
import datetime
import math

import arcpy

from dao.DBClass import DBProcess
from dao.Data import DetailData

source_db_name = r'D:\ShipProgram\DoctorPaper\MSRData\DBData\Test\CrudeOilTanker2017.db'  # 数据处理后的油轮数据库
speed_threshold = 18  # 此处修改为18，为共用MMSI识别的速度阈值
distance_threshold = 3.4  # 单位为km,6分钟以18节的速度航行的距离，6/60*18*1.852=3.33km
# MMSI识别，由于后面航次划分输入的数据格式为数据库，这里是否修改为数据库格式输出？
out_csv_name = r'D:\ShipProgram\DoctorPaper\MSRData\FileData\CrudeOilTanker_MMSIIdentify.csv'
table_name = 'CrudeOilTanker'


ocean_shp_name = r'D:\GeoData\OceanData\ne_10m_geography_marine_polys.shp'  # 全球海域面图层
ocean_shp = arcpy.da.SearchCursor(ocean_shp_name, ["SHAPE@"])
ocean_list = []
for ocean in ocean_shp:
    ocean_list.append(ocean[0])


def calculate_speed(data_position, point):
    """
    用于计算两个点之间的平均速度, 速度单位为节
    :param data_position:
    :param point:
    :return:
    """
    distance = 6378.138 * 2 * math.asin(math.sqrt(math.pow(math.sin(math.radians(data_position.ship_position.Y -
                                                                                 point.ship_position.Y) / 2), 2) +
                                                  math.cos(math.radians(data_position.ship_position.Y)) *
                                                  math.cos(math.radians(point.ship_position.Y)) *
                                                  pow(math.sin(math.radians(data_position.ship_position.X -
                                                                            point.ship_position.X) / 2), 2)))
    if distance < distance_threshold:
        return 0
    speed = distance / (math.fabs(data_position.utc - point.utc))
    speed = speed * 3600 / 1.852
    return speed


def judge_by_speed(data_position, sequentially, csv_writer):
    """
    根据速度来进行MMSI共用识别，并且尽量将相同的船舶合并在一起
    :param data_position: 现在位置的船舶用于进行判断
    :param sequentially: 为共用MMSI的多艘船舶的最后一个位置点的数组
    :param csv_writer: 用于保存输出结果的csv_writer
    :return:
    """
    i = 0
    for point in sequentially:
        if point.utc == data_position.utc:
            return
        speed = calculate_speed(data_position, point)
        if speed <= speed_threshold:
            data = [str(i)] + point.export_to_csv()[1:]
            csv_writer.writerow(data)
            sequentially[i] = data_position
            return
        i += 1
    sequentially.append(data_position)
    return


def mmsi_identify():
    """
    识别共用MMSI的船舶，并且剔除掉空间位置错误的船舶点以及重复数据，输出结果为一个.csv文件
    :return:
    """
    # 打开.db文件
    print('正在读取数据...')
    source_db = DBProcess(source_db_name)
    source_db.run_sql('SELECT mmsi,imo,vessel_name,vessel_type_sub,length,width,longitude,latitude,draught,sog,'
                      'ts_pos_utc FROM {} ORDER BY mmsi,ts_pos_utc'.format(table_name))
    print('读取数据完成')

    # 打开用于保存结果的.csv文件，并且写入必要的头文件
    with open(out_csv_name, 'wb') as out_csv:
        csv_writer = csv.writer(out_csv)
        csv_writer.writerow(['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width', 'longitude',
                             'latitude', 'draught', 'speed', 'utc'])

        # 初始化
        row = source_db.dbcursor.next()
        detail_data = DetailData(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                 row[10], )
        sequentially = [detail_data]

        # 遍历数据库中的每一条数据
        for row in source_db.dbcursor:
            temp_data = DetailData(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                   row[10], )
            # 剔除重复的船舶
            if detail_data.judge_repeat(temp_data):
                continue

            if not temp_data.judge_in_shp(ocean_list):
                continue

            if temp_data.mmsi == detail_data.mmsi:
                detail_data = DetailData(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                         row[9], row[10], )
                judge_by_speed(detail_data, sequentially, csv_writer)
            else:
                i = 0
                for ship_point in sequentially:
                    data = [str(i)] + ship_point.export_to_csv()[1:]
                    csv_writer.writerow(data)
                    i += 1
                detail_data = DetailData(0, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                         row[9], row[10], )
                print('Change Point', detail_data.mmsi)
                sequentially = [detail_data]
        i = 0
        for ship_point in sequentially:
            data = [str(i)] + ship_point.export_to_csv()[1:]
            csv_writer.writerow(data)
            i += 1

    source_db.close_db()
    return


def main():
    """
    调用共用MMSI识别的函数，并且统计函数运行的时间
    :return:
    """
    print('对共用MMSI的情况进行识别')
    start_time = datetime.datetime.now()
    mmsi_identify()
    end_time = datetime.datetime.now()
    print('程序运行的时间为', (end_time - start_time).seconds, 's')
    return


if __name__ == '__main__':
    main()
