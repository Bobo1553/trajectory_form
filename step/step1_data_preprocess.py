# -*- coding: utf-8 -*-
"""
Created on Thu Oct 11 10:05:33 2018

@author: Xiao
"""
import csv
import os
from dao.common_db import DBProcess
import datetime

from const.const_sql import ConstSQL

db_file_path = r'F:\NewShipsDB2017'
# temp_db_file_path = r'D:\ShipProgram\MainRouteExtraction2020\MSRData\DBData\Temp'
result_db_name = r'D:\ShipProgram\DoctorPaper\MSRData\DBData\Test\CrudeOilTanker2017.db'
# 对应于上一步判断船舶记录数
less_ship_mmsi_csv_name = r"D:\ShipProgram\DoctorPaper\Data\LessShipMMSI.csv"
table_name = 'CrudeOilTanker'
# draught_threshold = 30  # 吃水阈值
# speed_threshold = 20  # 删除速度异常值
# 用于创建新的表格
FieldList = [['mmsi', 'INTEGER'], ['imo', 'INTEGER'], ['vessel_name', 'TEXT'], ['callsign', 'TEXT'],
             ['vessel_type', 'TEXT'], ['vessel_type_code', 'INTEGER'], ['vessel_type_cargo', 'TEXT'],
             ['vessel_class', 'TEXT'], ['length', 'INTEGER'], ['width', 'INTEGER'], ['flag_country', 'TEXT'],
             ['flag_code', 'INTEGER'], ['destination', 'TEXT'], ['eta', 'INTEGER'], ['draught', 'DOUBLE'],
             ['longitude', 'DOUBLE'], ['latitude', 'DOUBLE'], ['sog', 'DOUBLE'], ['cog', 'DOUBLE'], ['rot', 'DOUBLE'],
             ['heading', 'INTEGER'], ['nav_status', 'TEXT'], ['nav_status_code', 'INTEGER'], ['source', 'TEXT'],
             ['ts_pos_utc', 'INTEGER'], ['ts_static_utc', 'INTEGER'], ['dt_pos_utc', 'TEXT'], ['dt_static_utc', 'TEXT'],
             ['vessel_type_main', 'TEXT'], ['vessel_type_sub', 'TEXT']]
MMSI_field_list = [['MMSI', 'INTEGER'], ['ShipName', 'TEXT'], ['DeadWeight', 'INTEGER'], ['GrossWeight', 'INTEGER'],
                   ['BuiltTime', 'INTEGER']]
MMSI_db_name = r'D:\ShipProgram\MainRouteExtraction2020\Data\DBData\OilTankerWithDeadWeight.db'
MMSI_table = 'OilTankerFiles'


def data_connect(result_db, source_db_name):
    """
    将原始数据根据MMSI号来进行连接,并且将连接后的结果导入到一个新的数据库中
    :param result_db: 是用来保存最后结果的数据库
    :param source_db_name: 原始数据所在的数据库
    :return:
    """
    result_db.create_table(table_name + 'Final', FieldList + MMSI_field_list[2:], False)
    insert_list = ["INSERT INTO {} SELECT ".format(table_name + 'Final')]
    for info_field in FieldList:
        insert_list.append("%s.%s," % (table_name, info_field[0]))
    for dwt_field in MMSI_field_list[2:]:
        insert_list.append("%s.%s," % (MMSI_table, dwt_field[0]))
    insert_sql = "".join(insert_list)
    insert_sql = insert_sql[:-1] + (" FROM SourceDB.%s INNER JOIN %s ON SourceDB.%s.MMSI = %s.MMSI" %
                                    (table_name, MMSI_table, table_name, MMSI_table))
    result_db.import_data(source_db_name, insert_sql)

    return


def less_ship_error(sourde_db, table_name, less_ship_mmsi_csv_name):
    with open(less_ship_mmsi_csv_name, "r") as less_ship_mmsi_csv:
        less_ship_mmsi_reader = csv.reader(less_ship_mmsi_csv)
        for row in less_ship_mmsi_reader:
            sourde_db.clean_dirty_data(table_name, " WHERE MMSI = {}".format(row[0]))


def dirty_data_clean(result_db):
    result_db.clean_dirty_data(table_name, ConstSQL.MMSI_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.LENGTH_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.WIDTH_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.SHIP_TYPE_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.UTC_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.SOG_ERROR_FILTER)
    result_db.clean_dirty_data(table_name, ConstSQL.DRAFT_ERROR_FILTER)
    less_ship_error(result_db, table_name, less_ship_mmsi_csv_name)
    result_db.db_file.commit()


def data_process(result_db, source_db_name):
    """

    :param temp_db_name:用来存储筛选完后的数据
    :param source_db_name:原始数据所在的数据库
    :return:
    """
    # 建立起一个数据库的类
    # print(temp_db_name)
    # result_db = DBProcess(temp_db_name)

    # 数据筛选
    # result_db.create_table(table_name, FieldList)
    result_db.import_data(source_db_name, "INSERT INTO {} SELECT * FROM SourceDB.Tracks WHERE longitude BETWEEN"
                                          " 20 AND 142 AND latitude BETWEEN -11 AND 42".format(table_name))

    # dirty_data_clean(result_db)

    # 关闭数据库
    # result_db.close_db()

    return


def main():
    print('数据预处理')
    start_time = datetime.datetime.now()

    # 建立起一个数据库，用于保存最后的数据
    result_db = DBProcess(result_db_name)

    # if result_db.create_table(MMSI_table, MMSI_field_list, False):
    #     result_db.import_data(MMSI_db_name, "INSERT INTO {} SELECT * FROM SourceDB.{}".format(MMSI_table, MMSI_table))

    result_db.create_table(table_name, FieldList)

    # 遍历文件夹下的所有子文件，进行处理
    for db_file in os.listdir(db_file_path):
        if db_file.endswith('.db'):
            # 进行数据预处理，将每个原始数据.db文件进行筛选，然后剔除脏数据
            print(db_file)
            data_process(result_db, os.path.join(db_file_path, db_file))

            # 进行数据融合，将融合后的数据导入到一个新的数据库中
            # data_connect(result_db, os.path.join(temp_db_file_path, db_file))

    dirty_data_clean(result_db)
    result_db.db_file.close()

    end_time = datetime.datetime.now()
    print('所用时间为', (end_time - start_time).seconds, 's')
    return


if __name__ == '__main__':
    main()
