# -*- coding: utf-8 -*-
"""
Created on Thu Oct 11 11:01:00 2018

@author: Xiao
"""

import sqlite3 as db


class DBProcess(object):

    # 进行初始化，并且打开数据库
    def __init__(self, db_name):
        self.db_file = db.connect(db_name)
        self.dbcursor = self.db_file.cursor()

    # rol_list中保存着这个创建这个表需要的字段名称和字段类型
    def create_table(self, table_name, rol_list, delete_table=True):
        table_exists_judge = ("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{}'".format(table_name))
        table = self.dbcursor.execute(table_exists_judge).fetchone()

        if table[0] == 0:
            query_info = ['CREATE TABLE {}('.format(table_name)]
            for rol in rol_list:
                query_info.append('%s %s,' % (rol[0], rol[1]))
            sql_info = ''.join(query_info)
            sql_info = sql_info[:-1] + ')'
            self.dbcursor.execute(sql_info)
            self.db_file.commit()
            return True
        else:
            if delete_table:
                delete_sql = 'DELETE FROM {}'.format(table_name)
                self.dbcursor.execute(delete_sql)
                self.db_file.commit()
            return delete_table

    # 最通用的用于执行sql语句
    def run_sql(self, sql):
        self.dbcursor.execute(sql)
        self.db_file.commit()

    # 将全部的数据合并导入到一个表格中
    def import_data(self, source_db_name, sql):
        self.dbcursor.execute("ATTACH '{}' AS SourceDB".format(source_db_name))
        self.dbcursor.execute(sql)
        self.dbcursor.execute("DETACH SourceDB")
        self.db_file.commit()

    def clean_dirty_data(self, table_name, filter_query, ):
        self.dbcursor.execute("DELETE FROM {} {}".format(table_name, filter_query))

    # 剔除掉mmsi错误的数据
    def mmsi_error(self, table_name):
        self.dbcursor.execute("DELETE FROM {} Where MMSI > 999999999 OR MMSI < 100000000".format(table_name))
        self.db_file.commit()

    # 剔除掉属性缺失的数据
    def lack_error(self, table_name):
        self.dbcursor.execute("DELETE FROM {} WHERE sog is null OR draught is null or longitude is null or"
                              " latitude is null or MMSI is null or ts_pos_utc is null".format(table_name))
        self.db_file.commit()

    # 删除掉速度异常的数据
    def speed_error(self, table_name, speed_threshold):
        self.dbcursor.execute("DELETE FROM {} WHERE sog < 0 OR sog > {}".format(table_name, str(speed_threshold)))
        self.db_file.commit()

    # 删除掉吃水异常的数据
    def draught_error(self, table_name, draught_threshold):
        self.dbcursor.execute(
            "DELETE FROM {} WHERE draught <= 0 OR draught > {}".format(table_name, str(draught_threshold)))
        self.db_file.commit()

    # 对其中的每一条数据进行处理，将满足条间的数据写到.txt文件中去
    # 然后将.txt中的数据重新导入到.db文件中去
    def record_process(self, table_name, land_shp):
        """self.dbcursor.execute("SELECT DISTINCT * FROM " + table_name + "ORDER BY MMSI,ts_pos_utc")
        for row in self.dbcursor:
            shipdata = ShipData(row)
            # 空间位置判断
            for land in land_shp:
                if land.contains(shipdata.Point):
                    break
            if not land.contains(shipdata.Point):
                # 写入到CSV中去
                print 1
            pass"""

    # 关闭数据库
    def close_db(self):
        self.db_file.close()
