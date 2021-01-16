# -*- encoding: utf -*-
"""
Create on 2021/1/4 11:07
@author: Xiao Yijia
"""
import csv
import datetime
import os

import arcpy

from dao.DBClass import DBProcess
from const.ConstSQL import ConstSQL


class ErrorCount(object):

    def __init__(self, ocean_shp_name):
        self.total_count = 0
        self.ship_count = {}
        self.mmsi_error_count = 0
        self.length_error_count = 0
        self.width_error_count = 0
        self.ship_type_error_count = 0
        self.longitude_error_count = 0
        self.latitude_error_count = 0
        self.utc_error_count = 0
        self.sog_error_count = 0
        self.draft_error_count = 0
        self.position_error_count = 0
        self.share_mmsi_count = 0
        self.ocean_list = self.fetch_shp(ocean_shp_name)

    @staticmethod
    def fetch_shp(shp_name):
        shp_items = arcpy.da.SearchCursor(shp_name, ["SHAPE@"])
        shp_list = []
        for shp_item in shp_items:
            shp_list.append(shp_item[0])

        return shp_list

    def error_count_show(self, csv_file_name, less_ship_csv_name, source_db_name=None, source_path=None,
                         table_name=None):
        self.clean_original_count()
        if source_db_name is not None:
            self.init_error_count_from_single_db(source_db_name, table_name)
        elif source_path is not None:
            self.init_error_count_from_path(source_path, table_name)
        self.export_error_count_to_csv(csv_file_name, less_ship_csv_name)

    def clean_original_count(self):
        self.share_mmsi_count = 0
        self.position_error_count = 0
        self.draft_error_count = 0
        self.sog_error_count = 0
        self.utc_error_count = 0
        self.latitude_error_count = 0
        self.longitude_error_count = 0
        self.ship_type_error_count = 0
        self.width_error_count = 0
        self.length_error_count = 0
        self.mmsi_error_count = 0
        self.ship_count = {}
        self.total_count = 0

    def init_error_count_from_path(self, source_path, table_name, ):
        for file_name in os.listdir(source_path):
            if file_name.endswith(".db"):
                self.init_error_count_from_single_db(os.path.join(source_path, file_name), table_name, )

    def init_error_count_from_single_db(self, source_db_name, table_name, ):
        print(source_db_name)
        source_db = DBProcess(source_db_name)
        self.get_total_count(source_db, table_name)
        self.get_ship_and_count(source_db, table_name)
        self.get_mmsi_error_count(source_db, table_name)
        self.get_length_error_count(source_db, table_name)
        self.get_width_error_count(source_db, table_name)
        self.get_ship_type_error_count(source_db, table_name)
        self.get_longitude_error_count(source_db, table_name)
        self.get_latitude_error_count(source_db, table_name)
        self.get_utc_error_count(source_db, table_name)
        self.get_sog_error_count(source_db, table_name)
        self.get_draft_error_count(source_db, table_name)
        # self.get_position_error_count(source_db, table_name, )
        self.get_share_mmsi_count(source_db, table_name)

    # region count init
    def get_total_count(self, source_db, table_name):
        self.add_value(source_db, "total_count", ConstSQL.GET_COUNT.format(table_name))

    def get_ship_and_count(self, source_db, table_name, ):
        source_db.run_sql(ConstSQL.GET_ALL_SHIP_COUNT.format(table_name))
        for row in source_db.dbcursor:
            mmsi = row[0]
            if mmsi in self.ship_count:
                self.ship_count[row[0]] += int(row[1])
            else:
                self.ship_count[mmsi] = int(row[1])

    def get_mmsi_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.MMSI_ERROR_FILTER
        self.add_value(source_db, "mmsi_error_count", sql)

    def get_length_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.LENGTH_ERROR_FILTER
        self.add_value(source_db, "length_error_count", sql)

    def get_width_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.WIDTH_ERROR_FILTER
        self.add_value(source_db, "width_error_count", sql)

    def get_ship_type_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.SHIP_TYPE_ERROR_FILTER
        self.add_value(source_db, "ship_type_error_count", sql)

    def get_longitude_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.LONGITUDE_ERROR_FILTER
        self.add_value(source_db, "longitude_error_count", sql)

    def get_latitude_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.LATITUDE_ERROR_FILTER
        self.add_value(source_db, "latitude_error_count", sql)

    def get_utc_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.UTC_ERROR_FILTER
        self.add_value(source_db, "utc_error_count", sql)

    def get_sog_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.SOG_ERROR_FILTER
        self.add_value(source_db, "sog_error_count", sql)

    def get_draft_error_count(self, source_db, table_name, ):
        sql = ConstSQL.GET_COUNT.format(table_name) + ConstSQL.DRAFT_ERROR_FILTER
        self.add_value(source_db, "draft_error_count", sql)

    def get_position_error_count(self, source_db, table_name, ):
        source_db.run_sql(ConstSQL.GET_ALL_SHIP_POSITION.format(table_name))
        for row in source_db.dbcursor:
            ship_position = arcpy.Point(row[0], row[1])
            if not self.point_in_ocean(ship_position):
                self.position_error_count += 1

    def point_in_ocean(self, ship_position):
        for ocean in self.ocean_list:
            if ocean.contains(ship_position):
                return True
        return False

    def get_share_mmsi_count(self, source_db, table_name, ):
        pass

    def add_value(self, source_db, value_name, sql):
        source_db.run_sql(sql)
        for row in source_db.dbcursor:
            value = self.__getattribute__(value_name)
            self.__setattr__(value_name, int(row[0]) + value)

    # endregion

    def export_error_count_to_csv(self, error_info_file_name, less_ship_csv_name):
        with open(error_info_file_name, 'wb') as error_info_file:
            error_info_writer = csv.writer(error_info_file)

            error_info_writer.writerow(["error_type", "count", "percent"])
            self.write_mmsi_error_info(error_info_writer)
            self.write_length_error_info(error_info_writer)
            self.write_width_error_info(error_info_writer)
            self.write_ship_type_error_info(error_info_writer)
            self.write_longitude_error_info(error_info_writer)
            self.write_latitude_error_info(error_info_writer)
            self.write_utc_error_info(error_info_writer)
            self.write_sog_error_info(error_info_writer)
            self.write_draft_error_info(error_info_writer)
            self.write_less_ship_error_info(error_info_writer, less_ship_csv_name)
            self.write_position_error_info(error_info_writer)
            self.share_mmsi_error_info(error_info_writer)

    # region export to csv
    def write_mmsi_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "mmsi_error", self.mmsi_error_count)

    def write_length_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "length_error", self.length_error_count)

    def write_width_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "width_error", self.width_error_count)

    def write_ship_type_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "ship_type_error", self.ship_type_error_count)

    def write_longitude_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "longitude_error", self.longitude_error_count)

    def write_latitude_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "latitude_error", self.latitude_error_count)

    def write_utc_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "utc_error", self.utc_error_count)

    def write_sog_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "sog_error", self.sog_error_count)

    def write_draft_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "draft_error", self.draft_error_count)

    def write_less_ship_error_info(self, error_info_writer, less_ship_csv_name):
        total_ship_num = len(self.ship_count.keys())
        less_ship_mmsi_list = [k for k, v in self.ship_count.items() if v < 120]
        less_ship_num = len(less_ship_mmsi_list)
        error_percent = less_ship_num * 1.0 / total_ship_num
        error_info_writer.writerow(["less_ship_error", less_ship_num, error_percent])

        with open(less_ship_csv_name, "wb") as less_ship_csv:
            less_ship_writer = csv.writer(less_ship_csv)
            for mmsi in less_ship_mmsi_list:
                less_ship_writer.writerow([mmsi])

    def write_position_error_info(self, error_info_writer):
        self.export_error_count_and_percent(error_info_writer, "position_error", self.position_error_count)

    def share_mmsi_error_info(self, error_info_writer):
        pass

    def export_error_count_and_percent(self, error_info_writer, error_type, error_count):
        error_percent = error_count * 1.0 / self.total_count
        error_info_writer.writerow([error_type, error_count, error_percent])
    # endregion


def main():
    start_time = datetime.datetime.now()
    ocean_shp_name = r"D:\GeoData\OceanData\ne_10m_geography_marine_polys.shp"
    csv_file_name = r"D:\ShipProgram\DoctorPaper\Data\ErrorCount.csv"
    less_ship_csv_name = r"D:\ShipProgram\DoctorPaper\Data\LessShipMMSI.csv"
    source_db_name = None
    source_path = r"F:\NewShipsDB2017"
    table_name = "Tracks"

    error_count = ErrorCount(ocean_shp_name)
    error_count.error_count_show(csv_file_name, less_ship_csv_name, source_db_name, source_path, table_name)

    end_time = datetime.datetime.now()
    print("Using Time is :" + str((end_time - start_time)))


if __name__ == '__main__':
    main()
