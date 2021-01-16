# -*- coding: utf-8 -*-
import csv
import math

from dao.DBClass import DBProcess
from dao.Data import DetailData
from dao.StillPointArea import StillPointArea

# 输入输出参数
input_db_name = r'D:\ShipProgram\DoctorPaper\MSRData\DBData\CrudeOilTanker_MMSIIdentify.db'
output_folder = r'D:\ShipProgram\DoctorPaper\MSRData\FileData'
output_sp_center_csv = r'StillPointCenter.csv'
output_still_point_csv = r'StillPoint.csv'
output_route_point_csv_name = r'RoutePoint.csv'
output_rp_center_csv_name = r'RoutePointCenter.csv'
output_change_point_csv_name = r'ChangePoint.csv'
output_trajectory_txt_name = r'ShipTrajectoryLine.txt'

# 海岸线5km的图层
ocean_shp_name = r'D:\ShipProgram\DoctorPaper\MSRData\ShpData\coastlineBuffer5.shp'  # 输入陆地五公里缓冲区的图层
water_deep_raster = r'D:\GeoData\waterDepth\waterDepth.tif'

input_table_name = 'CrudeOilTankerFinal'

# region 静止点参数
# 单位为节
sp_speed_threshold = 0.5
# 单位为秒
sp_still_time_threshold = 21600
sp_time_gaps_threshold = 3600
sp_combine_time_threshold = 3600
# 单位为km
sp_distance_threshold = 1
sp_combine_distance_threshold = 1
# 单位为点的个数
sp_point_threshold = 3
# 中心点处的水深
sp_center_water_deep = -1000
# endregion

# region 航路点参数
# 速度阈值，单位为节
rp_speed_threshold = 1
# 点数阈值
rp_point_threshold = 3
# 航向阈值，单位为度
rp_heading_threshold = 10
# rp_avg_heading_threshold_set = [0, 1, 2, 3, 4, 5]
# rp_acceleration_threshold = 0
# 中心点处的水深
rp_center_water_deep = -5000
# endregion

# region 吃水变化点参数
draft_difference_threshold = 3
# endregion

# 输出索引
point_index = 1
count = sp_speed_threshold
polyline_index = 0

ocean_shp = arcpy.da.SearchCursor(ocean_shp_name, ["SHAPE@"])
ocean_list = []
for ocean in ocean_shp:
    ocean_list.append(ocean[0])


class AISPoint(object):

    def __init__(self, db_name):
        self.source_db = DBProcess(db_name)

        self.still_point_area = StillPointArea()
        self.no_still_point_set = []
        self.trajectory_point_set = []

        self.output_file = None
        self.output_saver = None

    def extract_still_point(self, sql, output_file_name, output_header):
        self.prepare_deal(sql, output_file_name, output_header)
        self.between_line_transaction_deal()
        self.finish()

    def prepare_deal(self, sql, output_file_name, output_header):
        self.start_transaction(sql)
        self.init_output_saver(output_file_name, output_header)

    def start_transaction(self, sql):
        print("starting fetch data!")
        self.source_db.run_sql(sql)
        print("fetch data finished!")

    def init_output_saver(self, file_name, output_header):
        if self.output_file is not None:
            self.output_file.close()
        self.output_file = open(file_name, "wb")
        self.output_saver = csv.writer(self.output_file)
        self.output_saver.writerow(output_header)

    def between_line_transaction_deal(self, ):
        self.init_value()

        before_ship = self.fetch_data()
        after_ship = self.fetch_data()

        while self.has_next_data(after_ship):
            if self.judge_first_situation(before_ship, after_ship):
                self.deal_first_situation()
            elif self.judge_second_situation(before_ship, after_ship):
                self.deal_second_situation(after_ship, )
            else:
                self.deal_default_situation()

            before_ship = after_ship
            after_ship = self.fetch_data()

        self.final_deal_situation(before_ship, before_ship, )

    def fetch_data(self, ):
        row = self.source_db.dbcursor.fetchone()
        if row is not None:
            return DetailData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                              row[11], )
        return None

    def init_value(self):
        self.still_point_area.init_value()
        self.no_still_point_set = []
        self.trajectory_point_set = []

    def has_next_data(self, after_info):
        return after_info

    def judge_first_situation(self, before_ship, after_ship):
        return not before_ship.judge_same_ship(after_ship)

    def deal_first_situation(self, ):
        self.final_deal_situation()
        self.init_value()

    def judge_second_situation(self, before_ship, after_ship):
        return self.is_still_point(before_ship, after_ship)

    def is_still_point(self, before_ship, after_ship):
        return (after_ship.speed < sp_speed_threshold and
                math.fabs(after_ship.utc - before_ship.utc) < sp_time_gaps_threshold and
                before_ship.calculate_distance_to_item(after_ship) < sp_distance_threshold)

    def deal_second_situation(self, after_ship, ):
        self.still_point_area.append_value(after_ship)

    def deal_default_situation(self, ):
        self.export_still_point_set()

    def export_still_point_set(self):
        if self.still_point_area.is_suit_for_combine_set(self.still_point_area.temp_still_point_set,
                                                         self.still_point_area.still_point_set):
            self.still_point_area.update_temp_still_point_set()
        else:
            self.still_point_area.export_point_set(self.output_saver, self.still_point_area.temp_still_point_set)
            self.still_point_area.export_temp_still_point_set(self.output_saver)

    def final_deal_situation(self, ):
        self.still_point_area.merge_still_point_set()
        self.still_point_area.export_all_point_set(self.output_saver)

    def finish(self):
        self.output_file.close()


if __name__ == '__main__':
    pass
