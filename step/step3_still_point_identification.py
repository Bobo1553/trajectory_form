# -*- coding: utf-8 -*-
import math

import arcpy

from const.const import Const
from dao.common_db import DBProcess
from dao.data import DetailData
from dao.still_point_area import StillPointArea

# 输入输出参数
from dao.trajectory import Trajectory

input_db_name = r'D:\ShipProgram\DoctorPaper\MSRData\TestData\step2\bulk.db'
table_name = 'Tracks'
sql = ('select mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude, draught, speed, utc '
       'from {} order by mmsi, mark, utc'.format(table_name))

test_sql = ('select mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude, draught, speed, utc '
            'from {} where mmsi = 209292000 order by mmsi, mark, utc'.format(table_name))
# 港口图层
port_shp_name = r'D:\ShipProgram\DoctorPaper\MSRData\TestData\shp\coastline_area_10dis.shp'  # 输入港口图层

output_sp_csv = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\StillPoint.csv"
output_sp_header = ["sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type", "length", "width",
                    "longitude", "latitude", "draft", "speed", "utc"]
output_trajectory_csv = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\TrajectoryInfo.csv"
output_trajectory_header = ["trajectory_index", "sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type",
                            "length", "width"]
output_trajectory_txt_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\ShipTrajectory.txt"
output_trajectory_point_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\TrajectoryPoint.csv"
output_trajectory_point_header = ["trajectory_index", "sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type",
                                  "length", "width", "longitude", "latitude", "draft", "speed", "utc"]

# region 静止点参数
# 判断是否为停留点的速度阈值，单位为节
sp_speed_threshold = 1
# 判断是否为停留点的时间间隔阈值，单位为秒
sp_time_gaps_threshold = 172800
# 判断是否为停留点的距离阈值，单位为km
sp_distance_threshold = 2
# 判断是否为停留点的时候，是否考虑空间位置
is_consider_position = False

# 判断是否合并相邻停留区的时间间隔阈值，单位为秒
sp_combine_time_threshold = 3600
# 判断是否合并相邻停留区的距离阈值，单位为km
sp_combine_distance_threshold = 2

# 判断是否保留停留区域的持续时长阈值，单位为秒
sp_still_time_threshold = 0
# 判断是否保留停留区的个数，单位为点的个数
sp_point_threshold = 0
# endregion

port_shp = arcpy.da.SearchCursor(port_shp_name, ["SHAPE@"])
port_list = []
for port in port_shp:
    port_list.append(port[0])


class AISPoint(object):

    def __init__(self, db_name, ):
        self.source_db = DBProcess(db_name, )

        self.still_point_area = StillPointArea()
        self.trajectory = Trajectory()

        self.ais_state = Const.STILL

    def extract_still_point(self, sql, sp_file_name, sp_header, trajectory_file_name, trajectory_header,
                            trajectory_txt_name, trajectory_point_name, trajectory_point_header):
        self.prepare_deal(sql, sp_file_name, sp_header, trajectory_file_name, trajectory_header, trajectory_txt_name,
                          trajectory_point_name, trajectory_point_header)
        self.between_line_transaction_deal()
        self.finish()

    def prepare_deal(self, sql, sp_file_name, sp_header, trajectory_file_name, trajectory_header, trajectory_txt_name,
                     trajectory_point_name, trajectory_point_header, ):
        self.start_transaction(sql)
        self.still_point_area.init_output_saver(sp_file_name, sp_header)
        self.trajectory.init_output_saver(trajectory_file_name, trajectory_header, trajectory_txt_name,
                                          trajectory_point_name, trajectory_point_header, )

    def start_transaction(self, sql):
        print("starting fetch data!")
        self.source_db.run_sql(sql)
        print("fetch data finished!")

    def between_line_transaction_deal(self, ):
        self.init_value()

        before_ship = self.fetch_data()
        after_ship = self.fetch_data()

        print("deal the ship with mmsi: {}".format(before_ship.mmsi))
        while self.has_next_data(after_ship):
            if self.judge_first_situation(before_ship, after_ship):
                print("deal the ship with mmsi: {}".format(after_ship.mmsi))
                self.deal_first_situation()
            elif self.judge_second_situation(before_ship, after_ship):
                self.deal_second_situation(before_ship, after_ship, )
            else:
                self.deal_default_situation(before_ship, after_ship)

            before_ship = after_ship
            after_ship = self.fetch_data()

        self.final_deal_situation()

    def init_value(self):
        self.still_point_area.init_value()
        self.trajectory.init_value()

    def fetch_data(self, ):
        row = self.source_db.dbcursor.fetchone()
        if row is not None:
            return DetailData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                              row[11], )
        return None

    def has_next_data(self, after_info):
        return after_info

    def judge_first_situation(self, before_ship, after_ship):
        return not before_ship.judge_same_ship(after_ship)

    def deal_first_situation(self, ):
        self.final_deal_situation()
        self.init_value()

    def judge_second_situation(self, before_ship, after_ship):
        return (self.is_still_point(before_ship, after_ship) and
                (not is_consider_position or after_ship.judge_in_shp(port_list)))

    def is_still_point(self, before_ship, after_ship):
        return (after_ship.speed < sp_speed_threshold and
                math.fabs(after_ship.utc - before_ship.utc) < sp_time_gaps_threshold and
                before_ship.calculate_distance_to_item(after_ship) < sp_distance_threshold)

    def deal_second_situation(self, before_ship, after_ship, ):
        if self.ais_state == Const.STILL:
            self.deal_still_situation(after_ship)
        else:
            self.deal_moving_to_still_situation(before_ship, after_ship)

    def deal_still_situation(self, ship_point):
        self.still_point_area.append_value(ship_point)

    def deal_moving_to_still_situation(self, before_ship, after_ship, ):
        self.ais_state = Const.STILL
        if before_ship.speed < sp_speed_threshold:
            self.trajectory.remove_last_value()
            self.still_point_area.append_value(before_ship)
        self.still_point_area.append_value(after_ship)

    def deal_default_situation(self, before_ship, after_ship):
        if self.ais_state == Const.MOVING:
            self.deal_moving_situation(after_ship)
        else:
            self.deal_still_to_moving_situation(before_ship, after_ship)

    def deal_moving_situation(self, ship_point):
        self.trajectory.append_value(ship_point)

    def deal_still_to_moving_situation(self, before_ship, after_ship):
        self.ais_state = Const.MOVING

        self.merge_and_export_still_point_and_trajectory()

        self.trajectory.point_set = [before_ship, after_ship]

    def merge_and_export_still_point_and_trajectory(self):
        if self.still_point_area.merge_still_point_set(self.trajectory, sp_combine_time_threshold,
                                                       sp_combine_distance_threshold, ):
            return

        if self.still_point_area.is_suitable_point_set(self.still_point_area.temp_still_point_set, sp_point_threshold,
                                                       sp_still_time_threshold):
            self.trajectory.export_temp_trajectory_point(self.still_point_area.temp_still_point_set[0],
                                                         self.still_point_area.index - 1)
            self.still_point_area.export_temp_still_point_set()
        else:
            self.trajectory.update_temp_trajectory_point(self.still_point_area)

    def final_deal_situation(self):
        self.still_point_area.merge_still_point_set(self.trajectory, sp_combine_time_threshold,
                                                    sp_combine_distance_threshold, )

        while self.still_point_area.temp_still_point_set or self.still_point_area.still_point_set:
            if self.still_point_area.is_suitable_point_set(self.still_point_area.temp_still_point_set,
                                                           sp_point_threshold, sp_still_time_threshold):
                self.trajectory.export_temp_trajectory_point(self.still_point_area.temp_still_point_set[0],
                                                             self.still_point_area.index - 1)
                self.still_point_area.export_temp_still_point_set()
            else:
                self.trajectory.update_temp_trajectory_point(self.still_point_area)

    def finish(self):
        self.still_point_area.finish()
        self.trajectory.finish()


if __name__ == '__main__':
    ais_point = AISPoint(input_db_name)
    ais_point.extract_still_point(sql, output_sp_csv, output_sp_header, output_trajectory_csv, output_trajectory_header,
                                  output_trajectory_txt_name, output_trajectory_point_name,
                                  output_trajectory_point_header)
