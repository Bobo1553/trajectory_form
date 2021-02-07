# -*- encoding: utf -*-
"""
Create on 2021/1/30 10:47
@author: Xiao Yijia
"""
import math

from const.const import Const
from dao.ais_reader import AISReader
from dao.route_point import RoutePoint
from service.port_service import PortService

# input
input_sp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\StillPoint.csv"
input_trajectory_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step3\TrajectoryPoint.csv"

port_shp_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\shp\wld_trs_ports_wfp.shp"

# output
output_sp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step4\FinalStillPoint.csv"
sp_header = ["sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type", "length", "width", "longitude",
             "latitude", "draft", "speed", "utc", "port_name"]
output_rp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\step4\RoutePoint.csv"
rp_header = ["sp_index", "rp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type", "length", "width",
             "longitude", "latitude", "draft", "speed", "utc"]

# const
sp_index = 0
sp_offset = 1
trajectory_index = 1
trajectory_offset = 2

# 航路点参数
# 速度阈值，单位为节
rp_speed_threshold = 2
# 点数阈值
rp_point_threshold = 10
# 航向阈值，单位为度
rp_heading_threshold = 20
rp_avg_heading_threshold = 5
rp_acceleration_threshold = 0

# 单位为度
port_search_distance_threshold = 0.1


class TrajectoryService(object):

    def __init__(self):
        self.target_route_point = RoutePoint()
        self.ais_state = Const.MOVING

        self.before_point, self.middle_point, self.after_point = [None, None, None]

        self.source_sp_area = None
        self.source_trajectory = None

    def extract_route_point(self, input_sp_file_name, input_trajectory_file_name, output_sp_file_name, sp_header,
                            output_rp_file_name, rp_header, port_service, ):
        self.init_input_output(input_sp_file_name, input_trajectory_file_name, output_sp_file_name, sp_header,
                               output_rp_file_name, rp_header, )
        self.deal_with_trajectory_construction(port_service)
        self.finish_deal()

    def init_input_output(self, input_sp_file_name, input_trajectory_file_name, output_sp_file_name, sp_header,
                          output_rp_file_name, rp_header, ):
        # input
        self.source_sp_area = AISReader(input_sp_file_name, sp_index, sp_offset, )
        self.source_trajectory = AISReader(input_trajectory_file_name, trajectory_index, trajectory_offset, )

        self.source_sp_area.start_fetch_data()
        self.source_trajectory.start_fetch_data()

        self.target_route_point.init_output_saver(output_sp_file_name, sp_header, output_rp_file_name, rp_header)

    def deal_with_trajectory_construction(self, port_service):
        print("deal the ship with mmsi: {}".format(self.source_sp_area.ais_point.mmsi))
        while self.source_sp_area.has_next_data() and self.source_trajectory.has_next_data():
            compare = self.source_trajectory.index - self.source_sp_area.index
            if self.source_sp_area.index == 33:
                pass
            if compare < 0:
                print("!!!!!!!!!!!!!!!it would not happened in right situation")
                trajectory, _ = self.source_trajectory.fetch_data()
            elif compare > 0:
                self.deal_with_last_point(port_service)
                print("deal the ship with mmsi: {}".format(self.source_sp_area.ais_point.mmsi))
            else:
                self.deal_with_same_ship(port_service)

    def deal_with_last_point(self, port_service):
        sp_area, _ = self.source_sp_area.fetch_data()
        if self.target_route_point.export_sp_center(sp_area, port_service, port_search_distance_threshold):
            self.fetch_route_point_and_export()

        self.target_route_point.temp_trajectory_set = []
        self.target_route_point.temp_still_point_info = []

    def deal_with_same_ship(self, port_service):
        sp_area, _ = self.source_sp_area.fetch_data()
        trajectory_set, _ = self.source_trajectory.fetch_data()
        if self.target_route_point.export_sp_center(sp_area, port_service, port_search_distance_threshold):
            self.fetch_route_point_and_export()
            self.target_route_point.temp_trajectory_set = trajectory_set
        else:
            self.target_route_point.temp_trajectory_set = []

    # region fetch route point
    def fetch_route_point_and_export(self, ):
        trajectory_set = self.target_route_point.fetch_temp_trajectory_set()
        self.target_route_point.route_index = 0
        self.target_route_point.route_point_set = []
        for i in range(1, len(trajectory_set) - 1):
            self.before_point, self.middle_point, self.after_point = trajectory_set[i - 1:i + 2]
            speed_change = self.get_speed_change()
            heading_change = self.get_heading_change()

            if self.enter_route_process(speed_change, heading_change):
                self.deal_with_situation_of_enter(speed_change, heading_change, )
            elif self.out_route_process(speed_change):
                self.deal_with_situation_of_quit()
            else:
                self.deal_with_default_situation(speed_change, heading_change, )

    def get_speed_change(self):
        after_speed = self.after_point.calculate_avg_speed_to_item(self.middle_point)
        middle_speed = self.middle_point.calculate_avg_speed_to_item(self.before_point)
        return after_speed - middle_speed

    def get_heading_change(self, ):
        after_heading = self.after_point.calculate_heading_to_item(self.middle_point)
        middle_point = self.before_point.calculate_heading_to_item(self.before_point)
        return math.fabs(after_heading - middle_point)

    def enter_route_process(self, speed_change, heading_change):
        return 0 - speed_change > rp_speed_threshold and heading_change > rp_heading_threshold

    def deal_with_situation_of_enter(self, speed_change, heading_change, ):
        if self.ais_state == Const.MOVING:
            self.deal_moving_to_route_situation(speed_change, heading_change, )
        else:
            self.deal_route_situation(speed_change, heading_change, )

    def out_route_process(self, speed_change):
        return speed_change > rp_speed_threshold

    def deal_with_situation_of_quit(self):
        if self.ais_state == Const.ROUTE:
            self.deal_route_to_moving_situation()
        else:
            self.deal_moving_situation()

    def deal_with_default_situation(self, speed_change, heading_change, ):
        if self.ais_state == Const.ROUTE:
            self.deal_route_situation(speed_change, heading_change, )
        else:
            self.deal_moving_situation()

    def deal_moving_to_route_situation(self, speed_change, heading_change, ):
        self.target_route_point.route_point_set = [
            [self.middle_point, speed_change / math.fabs(self.after_point.utc - self.middle_point.utc), heading_change]]
        self.ais_state = Const.ROUTE

    def deal_route_situation(self, speed_change, heading_change, ):
        self.target_route_point.route_point_set.append(
            [self.middle_point, speed_change / math.fabs(self.after_point.utc - self.middle_point.utc), heading_change])

    def deal_route_to_moving_situation(self):
        self.target_route_point.export_route_center(rp_point_threshold, rp_acceleration_threshold,
                                                    rp_avg_heading_threshold)
        self.ais_state = Const.MOVING

    def deal_moving_situation(self):
        pass

    # endregion

    def finish_deal(self):
        self.source_sp_area.close()
        self.source_trajectory.close()

        self.target_route_point.close()


if __name__ == '__main__':
    port_service = PortService(port_shp_name)
    print("fetch port info finish!")

    trajectory_service = TrajectoryService()
    trajectory_service.extract_route_point(input_sp_file_name, input_trajectory_file_name, output_sp_file_name,
                                           sp_header, output_rp_file_name, rp_header, port_service)
