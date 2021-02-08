# -*- encoding: utf -*-
"""
Create on 2021/1/26 10:11
@author: Xiao Yijia
"""
from dao.ais_reader import AISReader
from dao.still_point_area import StillPointArea
from dao.trajectory import Trajectory

# input
input_sp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\StillPoint.csv"
input_trajectory_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\TrajectoryPoint.csv"

# output
output_sp_file_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\FinalStillPoint.csv"
output_sp_header = ["sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type", "length", "width", "longitude",
                    "latitude", "draft", "speed", "utc"]
output_trajectory_info_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\FinalTrajectoryInfo.csv"
output_trajectory_info_header = ["trajectory_index", "sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type",
                                 "length", "width"]
output_trajectory_txt_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\FinalShipTrajectory.txt"
output_trajectory_point_name = r"D:\ShipProgram\DoctorPaper\MSRData\TestData\FinalTrajectoryPoint.csv"
output_trajectory_point_header = ["trajectory_index", "sp_index", "mark", "mmsi", "imo", "vessel_name", "vessel_type",
                                  "length", "width", "longitude", "latitude", "draft", "speed", "utc"]

# const
sp_index = 0
sp_offset = 1
sp_mark = 13
trajectory_index = 1
trajectory_offset = 2


class AISPointMore(object):

    def __init__(self):
        self.target_sp_area = StillPointArea()
        self.target_trajectory = Trajectory()
        self.source_sp_area = None
        self.source_trajectory = None

    def join_trajectory(self, input_sp_file_name, input_trajectory_file_name, output_sp_file_name, output_sp_header,
                        output_trajectory_file_name, output_trajectory_header, output_trajectory_txt_name, ):
        self.init_input_output(input_sp_file_name, input_trajectory_file_name, output_sp_file_name, output_sp_header,
                               output_trajectory_file_name, output_trajectory_header, output_trajectory_txt_name, )
        self.deal_with_join_trajectory()
        self.finish_deal()

    def init_input_output(self, input_sp_file_name, input_trajectory_file_name, output_sp_file_name, output_sp_header,
                          output_trajectory_file_name, output_trajectory_header, output_trajectory_txt_name, ):
        # 打开输入输出
        self.source_sp_area = AISReader(input_sp_file_name, sp_index, sp_offset, sp_mark, )
        self.source_trajectory = AISReader(input_trajectory_file_name, trajectory_index, trajectory_offset, )

        self.source_sp_area.start_fetch_data()
        self.source_trajectory.start_fetch_data()

        self.target_sp_area.init_output_saver(output_sp_file_name, output_sp_header, )
        self.target_trajectory.init_output_saver(output_trajectory_file_name, output_trajectory_header,
                                                 output_trajectory_txt_name, output_trajectory_point_name,
                                                 output_trajectory_point_header)

    def deal_with_join_trajectory(self):
        print("deal the ship with mmsi: {}".format(self.source_sp_area.ais_point.mmsi))
        while self.source_sp_area.has_next_data() and self.source_trajectory.has_next_data():
            compare = self.source_trajectory.index - self.source_sp_area.index
            if compare < 0:
                print("!!!!!!!!!!!!!!!it would not happened in right situation")
                trajectory, _ = self.source_trajectory.fetch_data()
            elif compare > 0:
                self.deal_with_last_point()
                print("deal the ship with mmsi: {}".format(self.source_sp_area.ais_point.mmsi))
            else:
                self.deal_the_same_ship()

        while self.source_sp_area.has_next_data():
            self.deal_with_last_point()

    def deal_with_last_point(self):
        sp_area, is_suitable = self.source_sp_area.fetch_data()
        if is_suitable:
            self.target_trajectory.export_temp_trajectory_point(sp_area[0], self.target_sp_area.index - 1)
            self.target_sp_area.temp_still_point_set = sp_area
            self.target_sp_area.export_temp_still_point_set()

        self.target_sp_area.init_value()
        self.target_trajectory.init_value()

    def deal_the_same_ship(self):
        sp_area, is_suitable = self.source_sp_area.fetch_data()
        trajectory, _ = self.source_trajectory.fetch_data()
        self.target_trajectory.point_set = [sp_area[-1]] + trajectory
        self.target_sp_area.temp_still_point_set = sp_area
        if is_suitable:
            self.target_trajectory.export_temp_trajectory_point(sp_area[0], self.target_sp_area.index - 1)
            self.target_sp_area.export_temp_still_point_set()
            self.target_trajectory.is_ship_beginning = False
        else:
            self.target_trajectory.update_temp_trajectory_point(self.target_sp_area)

    def finish_deal(self):
        self.source_sp_area.close()
        self.source_trajectory.close()


if __name__ == '__main__':
    ais_point = AISPointMore()
    ais_point.join_trajectory(input_sp_file_name, input_trajectory_file_name, output_sp_file_name, output_sp_header,
                              output_trajectory_info_name, output_trajectory_info_header, output_trajectory_txt_name, )
