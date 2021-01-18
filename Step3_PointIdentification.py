# -*- coding: utf-8 -*-

import csv
import math
import arcpy
import os

from dao.DBClass import DBProcess
from dao.Data import DetailData

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


def radians(x):
    return x * math.pi / 180


def calculate_distance_between_items(lat1, lat2, lon1, lon2):
    R = 6371
    avgLat = radians(lat1 + lat2) / 2
    disLat = R * math.cos(avgLat) * radians(lon1 - lon2)
    disLon = R * radians(lat1 - lat2)
    return math.sqrt(disLat * disLat + disLon * disLon)


def get_center_point(point_set):
    center = [0, 0]
    for point in point_set:
        center[0] += point.ship_position.X
        center[1] += point.ship_position.Y
    center[0] = center[0] * 1.0 / len(point_set)
    center[1] = center[1] * 1.0 / len(point_set)
    return center


def combine_set(before_point_set, after_point_set):
    time_gaps = after_point_set[0].utc - before_point_set[-1].utc
    after_point_center = get_center_point(after_point_set)
    before_point_center = get_center_point(before_point_set)
    distance = calculate_distance_between_items(before_point_center[1], after_point_center[1], before_point_center[0],
                                                after_point_center[0])
    return time_gaps < sp_combine_time_threshold and distance < sp_combine_distance_threshold


def is_sp_center_meet_conditions(point_set):
    return True
    center = get_center_point(point_set)
    center_point = arcpy.Point(center[0], center[1])
    water_deep = get_raster_data(center_point, water_deep_raster)
    if water_deep != "NoData" and water_deep < sp_center_water_deep:
        return False
    for ocean in ocean_list:
        if ocean.contains(center_point):
            return True
    return False


def save_still_point_set(still_point_set):
    if not still_point_set:
        return False

    return len(still_point_set) > sp_point_threshold and math.fabs(
        still_point_set[-1].utc - still_point_set[0].utc) > sp_still_time_threshold and is_sp_center_meet_conditions(
        still_point_set)


def is_still_point(before_ship, after_ship):
    # return after_ship.speed < sp_speed_threshold and \
    #            before_ship.calculate_distance_to_item(after_ship) < sp_distance_threshold
    return after_ship.speed < sp_speed_threshold and \
           math.fabs(after_ship.utc - before_ship.utc) < sp_time_gaps_threshold and \
           before_ship.calculate_distance_to_item(after_ship) < sp_distance_threshold


def get_raster_data(center_point, water_deep_raster_name):
    position = str(center_point.X) + " " + str(center_point.Y)
    data = arcpy.GetCellValue_management(water_deep_raster_name, position).getOutput(0)
    return data


def save_route_point_set(route_point_set, rp_point_threshold):
    if len(route_point_set) <= rp_point_threshold:
        return False

    # center = get_center_point(route_point_set)
    # center_point = arcpy.Point(center[0], center[1])
    # water_deep = get_raster_data(center_point, water_deep_raster)
    # if water_deep != "NoData" and water_deep < rp_center_water_deep:
    #     return False

    # avg_acceleration = 0
    # avg_heading_change = 0
    # for route_point in route_point_set:
    #     avg_heading_change += route_point[2]
    #     avg_acceleration += route_point[1]
    # avg_acceleration /= len(route_point_set)
    # avg_heading_change /= len(route_point_set)
    return True


def route_point_extract(point_set, rp_csv_writer, rp_center_csv_wrtier, rp_point_threshold, rp_heading_threshold, ):
    global point_index
    is_append = False
    temp_route_point_set = []
    for i in range(2, len(point_set)):
        before_point = point_set[i - 1]
        after_point = point_set[i]
        after_speed = after_point.calculate_avg_speed_to_item(before_point)
        before_speed = before_point.calculate_avg_speed_to_item(point_set[i - 2])
        after_heading = after_point.calculate_heading_to_item(before_point)
        before_heading = before_point.calculate_heading_to_item(point_set[i - 2])
        speed_change = after_speed - before_speed
        heading_change = math.fabs(after_heading - before_heading)
        if 0 - speed_change > rp_speed_threshold and heading_change > rp_heading_threshold:
            is_append = True
        if speed_change > rp_speed_threshold:
            is_append = False
        if is_append:
            temp_route_point_set.append(
                [after_point, speed_change / math.fabs(after_point.utc - before_point.utc), heading_change])
        else:
            if save_route_point_set(temp_route_point_set, rp_point_threshold):
                center = [0, 0]
                for temp_route_point in temp_route_point_set:
                    rp_csv_writer.writerow(temp_route_point[0].export_to_csv() + [point_index])
                    center[0] += temp_route_point[0].ship_position.X
                    center[1] += temp_route_point[0].ship_position.Y
                center[0] /= len(temp_route_point_set)
                center[1] /= len(temp_route_point_set)
                rp = temp_route_point_set[0][0]
                rp_center_csv_wrtier.writerow(
                    [rp.mark, rp.mmsi, rp.imo, rp.vessel_name, rp.vessel_type, rp.length, rp.width, rp.dead_weight,
                     rp.gross_weight, center[0], center[1], rp.built_time, point_index]
                )
                point_index += 1
            temp_route_point_set = []

    return


def is_near_coastline(after_ship):
    """
    判断是否在海岸线附近
    :param after_ship:
    :return:
    """
    return after_ship.judge_in_shp(ocean_list)


def get_average_draught(point_set):
    draught = 0.0
    for point in point_set:
        draught += point.draught
    return draught / len(point_set)


def is_satisfy_change_point(before_still_point_set, after_still_point_set):
    before_draught = get_average_draught(before_still_point_set)
    after_draught = get_average_draught(after_still_point_set)
    if math.fabs(before_draught - after_draught) > draft_difference_threshold:
        return True
    else:
        return False


def trajectory_point_extract(no_still_point_set, end_point, trajectory_writer):
    global polyline_index
    trajectory_writer.write(str(polyline_index) + ' 0\n')
    for i in range(len(no_still_point_set)):
        ship_position = no_still_point_set[i].ship_position
        trajectory_writer.write(
            str(i) + ' ' + str(ship_position.X) + ' ' + str(ship_position.Y) + ' 1.#QNAN 1.#QNAN\n')
    trajectory_writer.write(str(len(no_still_point_set)) + ' ' + str(end_point.ship_position.X) + ' ' +
                            str(end_point.ship_position.Y) + ' 1.#QNAN 1.#QNAN\n')
    polyline_index += 1
    pass


def draft_change(before_still_point_set, after_still_point_set):
    before_draught = get_average_draught(before_still_point_set)
    after_draught = get_average_draught(after_still_point_set)
    return before_draught - after_draught


def trajectory_deal(before_set, after_set, trajectory_set, change_point_writer, trajectory_writer):
    if not before_set:
        return after_set, []

    draft_difference = draft_change(before_set, after_set)
    if math.fabs(draft_difference) > draft_difference_threshold:
        change_point_writer.writerow(
            trajectory_set[0].export_to_csv() + [draft_difference, polyline_index])
        change_point_writer.writerow(
            after_set[-1].export_to_csv() + [draft_difference, polyline_index])
        trajectory_point_extract(trajectory_set, after_set[-1], trajectory_writer)
        return after_set, []
    else:
        return before_set, trajectory_set


def still_point_identify(result_folder, rp_heading_threshold, rp_point_threshold):
    """

    :param result_folder:
    :param rp_heading_threshold:
    :param rp_point_threshold:
    :return:
    """
    global point_index
    global polyline_index
    # region 中间参数
    still_point_set = []
    temp_still_point_set = []
    before_before_still_point_set = []
    no_still_point_set = []
    trajectory_point_set = []
    # endregion

    # 数据读取
    source_db = DBProcess(input_db_name)
    print('读取数据中...')
    # 读取数据
    # source_db.run_sql('select mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude, draught, '
    #                   'speed, utc, from {} order by mmsi, mark, utc'.format(input_table_name))

    # For Test
    source_db.run_sql("select mark, mmsi, imo, vessel_name, vessel_type, length, width, longitude, latitude, draught, "
                      "speed, utc from {} where MMSI = '209292000' order by mmsi, mark, utc".format(input_table_name))
    print('读取数据完毕')

    # region 保存文件
    # 保存航路点文件
    rp_center_file = open(result_folder + '\\' + output_rp_center_csv_name, 'wb')
    rp_center_file_writer = csv.writer(rp_center_file)
    rp_center_file_writer.writerow(
        ['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width', 'dead_weight', 'gross_weight',
         'longitude', 'latitude', 'built_time', 'point_index'])

    # 保存航路点的中心文件
    route_point_file = open(result_folder + '\\' + output_route_point_csv_name, 'wb')
    route_point_file_writer = csv.writer(route_point_file)
    route_point_file_writer.writerow(
        ['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width', 'dead_weight', 'gross_weight',
         'longitude', 'latitude', 'draught', 'speed', 'utc', 'built_time', 'point_index'])

    # 保存吃水变化点的文件
    change_point_file = open(result_folder + '\\' + output_change_point_csv_name, 'wb')
    change_point_file_writer = csv.writer(change_point_file)
    change_point_file_writer.writerow(
        ['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width', 'dead_weight', 'gross_weight',
         'longitude', 'latitude', 'draught', 'speed', 'utc', 'built_time', 'draft_difference', 'point_index'])

    trajectory_point_writer = open(result_folder + '\\' + output_trajectory_txt_name, 'w')
    trajectory_point_writer.write('Polyline\n')

    # endregion

    # 保存静止点中心文件
    with open(result_folder + '\\' + output_sp_center_csv, 'wb') as center_file:
        center_file_writer = csv.writer(center_file)
        center_file_writer.writerow(
            ['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width', 'longitude', 'latitude',
             'point_index'])

        # 保存静止点文件
        with open(result_folder + '\\' + output_still_point_csv, 'wb') as still_point_file:
            still_point_file_writer = csv.writer(still_point_file)
            still_point_file_writer.writerow(
                ['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width',
                 'longitude', 'latitude', 'draught', 'speed', 'utc', 'point_index'])

            # 初始化数据
            row = source_db.dbcursor.next()
            before_ship = DetailData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                     row[10], row[11], )

            for row in source_db.dbcursor:
                after_ship = DetailData(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                        row[10], row[11], )
                # 如果不是同一艘船舶
                if not before_ship.judge_same_ship(after_ship):
                    # 把前一艘船舶的事情解决
                    # 判断该集合是否满足要求
                    if not save_still_point_set(still_point_set):
                        still_point_set = []
                    # 判断前一个集合是否为空
                    elif temp_still_point_set:
                        if combine_set(temp_still_point_set, still_point_set):
                            temp_still_point_set = temp_still_point_set + still_point_set
                            still_point_set = []
                        else:
                            route_point_extract(no_still_point_set, route_point_file_writer, rp_center_file_writer,
                                                rp_point_threshold, rp_heading_threshold)
                    if temp_still_point_set:
                        # 输出所有的temp
                        for temp_still_point in temp_still_point_set:
                            still_point_file_writer.writerow(temp_still_point.export_to_csv() + [point_index])
                        temp_still_point_center = get_center_point(temp_still_point_set)
                        temp_still_point = temp_still_point_set[0]
                        center_file_writer.writerow(
                            [temp_still_point.mark, temp_still_point.mmsi, temp_still_point.imo,
                             temp_still_point.vessel_name, temp_still_point.vessel_type, temp_still_point.length,
                             temp_still_point.width, temp_still_point_center[0], temp_still_point_center[1],
                             point_index]
                        )
                        point_index += 1
                        before_before_still_point_set, trajectory_point_set = trajectory_deal(
                            before_before_still_point_set, temp_still_point_set, trajectory_point_set,
                            change_point_file_writer, trajectory_point_writer)
                        trajectory_point_set += [temp_still_point_set[-1]] + no_still_point_set
                    if still_point_set:
                        for still_point in still_point_set:
                            still_point_file_writer.writerow(still_point.export_to_csv() + [point_index])
                        still_point_center = get_center_point(still_point_set)
                        still_point = still_point_set[0]
                        center_file_writer.writerow(
                            [still_point.mark, still_point.mmsi, still_point.imo, still_point.vessel_name,
                             still_point.vessel_type, still_point.length, still_point.width, still_point_center[0],
                             still_point_center[1], point_index]
                        )
                        point_index += 1
                        before_before_still_point_set, trajectory_point_set = trajectory_deal(
                            before_before_still_point_set, still_point_set, trajectory_point_set,
                            change_point_file_writer, trajectory_point_writer)

                    # 新的一艘船舶的初始化
                    still_point_set = []
                    temp_still_point_set = []
                    before_before_still_point_set = []
                    no_still_point_set = []
                    trajectory_point_set = []

                # 如果是静止点的话
                elif is_still_point(before_ship, after_ship):
                    # if is_near_coastline(after_ship):
                    still_point_set.append(after_ship)

                # 进行静止点处理
                else:
                    # 判断该集合是否满足要求
                    if not save_still_point_set(still_point_set):
                        pass
                    # 判断前一个集合是否为空
                    elif not temp_still_point_set:
                        temp_still_point_set = still_point_set
                        no_still_point_set = []
                    else:
                        # 判断两个集合是否合并
                        if combine_set(temp_still_point_set, still_point_set):
                            temp_still_point_set = temp_still_point_set + still_point_set
                        else:
                            # 输出所有的temp
                            for temp_still_point in temp_still_point_set:
                                still_point_file_writer.writerow(temp_still_point.export_to_csv() + [point_index])
                            temp_still_point_center = get_center_point(temp_still_point_set)
                            temp_still_point = temp_still_point_set[0]
                            center_file_writer.writerow(
                                [temp_still_point.mark, temp_still_point.mmsi, temp_still_point.imo,
                                 temp_still_point.vessel_name, temp_still_point.vessel_type, temp_still_point.length,
                                 temp_still_point.width, temp_still_point_center[0], temp_still_point_center[1],
                                 point_index])
                            point_index += 1

                            before_before_still_point_set, trajectory_point_set = trajectory_deal(
                                before_before_still_point_set, temp_still_point_set, trajectory_point_set,
                                change_point_file_writer, trajectory_point_writer)
                            trajectory_point_set += [temp_still_point_set[-1]] + no_still_point_set
                            # 非静止点提取航路点
                            route_point_extract(no_still_point_set, route_point_file_writer, rp_center_file_writer,
                                                rp_point_threshold, rp_heading_threshold)
                            temp_still_point_set = still_point_set
                        no_still_point_set = []
                    still_point_set = []
                    no_still_point_set.append(after_ship)

                before_ship.set_value_by_item(after_ship)

            # 最后一点的处理
            # 判断该集合是否满足要求
            if not save_still_point_set(still_point_set):
                still_point_set = []
            # 判断前一个集合是否为空
            elif temp_still_point_set:
                if combine_set(temp_still_point_set, still_point_set):
                    temp_still_point_set = temp_still_point_set + still_point_set
                    still_point_set = []
                    before_before_still_point_set, trajectory_point_set = trajectory_deal(
                        before_before_still_point_set, temp_still_point_set, trajectory_point_set,
                        change_point_file_writer, trajectory_point_writer)
                else:
                    route_point_extract(no_still_point_set, route_point_file_writer, rp_center_file_writer,
                                        rp_point_threshold, rp_heading_threshold)
            if temp_still_point_set:
                # 输出所有的temp
                for temp_still_point in temp_still_point_set:
                    still_point_file_writer.writerow(temp_still_point.export_to_csv() + [point_index])
                temp_still_point_center = get_center_point(temp_still_point_set)
                temp_still_point = temp_still_point_set[0]
                center_file_writer.writerow(
                    [temp_still_point.mark, temp_still_point.mmsi, temp_still_point.imo,
                     temp_still_point.vessel_name, temp_still_point.vessel_type, temp_still_point.length,
                     temp_still_point.width, temp_still_point_center[0], temp_still_point_center[1], point_index]
                )
                point_index += 1

                before_before_still_point_set, trajectory_point_set = trajectory_deal(
                    before_before_still_point_set, temp_still_point_set, trajectory_point_set,
                    change_point_file_writer, trajectory_point_writer)
                trajectory_point_set += [temp_still_point_set[-1]] + no_still_point_set

            if still_point_set:
                for still_point in still_point_set:
                    still_point_file_writer.writerow(still_point.export_to_csv() + [point_index])
                still_point_center = get_center_point(still_point_set)
                still_point = still_point_set[0]
                center_file_writer.writerow(
                    [still_point.mark, still_point.mmsi, still_point.imo, still_point.vessel_name,
                     still_point.vessel_type, still_point.length, still_point.width, still_point_center[0],
                     still_point_center[1], point_index]
                )
                point_index += 1

                before_before_still_point_set, trajectory_point_set = trajectory_deal(
                    before_before_still_point_set, still_point_set, trajectory_point_set,
                    change_point_file_writer, trajectory_point_writer)

    route_point_file.close()
    rp_center_file.close()
    change_point_file.close()
    trajectory_point_writer.write('END')
    trajectory_point_writer.close()
    return


def main():
    global point_index
    point_index = 1
    result_folder = output_folder
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)
    print(result_folder)
    still_point_identify(result_folder, rp_heading_threshold, rp_point_threshold)
    return


if __name__ == '__main__':
    main()
