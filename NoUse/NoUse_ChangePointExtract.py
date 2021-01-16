# -*- coding: utf-8 -*-

import csv
import math
from dao import DetailDataWithBuiltTime

# 输入输出
# 输入为静止点区域的所有静止点
input_still_point_csv = r'D:\ShipProgram\MainRouteExtraction2020\MSRData\FileData\StillPoint.csv'
# 输出为吃水变化点
output_change_point_csv = r'D:\ShipProgram\DoctorPaper\MSRData\FileData\ChangePoint.csv'

# 参数
draft_difference_threshold = 0.01
same_draft_point_number_threshold = 0
before_or_after = 'before'


def init():
    return DetailDataWithBuiltTime(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), False, 0


def deal_before_ship(mark_ship, draft_change_mark, same_draft_point_count, writer):
    if draft_change_mark and same_draft_point_count > same_draft_point_number_threshold:
        writer.writerow(mark_ship.export_to_csv())
        print(mark_ship.mark, mark_ship.mmsi, mark_ship.utc, mark_ship.draught)


def extract_change_point():
    with open(input_still_point_csv, 'r') as still_points:
        still_points_reader = csv.reader(still_points)

        head = next(still_points_reader)

        with open(output_change_point_csv, 'wb') as change_points:
            change_points_writer = csv.writer(change_points)

            change_points_writer.writerow(head[:-1])

            row = next(still_points_reader)
            before_ship = DetailDataWithBuiltTime(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            after_ship = DetailDataWithBuiltTime(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                 row[9], row[10], float(row[11]), row[12], row[13], row[14])
            before_point_index = row[-1]

            mark_ship, draft_change_mark, same_draft_point_count = init()

            for row in still_points_reader:
                # if still_points_reader.line_num == 300:
                #     break
                before_ship.set_value_by_item(after_ship)
                after_ship = DetailDataWithBuiltTime(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                     row[9], row[10], float(row[11]), row[12], row[13], row[14])
                # 代表不同的静止点区域
                if before_point_index != row[-1]:
                    deal_before_ship(mark_ship, draft_change_mark, same_draft_point_count, change_points_writer)
                    mark_ship, draft_change_mark, same_draft_point_count = init()

                # 前后点的吃水深度满足阈值
                elif math.fabs(after_ship.draught - before_ship.draught) > draft_difference_threshold:
                    deal_before_ship(mark_ship, draft_change_mark, same_draft_point_count, change_points_writer)
                    mark_ship, draft_change_mark, same_draft_point_count = init()
                    draft_change_mark = True
                    same_draft_point_count += 1
                    if before_or_after == 'before':
                        mark_ship.set_value_by_item(before_ship)
                    else:
                        mark_ship.set_value_by_item(after_ship)

                # 前后点的吃水值相等
                elif after_ship.draught == before_ship.draught:
                    if draft_change_mark:
                        same_draft_point_count += 1
                    pass

                # 啥也不是
                else:
                    deal_before_ship(mark_ship, draft_change_mark, same_draft_point_count, change_points_writer)
                    mark_ship, draft_change_mark, same_draft_point_count = init()
                before_point_index = row[-1]
        pass


if __name__ == '__main__':
    extract_change_point()
