# coding=utf8

import csv

from dao.data import DetailData
from util.calculate_weight import calculate_change_dead_weight

input_csv_name = r'D:\ShipProgram\DoctorPaper\MSRData\FileData\ChangePoint.csv'
output_csv_name = r'D:\ShipProgram\DoctorPaper\MSRData\FileData\ChangePointWithChangeWeight.csv'


def calculate_weight():
    """
    输入为吃水变化点的.csv文件
    输出为包含有载重变化结果的吃水变化点的.csv文件
    :return:
    """
    with open(input_csv_name, 'r') as source_csv:
        source_csv_reader = csv.reader(source_csv)
        source_csv_reader.next()

        with open(output_csv_name, 'wb') as output_csv:
            output_csv_writer = csv.writer(output_csv)
            output_csv_writer.writerow(['mark', 'mmsi', 'imo', 'vessel_name', 'vessel_type', 'length', 'width',
                                        'dead_weight', 'gross_weight', 'longitude', 'latitude', 'draught', 'speed',
                                        'utc', 'built_time', 'draft_differ', 'line_num', 'change_weight'])

        for row in source_csv_reader:
            before_ship_data = DetailData(int(row[0]), row[1], row[2], row[3], row[4], int(row[5]), int(row[6]),
                                          int(row[7]), int(row[8]), float(row[9]), float(row[10]), float(row[11]),
                                          float(row[12]), int(row[13]))
            row = source_csv_reader.next()
            after_ship_data = DetailData(int(row[0]), row[1], row[2], row[3], row[4], int(row[5]), int(row[6]),
                                         int(row[7]), int(row[8]), float(row[9]), float(row[10]), float(row[11]),
                                         float(row[12]), int(row[13]))
            change_dead_weight = calculate_change_dead_weight(before_ship_data, after_ship_data)
            output_csv_writer.writerow(before_ship_data.export_to_csv() + [row[14], row[15], row[16], change_dead_weight])
            output_csv_writer.writerow(after_ship_data.export_to_csv() + [row[14], row[15], row[16], change_dead_weight])

    return


if __name__ == '__main__':
    calculate_weight()
