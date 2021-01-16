# coding=utf8

import arcgisscripting


def create_feature_from_file():
    # region parameters
    input_txt_name = r'D:\ShipProgram\DoctorPaper\MSRData\FileData\ShipTrajectoryLine.txt'
    output_shp_name = r'D:\ShipProgram\DoctorPaper\MSRData\ShpData\ShipTrajectoryLine.shp'
    # endregion

    gp = arcgisscripting.create()
    gp.CreateFeaturesFromTextFile(input_txt_name, '.', output_shp_name, "#")


if __name__ == '__main__':
    create_feature_from_file()
