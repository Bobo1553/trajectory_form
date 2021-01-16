# coding=utf-8


def txt_write_demo():
    # region parameters
    output_txt_name = r'write_test.txt'
    # endregion

    output_file = open(output_txt_name, 'w')
    output_file.write('Test\n')
    output_file.write('End')
    pass


if __name__ == '__main__':
    txt_write_demo()
