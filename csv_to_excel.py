# coding=utf-8

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import xlwt

def print_help():
    print "USAGE: python csv_to_excel.py CSV_FILE EXCEL_FILE"
    return

def main():

    if len(sys.argv) != 3:
        print_help()
        return

    csv_file = open(sys.argv[1], "r")
    excel_file = xlwt.Workbook(encoding='gbk', style_compression=0)
    sheet = excel_file.add_sheet('sheet 1', cell_overwrite_ok=True)

    row = 0
    for line in csv_file:
        line = line.strip().decode("utf-8", "ignore")
        ary = line.split(",")
        col = 0

        for item in ary:
            sheet.write(row, col, item.encode("gbk", "ignore"))
            col = col + 1
        row = row + 1
        
    excel_file.save(sys.argv[2])

    return

main()

