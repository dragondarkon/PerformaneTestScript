import xlsxwriter
import xlrd
import openpyxl

from xlsxwriter import Workbook

wb = openpyxl.load_workbook('example.xlsx')
sheet = wb.get_sheet_by_name('Sheet1')

first_row = 1
last_row = 100

for row_num in range(first_row, last_row):
    worksheet.write(row_num, 0, "Text")

workbook.close()