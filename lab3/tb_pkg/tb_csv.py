import csv
from tb_pkg import tb_oper

def load_table(*files):
    tables = []
    for fl in files:
        with open(fl, newline='') as input_file:
            reader = csv.reader(input_file, delimiter=',')
            for i, row in enumerate(reader):
                if i > 0:
                    if len(row) != len(table['columns']):
                        raise ValueError('Ошибка с разделителями')
                    table['values'].append(row)
                else:
                    table = {'columns': row, 'values': [], 'dtypes': [str] * len(row)}
            tables.append(table)
    return tb_oper.concat(*tables)

def save_table(table, filename, max_rows=None):
    if max_rows is None:
        with open(filename, 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=',')
            writer.writerows([table['columns']] + table['values'])
    else:
        fl = filename.split('.')
        if len(fl) == 1:
            fl.append('')
        chunks = tb_oper.chunk(table, max_rows)
        for i, ch in enumerate(chunks):
            with open(fl[0] + '_' + str(i) + '.' + fl[1], 'w', newline='') as output_file:
                writer = csv.writer(output_file, delimiter=',')
                writer.writerows([ch['columns']] + ch['values'])
