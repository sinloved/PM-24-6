import pickle
from tb_pkg import tb_oper

def load_table(*files):
    tables = []
    for fl in files:
        with open(fl, 'rb') as input_file:
            tables.append(pickle.load(input_file))
    return tb_oper.concat(*tables)

def save_table(table, filename, max_rows=None):
    if max_rows is None:
        with open(filename, 'wb') as output_file:
            pickle.dump(table, output_file)
    else:
        fl = filename.split('.')
        if len(fl) == 1:
            fl.append('')
        chunks = tb_oper.chunk(table, max_rows)
        for i, ch in enumerate(chunks):
            with open(fl[0] + '_' + str(i) + '.' + fl[1], 'wb') as output_file:
                pickle.dump(ch, output_file)
