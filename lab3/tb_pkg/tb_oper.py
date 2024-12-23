import operator
from copy import deepcopy, copy


def get_col_index(table, column):
    if type(column) != int and type(column) != str:
        raise ValueError('Значения column должны быть целочисленными или строковыми')
    return table['columns'].index(column) if type(column) == str else column

def get_rows_by_number(table, start, stop=None, copy_table=False):
    if stop is None:
        stop = len(table['values'])
    if type(start) != int or type(stop) != int:
        raise ValueError('Значения start/stop должны быть целочисленными')
    return {'columns': table['columns'], 'dtypes': table['dtypes'],
            'values': deepcopy(table['values'][start:stop]) if copy_table else table['values'][start:stop]}

def get_rows_by_index(table, *args, **kwargs):
    ixs = set(args)
    if len(set([type(x) for x in ixs])) > 1:
        raise ValueError('Значения индекса должны быть одного типа данных')
    return {'columns': table['columns'], 'dtypes': table['dtypes'],
            'values': [copy(x) if kwargs.get('copy_table', False) else x for x in table['values'] if x[0] in ixs]}

def set_column_types(table, types_dict, by_number=True):
    if by_number and any([type(x) != int for x in types_dict.keys()]):
        raise ValueError('Для by_number=True индексы столбцов должны быть целочисленными')
    if any([x not in (int, float, bool, str) for x in types_dict.values()]):
        raise ValueError('Разрешенные типы данных: int, float, bool, str')
    for i, col in enumerate(table['columns']):
        table['dtypes'][i] = types_dict.get(i if by_number else col, str)

def get_column_types(table, by_number=True):
    if 'dtypes' not in table:
        raise ValueError('Поврежденная таблица: не указаны типы данных')
    out = {}
    for i, col in enumerate(zip(table['columns'], table['dtypes'])):
        out[i if by_number else col[0]] = col[1]
    return out

def get_values(table, column=0):
    column = get_col_index(table, column)
    return [get_column_types(table)[column](x[column]) for x in table['values']]

def get_value(table, column=0):
    if len(table['values']) > 1:
        raise ValueError('Функция get_value применима только для таблиц с одной строкой')
    column = get_col_index(table, column)
    return get_column_types(table)[column](table['values'][0][column])

def set_values(table, values, column=0):
    column = get_col_index(table, column)
    if len(values) != len(table['values']):
        raise ValueError('Несовпадение размерности')
    for i, row in enumerate(table['values']):
        if type(values[i]) != get_column_types(table)[column]:
            raise ValueError('Значения должны быть типа данных ' + str(get_column_types(table)[column]))
        row[column] = values[i]

def set_value(table, value, column=0):
    column = get_col_index(table, column)
    if len(table['values']) > 1:
        raise ValueError('Функция set_value применима только для таблиц с одной строкой')
    if type(value) != get_column_types(table)[column]:
        raise ValueError('Значение должно быть типа данных ' + str(get_column_types(table)[column]))
    table['values'][0][column] = value

def mat_op(oper, table0, column0, table1, column1):
    if len(table0['values']) != len(table1['values']):
        raise ValueError('Операнды должны иметь одну длину')
    if get_column_types(table0)[get_col_index(table0, column0)] not in (int, float, bool) or \
            get_column_types(table1)[get_col_index(table1, column1)] not in (int, float, bool):
        raise ValueError('Разрешенные типы данных: int, float, bool')
    return [oper(x[0], x[1]) for x in zip(get_values(table0, column0), get_values(table1, column1))]

def add(table0, column0, table1, column1):
    return mat_op(operator.add, table0, column0, table1, column1)

def sub(table0, column0, table1, column1):
    return mat_op(operator.sub, table0, column0, table1, column1)

def mul(table0, column0, table1, column1):
    return mat_op(operator.mul, table0, column0, table1, column1)

def div(table0, column0, table1, column1):
    if any([x == 0 for x in get_values(table1, column1)]):
        raise ValueError('Делить на ноль нельзя')
    return mat_op(operator.truediv, table0, column0, table1, column1)

def eq(table0, column0, table1, column1):
    return mat_op(operator.eq, table0, column0, table1, column1)

def ne(table0, column0, table1, column1):
    return mat_op(operator.ne, table0, column0, table1, column1)

def gr(table0, column0, table1, column1):
    return mat_op(operator.gt, table0, column0, table1, column1)

def ls(table0, column0, table1, column1):
    return mat_op(operator.lt, table0, column0, table1, column1)

def ge(table0, column0, table1, column1):
    return mat_op(operator.ge, table0, column0, table1, column1)

def le(table0, column0, table1, column1):
    return mat_op(operator.le, table0, column0, table1, column1)

def filter_rows(table, bool_list, copy_table=False):
    if len(bool_list) != len(table['values']):
        raise ValueError('Несовпадение размерности')
    return {'columns': table['columns'], 'dtypes': table['dtypes'],
            'values': [copy(x) if copy_table else x for i, x in enumerate(table['values']) if bool_list[i]]}

def concat(*tables):
    out = []
    for i, t in enumerate(tables):
        if i > 0:
            if t['columns'] != tables[i - 1]['columns']:
                raise ValueError('Колонки должны совпадать')
            if t['dtypes'] != tables[i - 1]['dtypes']:
                raise ValueError('Типы данных должны совпадать')
        out.extend(deepcopy(t['values']))
    return {'columns': tables[0]['columns'], 'dtypes': tables[0]['dtypes'], 'values': out}

def split(table, row_number):
    return (get_rows_by_number(table, 0, stop=row_number, copy_table=True),
            get_rows_by_number(table, row_number, copy_table = True))

def chunk(table, max_rows):
    i = 0
    out = []
    for j in range(max_rows, len(table['values']) + max_rows, max_rows):
        out.append(get_rows_by_number(table, i, stop=j, copy_table=True))
        i += max_rows
    return out

def print_table(table):
    print('\t'.join(table['columns']))
    for value in table['values']:
        print('\t'.join(map(str, value)))
