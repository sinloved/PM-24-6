def save_table(table, filename):
    with open(filename, 'w') as output_file:
        output_file.write('\t'.join(table['columns']) + '\n')
        for value in table['values']:
            output_file.write('\t'.join(map(str, value)) + '\n')
