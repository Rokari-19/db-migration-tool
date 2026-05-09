def transform(data, column_mapping, drop_nones=True):
    transformed = []
    for row in data:
        new_row = {}
        valid = True
        for source_col, target_col in column_mapping.items():
            value = row.get(source_col)
            if value is None and drop_nones:
                valid = False
                break
            new_row[target_col] = value
        if valid:
            transformed.append(new_row)
    return transformed
