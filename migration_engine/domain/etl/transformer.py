def transform(data, column_mapping, drop_nones=True):
    transformed = []
    
    for row in data:
        new_row = {}
        is_valid = True
        
        for source_col, target_col in column_mapping.items():
            # print(source_col, target_col)
            value = row.get(source_col)
            # print(value)
            
            # Validation logic: if a mapped field is missing and we can't have NULLs
            if value is None and drop_nones:
                is_valid = False
                # print(f"Validity: {is_valid}")
                break 
            
            new_row[target_col] = value
        
        if is_valid:
            transformed.append(new_row)

    print(f"Transformation complete: {len(transformed)} rows ready.")
    return transformed
