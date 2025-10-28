def transform_data(df):
    
    #Привидение типов
    import pandas as pd
    print("Первые три строки:")
    print(df.head(3)) #вывод первых трёх строк
    print()
    print("Типы данных до преобразования:")
    print(df.dtypes) #вывод типов данных до преобразования
    print()
    
    #преобразование типов данных
    df['ssm_id'] = df['ssm_id'].astype("string")
    df['dna_change'] = df['dna_change'].astype("string")
    df['protein_change'] = df['protein_change'].astype("string")
    df['type'] = df['type'].astype("string")
    df['consequence'] = df['consequence'].astype("string")
    df['num_ssm_affected_cases'] = df['num_ssm_affected_cases'].astype("int")
    df['num_TP53_cases'] = df['num_TP53_cases'].astype("int")
    df['ssm_affected_cases_percentage'] = df['ssm_affected_cases_percentage'].astype("float")
    df['num_gdc_ssm_affected_cases'] = df['num_gdc_ssm_affected_cases'].astype("int")
    df['num_gdc_ssm_cases'] = df['num_gdc_ssm_cases'].astype("int")
    df['ssm_affected_cases_percentage'] = df['ssm_affected_cases_percentage'].astype("float")
    df['vep_impact'] = df['vep_impact'].astype("string")
    df['sift_impact'] = df['sift_impact'].astype("string")
    df['sift_score'] = df['sift_score'].astype("float")
    df['polyphen_impact'] = df['polyphen_impact'].astype("string")
    df['polyphen_score'] = df['polyphen_score'].astype("float")
    
    print("Типы данных после преобразования:")
    print(df.dtypes) #вывод типов данных после преобразования
    df.to_parquet('data.parquet', engine='pyarrow') #сохранение в соотвествующий формат

    return df
