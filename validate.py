def col_rename(df):
    #изменение названий столбцов
    df = df.rename(columns={'ssm_id': 'ID', 'type': 'type_of_mutation', 'consequence': 'consequence_for_transcript', 'num_ssm_affected_cases':'num_mut_acrossTP53', 'num_TP53_cases': 'num_all_TP53_mut', 'ssm_affected_cases_percentage': 'perc_mut_to_allTP53mut', 'num_gdc_ssm_affected_cases': 'num_this_mut_acrossall', 'num_gdc_ssm_cases': 'num_allmut', 'gdc_ssm_affected_cases_percentage': 'perc_thismut_to_allmut'})
    df.head(4)
    return(df)

def coll_del(df):
    #удаляем столбец num_this_mut_acrossall
    df.drop('num_this_mut_acrossall', axis=1, inplace=True)
    df = df.rename(columns={'num_mut_acrossTP53': 'num_TP53mut'})
    df.head(4)
    return(df)