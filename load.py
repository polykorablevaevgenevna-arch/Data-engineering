def load_df(df, db_path=r"C:\data_eng\creds.db", table_name="korableva"):
    #получение учётных данных
    import pandas as pd
    import sqlite3
    import psycopg2
    from psycopg2.extras import execute_values
    
    conn1 = sqlite3.connect(db_path)
    cursor = conn1.cursor()
    
    cursor.execute("SELECT * FROM access")
    access = cursor.fetchone()
    
    #закрыть соединение
    cursor.close()
    conn1.close()
    
    #подключение к базе данных homeworks
    if access:
        host, dbname, user, password = access
        
        try:
            conn_pg = psycopg2.connect(
                host=host,
                dbname='homeworks',
                user=user,
                password=password
            )
            print('Подключение к PostgreSQL успешно выполнено')
        except Exception as e:
            print('Подключиться к PostgreSQL не удалось')
            print("Ошибка:", e)
    else:
        print("Нет учетных данных для подключения")
    
    cur = conn_pg.cursor()
    
    #Создание таблицы
    cur.execute("""
    CREATE TABLE IF NOT EXISTS korableva (
        id TEXT,
        dna_change TEXT,
        protein_change TEXT,
        type_of_mutation TEXT,
        consequence_for_transcript TEXT,
        num_tp53mut INTEGER,
        num_all_tp53_mut INTEGER,
        perc_mut_to_alltp53mut REAL,
        num_allmut INTEGER,
        perc_thismut_to_allmut REAL,
        vep_impact TEXT,
        sift_impact TEXT,
        sift_score REAL,
        polyphen_impact TEXT,
        polyphen_score REAL
    );
    """)
    
    #Берем только первые 100 строк
    df_subset = df.head(100)
    df_subset = df_subset.applymap(lambda x: None if pd.isna(x) else x)
    df_subset.columns = [c.lower() for c in df_subset.columns]
    
    #Преобразуем DataFrame в список кортежей
    data_tuples = [tuple(x) for x in df_subset[[
        'id', 'dna_change', 'protein_change', 'type_of_mutation', 'consequence_for_transcript',
        'num_tp53mut', 'num_all_tp53_mut', 'perc_mut_to_alltp53mut',
        'num_allmut', 'perc_thismut_to_allmut',
        'vep_impact', 'sift_impact', 'sift_score', 'polyphen_impact', 'polyphen_score'
    ]].values]
    
    #SQL-запрос для вставки данных в таблицу
    sql = """
    INSERT INTO korableva (
        id, dna_change, protein_change, type_of_mutation, consequence_for_transcript,
        num_tp53mut, num_all_tp53_mut, perc_mut_to_alltp53mut,
        num_allmut, perc_thismut_to_allmut,
        vep_impact, sift_impact, sift_score, polyphen_impact, polyphen_score
    ) VALUES %s
    """
    
    #Вставка первых 100 строк
    execute_values(cur, sql, data_tuples)
    conn_pg.commit()
    print(f"Вставлено первые {len(data_tuples)} строк")
    
    #вывод таблицы
    cur.execute("SELECT * FROM korableva;")
    rows = cur.fetchall()
    
    df_all = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
    print(df_all)
    
    cur.close()
    conn_pg.close()
