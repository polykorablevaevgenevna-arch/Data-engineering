#получение учётных данных
import pandas as pd
import sqlite3
import psycopg2
from psycopg2.extras import execute_values

db_path = r"C:\data_eng\creds.db"
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

#добавление данных в таблицу
df = pd.read_csv(r'C:\data_eng\frequent-mutations.2025-09-28.tsv', sep = '\t')

cur = conn_pg.cursor()

#Берем только первые 100 строк
df_subset = df.head(100)

#Преобразуем DataFrame в список кортежей
data_tuples = [tuple(x) for x in df_subset[[
    'ssm_id', 'dna_change', 'protein_change', 'type', 'consequence',
    'num_ssm_affected_cases', 'num_TP53_cases', 'ssm_affected_cases_percentage',
    'num_gdc_ssm_affected_cases', 'num_gdc_ssm_cases', 'gdc_ssm_affected_cases_percentage',
    'vep_impact', 'sift_impact', 'sift_score', 'polyphen_impact', 'polyphen_score'
]].values]

#SQL-запрос для вставки
sql = """
INSERT INTO korableva (
    ssm_id, dna_change, protein_change, type, consequence,
    num_ssm_affected_cases, num_TP53_cases, ssm_affected_cases_percentage,
    num_gdc_ssm_affected_cases, num_gdc_ssm_cases, gdc_ssm_affected_cases_percentage,
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
