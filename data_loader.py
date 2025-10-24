#Выгрузка файла с хранилища MiniO
import urllib3
from minio import Minio
from tqdm import tqdm

# Отключение проверки SSL
http_client = urllib3.PoolManager(
    cert_reqs='CERT_NONE',
    timeout=urllib3.Timeout(connect=5.0, read=10.0)  #тайм-ауты для соединения и чтения
)

# Создаем клиента Minio. Ключи не нужны - файл в откртом доступе
client = Minio(
    "94.124.179.195:9001",
    access_key="",
    secret_key="",
    secure=True, #потому что hhtps:\
    http_client=http_client
)

bucket_name = "poly-bio-data"
object_name = "frequent-mutations.2025-09-28.tsv"
file_path = r"C:\data_eng\RAW.tsv"

#скаиванием файл с указанием времени скачивания
try:
    stat = client.stat_object(bucket_name, object_name)
    total_size = stat.size

    with client.get_object(bucket_name, object_name) as response, open(file_path, 'wb') as file, tqdm(
        total=total_size, unit='B', unit_scale=True, desc='Downloading'
    ) as pbar:
        for data in response.stream(1024 * 1024):
            file.write(data)
            pbar.update(len(data))
    print("Object downloaded successfully.")
except Exception as err:
    print(f"Error downloading object: {err}")



#Привидение типов
import pandas as pd
df = pd.read_csv('C:/data_eng/frequent-mutations.2025-09-28.tsv', sep='\t') #чтение файла
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
