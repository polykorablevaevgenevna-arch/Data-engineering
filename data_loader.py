import urllib3
from minio import Minio
from tqdm import tqdm

# Отключение проверки SSL (не рекомендуется для продуктивной среды)
http_client = urllib3.PoolManager(
    cert_reqs='CERT_NONE',  # Игнорирование SSL-сертификатов
    timeout=urllib3.Timeout(connect=5.0, read=10.0)  # Устанавливаем тайм-ауты для соединения и чтения
)

# Создаем клиента Minio
client = Minio(
    "94.124.179.195:9001",
    access_key="",    # Вставьте ваш access_key
    secret_key="",    # Вставьте ваш secret_key
    secure=True,
    http_client=http_client
)

bucket_name = "poly-bio-data"
object_name = "frequent-mutations.2025-09-28.tsv"
file_path = r"C:\data_eng\RAW.tsv"  # Укажите правильный путь

try:
    # Получаем информацию о файле
    stat = client.stat_object(bucket_name, object_name)
    total_size = stat.size

    # Загружаем файл с прогресс-баром
    with client.get_object(bucket_name, object_name) as response, open(file_path, 'wb') as file, tqdm(
        total=total_size, unit='B', unit_scale=True, desc='Downloading'
    ) as pbar:
        for data in response.stream(1024 * 1024):  # по 1 МБ
            file.write(data)
            pbar.update(len(data))
    print("Object downloaded successfully.")
except Exception as err:
    print(f"Error downloading object: {err}")
