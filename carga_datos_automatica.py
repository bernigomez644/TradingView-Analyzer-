import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from TradingviewData import TradingViewData, Interval

# Nombre del bucket de S3
BUCKET_NAME = "mi-bucket-de-cripto"

# Diccionario de criptomonedas con sus respectivos símbolos
crypto_symbols = {
    "Bitcoin": "BTCUSD",
    "Ethereum": "ETHUSD",
    "Ripple": "XRPUSD",
    "Solana": "SOLUSD",
    "Dogecoin": "DOGEUSD",
    "Cardano": "ADAUSD",
    "Shiba Inu": "SHIBUSD",
    "Polkadot": "DOTUSD",
    "Aave": "AAVEUSD",
    "Stellar": "XLMUSD",
}

def create_s3_bucket(bucket_name):
    """Crea un bucket en S3 si no existe."""
    s3 = boto3.client("s3")
    
    try:
        # Verifica si el bucket ya existe
        s3.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' creado correctamente.")
        else:
            print(f"Error al verificar el bucket: {e}")

def upload_to_s3(local_file_path, bucket_name, s3_file_path):
    """Sube un archivo a AWS S3."""
    s3 = boto3.client("s3")
    
    try:
        s3.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"Archivo {local_file_path} subido a S3 en {s3_file_path}")
    except NoCredentialsError:
        print("Error: No se encontraron las credenciales de AWS.")
    except Exception as e:
        print(f"Error al subir archivo: {e}")

def fetch_and_save_crypto_data():
    """Descarga los datos de TradingView y los guarda en CSVs, luego los sube a S3."""
    os.makedirs("data", exist_ok=True)
    
    trading_view = TradingViewData()

    for crypto_name, symbol in crypto_symbols.items():
        print(f"Descargando datos históricos de {crypto_name}...")

        historical_data = trading_view.get_hist(
            symbol, exchange="COINBASE", interval=Interval.daily, n_bars=4 * 365
        )

        total_data_points = len(historical_data)
        days_per_year = 365

        # Dividir los datos en conjuntos de un año
        yearly_data_splits = [
            (historical_data[i : i + days_per_year], historical_data.index[i].year)
            for i in range(0, total_data_points, days_per_year)
        ]

        for yearly_data, year in yearly_data_splits:
            local_file_path = os.path.join("data", f"{crypto_name}historical_data{year}.csv")
            s3_file_path = f"{crypto_name}/{crypto_name}historical_data{year}.csv"

            # Guardar en CSV local
            yearly_data.to_csv(local_file_path)
            print(f"Datos de {crypto_name} para el año {year} guardados en {local_file_path}.")

            # Subir a S3
            upload_to_s3(local_file_path, BUCKET_NAME, s3_file_path)

if _name_ == "_main_":
    create_s3_bucket(BUCKET_NAME)
    fetch_and_save_crypto_data()