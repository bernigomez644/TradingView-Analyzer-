import boto3
 
# Configuración AWS
aws_region = "eu-south-2"  # Cambia según tu región
database_name = "trade_data_imat3b10"  # Nombre de la base de datos en Glue
role_arn = "arn:aws:iam::354918392915:role/GlueCrawlerRole"  # ARN de IAM Role
bucket_name = "cargadatostradingview"  # Nombre del bucket de S3
crawler_name = "s3-to-glue-crawler"

glue_client = boto3.client("glue", region_name=aws_region)
 

def create_crawler():

    for crypto in cryptos:
        s3_target_path = f"s3://{bucket_name}"  # Carpeta donde están los CSV preprocesados
       
 
        try:
            glue_client.create_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={"S3Targets": [{"Path": s3_target_path}]},
            )
            print(f"Crawler '{crawler_name}' creado exitosamente")
        except Exception as e:
            print(f"Error al crear el crawler '{crawler_name}': {e}")
 
def main():
    
    create_crawler()
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"Crawler '{crawler_name}' iniciado para {crypto}.")
    except Exception as e:
        print(f"Error al iniciar el crawler '{crawler_name}': {e}")
 
# Ejecutar funciones
if __name__ == "__main__":
    main()
 