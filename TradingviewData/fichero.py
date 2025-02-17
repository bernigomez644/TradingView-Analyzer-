import boto3

# Sustituye estos valores por los que generaste en AWS IAM
aws_access_key_id = "ASIAVFIWI4RJ6VCDXVYC"
aws_secret_access_key = "nrFpQLKA8CPcf7tUc5rXsG4dztDHIfuZGEq6pbPw"
aws_region = "eu-south-2"  # Cambia si usas otra región

glue_client = boto3.client(
    "glue",
    region_name=aws_region,
)

# Prueba la conexión
try:
    response = glue_client.get_jobs()
    print("Conexión exitosa:", response["Jobs"])
except Exception as e:
    print("Error de conexión:", e)
