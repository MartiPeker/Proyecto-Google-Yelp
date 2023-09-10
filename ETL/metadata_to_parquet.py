### Cloud Function Metadata-To-Parquet

import functions_framework
import os
import tempfile
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


@functions_framework.http
def process_and_save_to_parquet(request):
    try:
        bucket_name = "json-bucket-datos"
        source_folder_name = "Google/metadata-sitios"
        destination_folder_name = "Google/metadata-sitios-parquet"

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        # Listar los objetos
        blobs = bucket.list_blobs(prefix=source_folder_name)

        for blob in blobs:
            if blob.name.endswith(".json"):
                # Bajar archivo JSON
                temp_dir = tempfile.mkdtemp()
                local_file_path = os.path.join(temp_dir, os.path.basename(blob.name))
                blob.download_to_filename(local_file_path)

                # Cargar data JSON
                objetos_json = []
                with open(local_file_path, 'r') as json_file:
                    for linea in json_file:
                        try:
                            objeto = json.loads(linea)
                            objetos_json.append(objeto)
                        except json.JSONDecodeError as e:
                            print(f"error al decodificar JSON en línea: {linea}, Error: {e}")

                # Crear Pandas DataFrame
                df = pd.DataFrame(objetos_json)

                # Crear tabla Parquet
                table = pa.Table.from_pandas(df)

                # Construir el path a destino archivo parquet
                destination_blob_name = os.path.join(destination_folder_name, os.path.basename(blob.name).replace('.json', '.parquet'))

                # Subir archivo parquet a destino
                destination_blob = bucket.blob(destination_blob_name)
                with tempfile.NamedTemporaryFile() as temp_parquet_file:
                    pq.write_table(table, temp_parquet_file.name)
                    temp_parquet_file.seek(0)
                    destination_blob.upload_from_filename(temp_parquet_file.name)

                # Limpiar Directorio temporal
                os.remove(local_file_path)
                os.rmdir(temp_dir)

        return f"Archivos JSON en {source_folder_name} convertidos a Parquet y guardados en {destination_folder_name}"
    except Exception as e:
        return f"Error: {str(e)}"