### Cloud Function Reviews-To-Parquet

import functions_framework
import os
import json
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage


@functions_framework.http
def reviews_to_parquet(request):
    try:
        bucket_name = "json-bucket-datos"
        source_folder_name = "Google/reviews-estados"
        destination_folder_name = "Google/reviews-estados-parquet"

        # Listado de directorios a procesar
        directories_to_process = ['review-Alabama',
                                'review-Arizona',
                                'review-California',
                                'review-Colorado',
                                'review-District_of_Columbia',
                                'review-Florida',
                                'review-Georgia',
                                'review-Illinois',
                                'review-Indiana',
                                'review-Maryland',
                                'review-Michigan',
                                'review-Minnesota',
                                'review-Mississippi',
                                'review-Montana',
                                'review-Nebraska',
                                'review-New_Mexico',
                                'review-New_York',
                                'review-North_Carolina',
                                'review-Ohio',
                                'review-Oregon',
                                'review-Pennsylvania',
                                'review-South_Carolina',
                                'review-Texas',
                                'review-Virginia',
                                'review-Washington'
                            ]

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        for directory in directories_to_process:
            source_directory = os.path.join(source_folder_name, directory)

            # Listar los JSONs en el directorio
            blobs = bucket.list_blobs(prefix=source_directory)

            # Crear la lista para la data de los JSON
            json_data = []

            for blob in blobs:
                if blob.name.endswith(".json"):
                    # Bajar Archivo JSON
                    temp_dir = tempfile.mkdtemp()
                    local_file_path = os.path.join(temp_dir, os.path.basename(blob.name))
                    blob.download_to_filename(local_file_path)

                    # Cargar data JSON
                    with open(local_file_path, 'r') as json_file:
                        for linea in json_file:
                            try:
                                objeto = json.loads(linea)
                                json_data.append(objeto)
                            except json.JSONDecodeError as e:
                                print(f"error al decodificar JSON en línea: {linea}, Error: {e}")

                    # Limpiar directorio temporal
                    os.remove(local_file_path)
                    os.rmdir(temp_dir)

            if json_data:
                # Generar el dataframe de pandas en base al json
                df = pd.DataFrame(json_data)

                # convertir el dataframe a pyarrow
                table = pa.Table.from_pandas(df)

                # Construir el path
                destination_blob_name = os.path.join(destination_folder_name, f"{directory}.parquet")

                # Tabla a Parquet
                with tempfile.NamedTemporaryFile() as temp_parquet_file:
                    pq.write_table(table, temp_parquet_file.name)
                    temp_parquet_file.seek(0)

                    # Guardar archivos parquet
                    destination_blob = bucket.blob(destination_blob_name)
                    destination_blob.upload_from_filename(temp_parquet_file.name)

        return "JSON files consolidated to Parquet"
    except Exception as e:
        return f"Error: {str(e)}"
