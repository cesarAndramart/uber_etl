from google.cloud import storage
import requests
for i in range(8,12):
    if i+1 < 10:
        month = '0' + str(i+1)
    else:
        month = str(i+1)
    file_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month}.parquet'
    try:
        #request_json = request.get_json(silent=True)
        #file_url = request_json.get('fileUrl')
        
        #if not file_url:
        #    return "Se necesita un parámetro fileUrl.", 400

        bucket_name = 'your_bucket_name'
        file_name = file_url.split('/')[-1]

        # Download the file
        response = requests.get(file_url, stream=True)
        if response.status_code != 200:
            return f"Error al descargar el archivo: {response.status_code}", 500
        folder = 'raw_data'
        # Upload file to bucket
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f'{folder}/{file_name}')

        blob.upload_from_string(response.content, content_type=response.headers['Content-Type'])
        f"Archivo subido exitosamente: {bucket_name}/{folder}/{file_name}", True

    except Exception as e:
        print(f"Error: {e}")

