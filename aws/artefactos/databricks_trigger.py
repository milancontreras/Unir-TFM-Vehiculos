import requests

def trigger_databricks_job(host, token, job_id):
    """
    Ejecuta un Job de Databricks usando la API 2.1.
    Toma host, token y job_id desde variables de entorno.
    Devuelve el response JSON o lanza excepción.
    """
    
    job_id = int(job_id)

    # Construir endpoint
    url = f"{host}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "job_id": job_id
    }

    # Llamar API
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    return response.json()