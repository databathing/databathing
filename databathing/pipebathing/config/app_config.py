from auto_generated_pipieline_framework.utils import storage

project_name = storage.get_project_name()

dev = {
    "raw": "gs://",
    "stg": "gs://",
    "ctlg": "gs://"
}

prod = {
    "raw": "gs://",
    "stg": "gs://",
    "ctlg": "gs://"
}

bucket_mapping = dev if project_name.split('-')[-1] == 'dev' else prod
