import os, six
from google.cloud import storage
import github3

DAG_FOLDER = 'dags/'
SQL_FOLDER = 'dags/sql/'
DAG_REPO_ORG = 'rendybjunior'
DAG_REPO_NAME = 'freddie-dags'
MASTER_BRANCH_NAME = 'master'

PROJECT = 'xxx'
BUCKET = 'xxx'
GITHUB_TOKEN = 'xxx'

def upload_file(file_path, content):
    client = storage.Client(project=PROJECT)
    bucket = client.get_bucket(BUCKET)
    blob = bucket.blob(file_path)
    blob.upload_from_string(content)
    url = blob.public_url
    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')
    print(url)
    # todo return meaningful status & message

def upload_folder(repository, folder, file_endswith):
    contents = repository.directory_contents(directory_path=folder,ref=MASTER_BRANCH_NAME)
    for filename, content in contents:
        if filename.endswith(file_endswith) and content.type == 'file':
            content = repository.file_contents(content.path, ref=MASTER_BRANCH_NAME)
            upload_file(content.path, content.decoded)

def deploy_dags(repository):
    upload_folder(repository, DAG_FOLDER, '.py')

def deploy_sqls(repository):
    upload_folder(repository, SQL_FOLDER, '.sql')

def handle_pr_freddie_dag(request):
    # request_json = request.get_json()
    # TODO check if it is a pr merged
    g = github3.login(token=GITHUB_TOKEN)
    repository = g.repository(DAG_REPO_ORG, DAG_REPO_NAME)
    deploy_dags(repository)
    deploy_sqls(repository)
    return 'Hello World!' #+ request_json.get('action')

if __name__ == '__main__':
    handle_pr_freddie_dag(request={
        "action": "closed"
    })