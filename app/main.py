import datetime
import os, sys, six, base64, copy

from jinja2 import Environment, FileSystemLoader, Template

from google.auth.transport import requests
from google.cloud import datastore
from google.cloud import storage
from google.cloud import bigquery
import google.oauth2.id_token

from flask import Flask, render_template, request
from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SubmitField, IntegerField
from wtforms.fields.html5 import DateField
from wtforms.validators import DataRequired, Email

import github3

DAG_FOLDER = 'dags/'
SQL_FOLDER = 'dags/sql/'
DAG_REPO_ORG = 'rendybjunior'
DAG_REPO_NAME = 'freddie-dags'
MASTER_BRANCH_NAME = 'master'

PROJECT = 'xxx'
BUCKET = 'xxx'
g = github3.login(token='xxx')

DOLLAR_TO_IDR = 14000
BQ_DOLLAR_PER_TB = 5

datastore_client = datastore.Client()

app = Flask(__name__)
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

class DagForm(FlaskForm):
    dag_name = StringField('DAG Name', validators=[DataRequired()], render_kw={"placeholder": "lower_case_underscored"})
    owner = StringField('Owner', validators=[DataRequired()], render_kw={"placeholder": "lower_case_underscored"})
    start_date = DateField('Start Date', validators=[DataRequired()], format='%Y-%m-%d')
    email = StringField('Email', validators=[DataRequired(), Email()], render_kw={"placeholder": "separate@bycomma.com,separate@bycomma2.com,"})
    retries = IntegerField('Num of Retries', validators=[DataRequired()], default=1)
    retry_delay_minutes = IntegerField('Retry Delay (in minutes)', validators=[DataRequired()], default=15)
    schedule_interval = StringField('Schedule (in cron) UTC', validators=[DataRequired()], render_kw={"placeholder": "0 17 * * *"})
    tasks = StringField('Tasks', validators=[DataRequired()], render_kw={"placeholder": "separated_by_comma, lower_case_underscored"})
    dependencies = StringField('Dependencies', validators=[DataRequired()], render_kw={"placeholder": "eg. prev_task_id1,task_id1;prev_task_id1,task_id2)"})
    submit = SubmitField('Save')

class TaskForm(FlaskForm):
    task_id = StringField('Task ID', validators=[DataRequired()], render_kw={"placeholder": "lower_case_underscored"})
    destination_table = StringField('Destination table', validators=[DataRequired()], render_kw={"placeholder": "my-project.test.freddie_mercury"})
    sql = TextAreaField('SQL', validators=[DataRequired()])
    sql_params = StringField('SQL Param to test SQL. THIS VALUE FOR TESTING ONLY', render_kw={"placeholder": "example: ds=2019-01-01,dsnodash=20190101"})
    save = SubmitField('Save')
    check_query = SubmitField('Check Query')
    run_query = SubmitField('Run Query')

def store_task(task_id, destination_table, sql, sql_params, updated_by, type_):
    entity = datastore.Entity(key=datastore_client.key('Task', task_id), exclude_from_indexes=['sql_base64'])
    entity.update({
        'type': type_,
        'destination_table': destination_table,
        'sql_base64' : base64.b64encode(sql.encode()),
        'sql_params' : sql_params,
        'updated_at' : datetime.datetime.now(),
        'updated_by' : updated_by
    })
    datastore_client.put(entity)
    return True, "{} saved".format(task_id) # todo check put return value

def fetch_task(task_id):
    key = datastore_client.key('Task', task_id)
    task = datastore_client.get(key=key)
    task_obj = {
        'type': task.get('type'),
        'task_id': task.key.name,
        'sql': base64.b64decode(task.get('sql_base64')).decode(),
        'sql_params': task.get('sql_params'),
        'destination_table': task.get('destination_table')
    }
    return task_obj

def fetch_tasks(limit=10):
    query = datastore_client.query(kind='Task')
    query.order = ['-updated_at']

    tasks = query.fetch(limit=limit)
    tasks_obj = []
    for task in tasks:
        tasks_obj.append({
            'type': task.get('type'),
            'task_id': task.key.name,
            'sql': base64.b64decode(task.get('sql_base64')).decode(),
            'sql_params': task.get('sql_params'),
            'destination_table': task.get('destination_table')
        })
    return tasks_obj

def store_dag(dag_name, owner, start_date, retries, retry_delay_minutes, email, schedule_interval, tasks, dependencies, updated_by):
    entity = datastore.Entity(key=datastore_client.key('Dag', dag_name))
    entity.update({
        'dag_name': dag_name,
        'owner': owner,
        'start_date' : start_date,
        'retries': retries,
        'retry_delay_minutes': retry_delay_minutes,
        'email': email,
        'schedule_interval': schedule_interval,
        'tasks': tasks,
        'dependencies': dependencies,
        'updated_at' : datetime.datetime.now(),
        'updated_by' : updated_by
    })
    datastore_client.put(entity)
    return True, "{} saved".format(dag_name) # todo check put return value

def fetch_dags(limit=10):
    query = datastore_client.query(kind='Dag')
    query.order = ['-updated_at']

    dags = query.fetch(limit=limit)
    dags_obj = []
    for dag in dags:
        dags_obj.append({
            'dag_name': dag.key.name,
            'owner': dag.get('owner'),
            'start_date' : dag.get('start_date'),
            'retries': dag.get('retries'),
            'retry_delay_minutes': dag.get('retry_delay_minutes'),
            'email': dag.get('email'),
            'schedule_interval': dag.get('schedule_interval'),
            'tasks': dag.get('tasks'),
            'dependencies': dag.get('dependencies'),
            'updated_by' : dag.get('updated_by')
        })
    return dags_obj

def fetch_dag(dag_name):
    key = datastore_client.key('Dag', dag_name)
    dag = datastore_client.get(key=key)
    dag_obj = {
        'dag_name': dag.key.name,
        'owner': dag.get('owner'),
        'start_date' : dag.get('start_date'),
        'retries': dag.get('retries'),
        'retry_delay_minutes': dag.get('retry_delay_minutes'),
        'email': dag.get('email'),
        'schedule_interval': dag.get('schedule_interval'),
        'tasks': dag.get('tasks'),
        'dependencies': dag.get('dependencies'),
        'updated_by' : dag.get('updated_by')
    }
    return dag_obj

def upload_sql(task_id, sql):
    file_path = os.path.join(SQL_FOLDER, task_id + ".sql")
    client = storage.Client(project=PROJECT)
    bucket = client.get_bucket(BUCKET)
    blob = bucket.blob(file_path)
    blob.upload_from_string(sql)
    url = blob.public_url
    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')
    print(url)
    # todo return meaningful status & message

def upload_dag(dag_name, dag_text):
    file_path = os.path.join(DAG_FOLDER, dag_name + ".py")
    client = storage.Client(project=PROJECT)
    bucket = client.get_bucket(BUCKET)
    blob = bucket.blob(file_path)
    blob.upload_from_string(dag_text)
    url = blob.public_url
    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')
    print(url)
    # todo return meaningful status & message

def check_query(sql):
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job_config.use_query_cache = False
    job_config.use_legacy_sql = False
    client = bigquery.Client(project=PROJECT)
    try:
        query_job = client.query(sql, job_config)
        query_size_megabyte = query_job.total_bytes_processed / 1024 / 1024
        query_size_terabyte = query_size_megabyte / 1024 / 1024
        dollar_est = BQ_DOLLAR_PER_TB * query_size_terabyte
        rp_est = dollar_est * DOLLAR_TO_IDR
        message = "Total MB that will be processed: {0:.2f}".format(query_size_megabyte)
        message += ". Cost estimate: ${0:.2f}".format(dollar_est)
        message += " or Rp{0:.2f})".format(rp_est)
        return True, message
    except:
        return False, sys.exc_info()[1]

def run_query(sql, limit=25):
    sql_with_limit = sql + "\n LIMIT {}".format(limit)
    job_config = bigquery.QueryJobConfig()
    job_config.flatten_results = True
    job_config.use_query_cache = False
    job_config.use_legacy_sql = False
    client = bigquery.Client(project=PROJECT)
    try:
        query_job = client.query(sql_with_limit, job_config=job_config)  # API request
        rows = query_job.result()
        return rows, "OK"
    except:
        return [], sys.exc_info()[1]

def create_branch(repository, dag_name):
    branch_name = '-'.join([dag_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S')])
    master_branch = repository.branch(MASTER_BRANCH_NAME)
    master_head_sha = master_branch.commit.sha
    repository.create_branch_ref(branch_name, master_head_sha)
    return branch_name

def create_github_pr(dag_name, dag_file_content, sql_file_contents, committer_name, committer_email):
    repository = g.repository(DAG_REPO_ORG, DAG_REPO_NAME)
    branch_name = create_branch(repository, dag_name)

    dag_file_path = DAG_FOLDER + dag_name + '.py'
    content = None
    try:
        content = repository.file_contents(path=dag_file_path, ref=branch_name)
    except Exception:
        pass

    if content is None:
        repository.create_file(path=dag_file_path,
            message="Create DAG File {}".format(dag_name),
            content=dag_file_content,
            branch=branch_name,
            committer={
                "name": committer_name,
                "email": committer_email
            })
    else:
        content.update(
            message="Update DAG File {}".format(dag_name),
            content=dag_file_content,
            branch=branch_name,
            committer={
                "name": committer_name,
                "email": committer_email
            })

    for task_id, sql in sql_file_contents:
        sql_file_path = SQL_FOLDER + task_id + '.sql'
        content = None
        try:
            content = repository.file_contents(path=sql_file_path, ref=branch_name)
        except Exception:
            pass

        if content is None:
            repository.create_file(path=sql_file_path,
                message="Create SQL for task {}".format(task_id),
                content=sql,
                branch=branch_name,
                committer={
                    "name": committer_name,
                    "email": committer_email
                })
        else:
            content.update(
                message="Update SQL File for task {}".format(task_id),
                content=sql,
                branch=branch_name,
                committer={
                    "name": committer_name,
                    "email": committer_email
                })


    pull_body="*test* _123_" #TODO
    repository.create_pull(title=branch_name, base=MASTER_BRANCH_NAME, head=branch_name, body=pull_body)

firebase_request_adapter = requests.Request()
@app.route('/')
def root():
    # Verify Firebase auth.
    id_token = request.cookies.get("token")
    error_message = None
    claims = None
    dags = None
    tasks = None

    if id_token:
        try:
            # Verify the token against the Firebase Auth API. This example
            # verifies the token on each page load. For improved performance,
            # some applications may wish to cache results in an encrypted
            # session store (see for instance
            # http://flask.pocoo.org/docs/1.0/quickstart/#sessions).
            claims = google.oauth2.id_token.verify_firebase_token(
                id_token, firebase_request_adapter)

            tasks = fetch_tasks()
            dags = fetch_dags()

        except ValueError as exc:
            # This will be raised if the token is expired or any other
            # verification checks fail.
            error_message = str(exc)

    return render_template(
        'index.html',
        user_data=claims, error_message=error_message, dags=dags, tasks=tasks)

@app.route('/dag_form', methods=["GET", "POST"])
def dag_form():
    # Verify Firebase auth.
    id_token = request.cookies.get("token")
    error_message = None
    claims = None

    if id_token:
        claims = google.oauth2.id_token.verify_firebase_token(
            id_token, firebase_request_adapter)
        form = DagForm()
        dag_text = ""
        if form.validate_on_submit():
            root = os.path.dirname(os.path.abspath(__file__))
            templates_dir = os.path.join(root, 'templates')
            env = Environment( loader = FileSystemLoader(templates_dir) )
            template = env.get_template('dag_template.py')
            store_dag(dag_name=form.dag_name.data,
                owner=form.owner.data,
                start_date=form.start_date.data.strftime("%Y-%m-%d"),
                email=form.email.data,
                retries=form.retries.data,
                retry_delay_minutes=form.retry_delay_minutes.data,
                schedule_interval=form.schedule_interval.data,
                tasks=form.tasks.data,
                dependencies=form.dependencies.data,
                updated_by=claims['email'])

            tasks = []
            sql_file_contents = []
            for task_id in form.tasks.data.replace(' ','').split(','):
                task = fetch_task(task_id)
                if task != "":
                    # upload_sql(task_id, task.get('sql'))
                    sql_file_contents.append((task_id, task.get('sql').encode()))
                    task_for_dag = copy.deepcopy(task)
                    task_for_dag['sql'] = 'sql/' + task_id + ".sql"
                    tasks.append(task_for_dag)

            dependencies = []
            for dependency in form.dependencies.data.replace(' ','').split(';'):
                temp = dependency.split(',')
                dependencies.append({
                    'preceding_task_id': temp[0],
                    'task_id': temp[1]
                })

            dag_text = template.render(
                dag_name=form.dag_name.data,
                owner=form.owner.data,
                start_date=form.start_date.data.strftime('%Y-%m-%d'),
                email=form.email.data,
                retries=form.retries.data,
                retry_delay_minutes=form.retry_delay_minutes.data,
                schedule_interval=form.schedule_interval.data,
                tasks=tasks,
                dependencies=dependencies,
            )

            # upload_dag(dag_name=form.dag_name.data, dag_text=dag_text)
            create_github_pr(dag_name=form.dag_name.data,
                dag_file_content=dag_text.encode(),
                sql_file_contents=sql_file_contents,
                committer_name=claims['name'],
                committer_email=claims['email'])

        else:
            if request.args.get('dag_name') is not None:
                dag = fetch_dag(dag_name=request.args.get('dag_name'))
                if dag is not None:
                    form.dag_name.data = dag.get('dag_name')
                    form.owner.data = dag.get('owner')
                    form.start_date.data = datetime.datetime.strptime(dag.get('start_date'),"%Y-%m-%d")
                    form.retries.data = dag.get('retries')
                    form.retry_delay_minutes.data = dag.get('retry_delay_minutes')
                    form.email.data = dag.get('email')
                    form.schedule_interval.data = dag.get('schedule_interval')
                    form.tasks.data = dag.get('tasks')
                    form.dependencies.data = dag.get('dependencies')
        return render_template('dag_form.html', user_data=claims, title='DAG Form', form=form, dag_text=dag_text)

@app.route('/task_form', methods=["GET", "POST"])
def task_form():
    # Verify Firebase auth.
    id_token = request.cookies.get("token")
    error_message = None
    claims = None
    times = None

    if id_token:
        claims = google.oauth2.id_token.verify_firebase_token(
            id_token, firebase_request_adapter)
        form = TaskForm()
        is_save_ok, save_msg = None, None
        is_query_ok, check_query_result = None, None
        run_query_result, run_query_result_headers, run_query_result_msg = [], [], None
        if form.validate_on_submit():
            sql = form.sql.data
            if form.sql_params.data:
                params = form.sql_params.data.replace(' ','').split(',')
                param_dict = {}
                for param in params:
                    param_dict[param.split('=')[0]] = param.split('=')[1]
                sql = Template(sql).render(param_dict)
            is_query_ok, check_query_result = check_query(sql)
            if form.save.data:
                if is_query_ok:
                    is_save_ok, save_msg = store_task(task_id=form.task_id.data,
                        destination_table=form.destination_table.data,
                        sql=sql,
                        sql_params=form.sql_params.data,
                        type_='BQ_ETL',
                        updated_by=claims['email'])
                else:
                    save_msg = "Can not save, something happened, see error msg"
            # elif form.check_query.data:
                # do nothing
            elif form.run_query.data:
                if is_query_ok:
                    run_query_result, run_query_result_msg = run_query(sql)
                    run_query_result_headers = [field.name for field in run_query_result.schema]
                else:
                    run_query_result_msg = "Can not run, something happened, see error msg"
        else:
            if request.args.get('task_id') is not None:
                task = fetch_task(task_id=request.args.get('task_id'))
                if task is not None:
                    form.task_id.data = task.get('task_id')
                    form.destination_table.data = task.get('destination_table')
                    form.sql.data = task.get('sql')
                    form.sql_params.data = task.get('sql_params')

        return render_template('task_form.html', user_data=claims, title='Task Form', form=form, 
            is_save_ok=is_save_ok, save_msg=save_msg,
            is_query_ok=is_query_ok, check_query_result=check_query_result,
            run_query_result_headers=run_query_result_headers,
            run_query_result=run_query_result, run_query_result_msg=run_query_result_msg)

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)