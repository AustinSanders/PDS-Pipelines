import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from pds_pipelines.models.pds_models import Files

import hashlib
import shutil
import os
import pytz


def file_lookup(ds, **kwargs):
    ti = kwargs['ti']
    in_list = ti.xcom_pull(task_ids='get_items', dag_id='di_process')
    session = kwargs['session']
    out = list()
    for item in in_list:
        Qelement = session.query(Files).filter(Files.filename == item).one()
        out.append(Qelement)
    return out


def hash_file(ds, **kwargs):
    ti = kwargs['ti']
    archiveID = kwargs['archiveID']
    in_list = ti.xcom_pull(task_ids='file_lookup')
    out = list()
    for item in in_list:
        cpfile = archiveID[item.archiveid] + item.filename
        if os.path.isfile(cpfile):
            f_hash = hashlib.md5()
            with open(cpfile, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    f_hash.update(chunk)
                out.append((item,f_hash.hexdigest()))
        else:
            print('Unable to locate {}'.format(cpfile))
            out.append((item,0))
    return out

def cmp_checksum(ds, **kwargs):
    ti = kwargs['ti']
    in_list = ti.xcom_pull(task_ids='hash_file')
    session = kwargs['session']
    for old, new in in_list:
        if old.checksum == new:
            old.di_pass = True
            print("PASS")
        else:
            old.di_pass = False
            print("FAIL")
        old.di_date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        session.merge(old)
    session.flush()
    session.commit()


def process_subdag(parent_dag_name, child_dag_name, **kwargs):
    schedule_interval = '@once'
    session = kwargs['session']
    archiveID = kwargs['archiveID']
    start_date = datetime.datetime.now()
    dag = DAG(
            '%s.%s' % (parent_dag_name, child_dag_name),
            schedule_interval=schedule_interval,
            start_date = start_date,
            )
    file_lookup_operator = PythonOperator(task_id='file_lookup',
            provide_context=True,
            python_callable=file_lookup,
            op_kwargs={'session':session},
            dag=dag)
    hash_file_operator = PythonOperator(task_id='hash_file',
            provide_context=True,
            python_callable=hash_file,
            op_kwargs={'session':session, 'archiveID':archiveID},
            dag=dag)

    cmp_checksum_operator = PythonOperator(task_id='cmp_checksum',
            provide_context=True,
            python_callable=cmp_checksum,
            op_kwargs={'session':session},
            dag=dag)

    file_lookup_operator >> hash_file_operator >> cmp_checksum_operator
    return dag
