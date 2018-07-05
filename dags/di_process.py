import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.RedisQueue import RedisQueue

import hashlib
import shutil
import os
import pytz


archiveID = {53: '/pds_san/PDS_Archive/Cassini/ISS/',
                51: '/pds_san/PDS_Archive/Cassini/RADAR/',
                75: '/pds_san/PDS_Archive/Cassini/VIMS/',
                97: '/pds_san/PDS_Archive/Apollo/Metric_Camera/',
                101: '/pds_san/PDS_Archive/Apollo/Rock_Sample_Images/',
                50: '/pds_san/PDS_Archive/Clementine/',
                104: '/pds_san/PDS_Archive/Chandrayaan_1/M3/',
                119: '/pds_san/PDS_Archive/Dawn/Vesta/',
                123: '/psa_san/PDS_Archive/Dawn/Ceres/',
                24: '/pds_san/PDS_Archive/Galileo/NIMS/',
                25: '/pds_san/PDS_Archive/Galileo/SSI/',
                92: '/pds_san/PDS_Safed/Data/Kaguya/LISM/',
                38: '/pds_san/PDS_Archive/LCROSS/',
                79: '/pds_san/PDS_Archive/Lunar_Orbiter/',
                116: '/pds_san/PDS_Archive/Mars_Odyssey/THEMIS/USA_NASA_PDS_ODTSDP_100XX/',
                117: '/pds_san/PDS_Archive/Mars_Odyssey/THEMIS/USA_NASA_PDS_ODTSDP_100XX/',
                118: '/pds_san/PDS_Archive/Mars_Odyssey/THEMIS/USA_NASA_PDS_ODTGEO_200XX/',
                41: '/pds_san/PDS_Derived/Map_A_Planet/',
                44: '/pds_san/PDS_Archive/Mars_Global_Surveyor/MOC/',
                16: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX/',
                17: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/MARCI/',
                74: '/pds_san/PDS_Archive/Lunar_Reconnaissance_Orbiter/LROC/EDR/',
                84: '/pds_san/PDS_Archive/Lunar_Reconnaissance_Orbiter/LAMP/',
                71: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/HiRISE/',
                124: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/HiRISE/',
                125: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/HiRISE/',
                126: '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/HiRISE/',
                46: '/pds_san/PDS_Archive/Mariner_10/',
                27: '/pds_san/PDS_Archive/Magellan/',
                78: '/pds_san/PDS_Archive/Mars_Express/',
                18: '/pds_san/PDS_Archive/Mars_Pathfinder/',
                14: '/pds_san/PDS_Archive/MESSENGER/',
                9: '/pds_san/PDS_Archive/Phoenix/',
                7: '/pds_san/PDS_Archive/Viking_Lander/',
                3: '/pds_san/PDS_Archive/Viking_Orbiter/',
                30: '/pds_san/PDS_Archive/Voyager/'
                }


dag = DAG('di_process', description='Data integrity check for PDS products',
          schedule_interval='0 12 * * *',
          start_date=datetime.datetime.now())

def get_items(ds, **kwargs):
    outlist = list()
    n_items = kwargs.get('n_items')
    rq = kwargs.get('rq')
    while (rq.QueueSize() > 0 and n_items > 0):
        inputfile = rq.QueueGet().decode('utf-8')
        outlist.append(inputfile)
        n_items -= 1
    return outlist


def file_lookup(ds, **kwargs):
    ti = kwargs['ti']
    in_list = ti.xcom_pull(task_ids='get_items')
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
    print(in_list)
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
    print(session)
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



# @TODO find a way to make these separate tasks.  Difficult because they
#  can't be pickled, therefore they can't be returned via a task.
session, _ = db_connect('pdsdi_dev')
rq = RedisQueue('DI_ReadyQueue')


dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)
get_items_operator = PythonOperator(task_id='get_items',
        provide_context=True,
        python_callable=get_items,
        op_kwargs={'n_items':50, 'rq':rq},
        dag=dag)

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


dummy_operator >> get_items_operator >> file_lookup_operator >> hash_file_operator >> cmp_checksum_operator
