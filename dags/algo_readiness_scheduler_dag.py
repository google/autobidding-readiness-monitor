#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Scheduler for the Algo Readiness Monitor.

   This DAG does 2 things:
     1. Records all the advertiser IDs for each partner (needed for SDF reporting) and stores them in an Airflow Variable.
     2. Triggers one reporting DAG for each partner. The reporting DAGs are built by the algo_readiness_factory
"""

import datetime
import glob
from airflow import DAG
from airflow import models
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from orchestra.google.marketing_platform.operators.display_video_360 import (
  GoogleDisplayVideo360CreateReportOperator,
  GoogleDisplayVideo360RunReportOperator,
  GoogleDisplayVideo360DeleteReportOperator,
  GoogleDisplayVideo360RecordSDFAdvertiserOperator
)
from orchestra.google.marketing_platform.sensors.display_video_360 import (
  GoogleDisplayVideo360ReportSensor
)
from gps.utils.resource_helpers import ResourceLoader
import algo_readiness_factory_dag

# Declare config file path and load config
config_file_path = "dashboard.config"
resource_loader = ResourceLoader()
config = resource_loader.load_config(config_file_path)
CONN_ID = config["gcp_connection_id"]
partner_id_list = models.Variable.get("partner_ids").split(",")

def _get_default_args():
  """Builds the default DAG arguments.

  Returns:
    The default arguments.
  """
  yesterday = datetime.datetime.combine(
      datetime.datetime.today() - datetime.timedelta(days=1),
      datetime.datetime.min.time())

  default_args = {
      'owner': 'airflow',
      'depends_on_past': False,
      'start_date': yesterday,
      'email_on_failure': False,
      'email_on_retry': False,
      'retries': 1,
      'retry_delay': datetime.timedelta(minutes=5)
  }

  return default_args

def _get_dag_run_obj(context, dag_run_obj):
  return dag_run_obj

def _build(dag_id, default_args):
  """Builds a new DAG defining the Algo Readiness workflow.

  Args:
    dag_id: The DAG ID.
    default_args: The default arguments for the DAG.

  Returns:
    The DAG object.
  """

  config_dag = DAG(dag_id=dag_id, default_args=default_args)

  # Define SDF record advertiser tasks
  start_workflow = DummyOperator(
        task_id='start_workflow',
        dag=config_dag)

  create_report = GoogleDisplayVideo360CreateReportOperator(
    task_id="create_report",
    gcp_conn_id=CONN_ID,
    report=resource_loader.get_report_path("dv360_adv_report.json"),
    params={"partners": partner_id_list},
    dag=config_dag)

  query_id = "{{ task_instance.xcom_pull('create_report', key='query_id') }}"

  run_report = GoogleDisplayVideo360RunReportOperator(
      task_id="run_report",
      gcp_conn_id=CONN_ID,
      query_id=query_id,
      dag=config_dag)

  wait_for_report = GoogleDisplayVideo360ReportSensor(
      task_id="wait_for_report",
      gcp_conn_id=CONN_ID,
      query_id=query_id,
      dag=config_dag)

  report_url = "{{ task_instance.xcom_pull('wait_for_report', key='report_url') }}"

  record_advertisers = GoogleDisplayVideo360RecordSDFAdvertiserOperator(
      task_id='record_advertisers',
      conn_id=CONN_ID,
      report_url=report_url,
      variable_name='dv360_sdf_advertisers',
      dag=config_dag)

  delete_report = GoogleDisplayVideo360DeleteReportOperator(
    task_id="delete_report",
    gcp_conn_id=CONN_ID,
    query_id=query_id,
    dag=config_dag)

  # Set dependencies for recording advertisers
  start_workflow >> create_report >> run_report >> wait_for_report >> record_advertisers >> delete_report

  # Trigger one reporting DAG for each partner
  for partner_id in partner_id_list:
    trigger_dag_id = algo_readiness_factory_dag.build_dag_id(
        partner_id)
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_%s' % trigger_dag_id,
        trigger_dag_id=trigger_dag_id,
        python_callable=_get_dag_run_obj,
        dag=config_dag)
    delete_report.set_downstream(trigger_dag_task)

  return config_dag

dag = _build(
    dag_id='algo_readiness_scheduler',
    default_args=_get_default_args())
