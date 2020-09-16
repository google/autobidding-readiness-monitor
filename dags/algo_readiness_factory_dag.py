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

"""
    Factory for the algo readiness monitor.
   
   This DAG factory generates the workflows needed to produce the SDF and DV360 report files.
   This builds one DAG per partner in the partner_ids variable.
"""
import datetime
import glob
from airflow import models
from gps.utils.resource_helpers import ResourceLoader
import algo_readiness_reporting_dag

# Pull in config and partner_ids
config_path = "dashboard.config"
resource_loader = ResourceLoader()
config = resource_loader.load_config(config_path)
partner_id_list = models.Variable.get('partner_ids').split(",")

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
      'retry_delay': datetime.timedelta(minutes=5),
      'schedule_interval': None
  }

  return default_args


def build_dag_id(partner_id):
  """Builds the DAG ID for the given Airflow variable.

  Args:
    partner_id: Partner ID to build the dag_id for.

  Returns:
    The DAG ID.
  """
  dag_name = 'algo_readiness_reporting_%s_dag' % partner_id
  return dag_name

for partner_id in partner_id_list:
  dag = algo_readiness_reporting_dag.build(
      dag_id=build_dag_id(partner_id),
      default_args=_get_default_args(),
      config_path=config_path,
      partner_id=partner_id
   )
  globals()[dag.dag_id] = dag

