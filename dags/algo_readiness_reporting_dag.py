###########################################################################
#
#  Copyright 2019 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################
'''
  A DAG to do the following:
    1. Pulls SDF Insertion Order and Line Item data into BigQuery for the given partner
    2. Pulls DV360 reporting into BigQuery for the given partner
    3. Runs joins and queries on the data 
    4. Creates the BigQuery view "Final View" that is necessary for the dashboard.
    
  This DAG creates the following tables and views:
  * In the SDF dataset:
    - SDFLineItem: Holds line item data for all partners
    - SDFInsertionOrder: Holds insertion order data for all partners
  * In the DV360 dataset:
    - dv360_report_[PARTNERID]: holds the DV360 reporting data (one table per partner) 
    - sdf_dv360_join_[PARTNERID]: holds the joined DV360 and SDF data (one table per partner)
    - scoring_data_[PARTNERID]: holds the scored DV360 and SDF data (one table per partner) 
    - Final_View: a single view to hold all data needed for the final dashboard, including Opportunity scores 
     
  Define the Airflow variable "sdf_file_types" as 'INSERTION_ORDER,LINE_ITEM'
  Define the Airflow variable "sdf_api_version" as 4.2
'''
import logging
import json
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import (
    GoogleCloudStorageToBigQueryOperator
)
from orchestra.google.marketing_platform.operators.display_video_360 import (
  GoogleDisplayVideo360CreateReportOperator,
  GoogleDisplayVideo360RunReportOperator,
  GoogleDisplayVideo360DeleteReportOperator,
  GoogleDisplayVideo360DownloadReportOperator,
  GoogleDisplayVideo360SDFToBigQueryOperator
)
from orchestra.google.marketing_platform.sensors.display_video_360 import (
    GoogleDisplayVideo360ReportSensor
)
from gps.utils.resource_helpers import ResourceLoader

logger = logging.getLogger(__name__)

# Declare config file path and load config
config_file_path = "dashboard.config"
resource_loader = ResourceLoader()
config = resource_loader.load_config(config_file_path)

# Pull in variables from config and Airflow
conn_id = config["gcp_connection_id"]
filter_type = 'ADVERTISER_ID'
file_types = models.Variable.get('sdf_file_types').split(',')
api_version = models.Variable.get('sdf_api_version')
group_size = int(models.Variable.get('number_of_advertisers_per_sdf_api_call'))
sdf_bq_dataset = models.Variable.get('sdf_bq_dataset')
dv_bq_dataset = models.Variable.get('dv_bq_dataset')
cloud_project_id = models.Variable.get('cloud_project_id')
gcs_bucket = models.Variable.get('gcs_bucket')
view_name = "Final_View" # Necessary for naming the final view in BigQuery
partner_id_list = models.Variable.get('partner_ids').split(",")
gcs_temp_bucket = models.Variable.get('gcs_temp_bucket')

# Helper functions
def yesterday():
  return datetime.today() - timedelta(days=1)

def group_advertisers(l, n):
  for i in range(0, len(l), n):
    yield l[i:i + n]

def _xcom_pull(task, key="return_value"):
  """Generates a Jinja template that pulls an XCom variable for a task.
     XCom allows data to be shared between different tasks.
  Args:
    task: The task instance.
    key: The XCom key to retrieve the value for. (default: return_value)
  Returns:
    The Jinja template representation of the XCom variable.
  """
  return "{{ task_instance.xcom_pull('%s', key='%s') }}" % (task.task_id, key)

# Function to build the DAG
def build(dag_id, default_args, config_path, partner_id):
  """Generates a DAG from the given config file, for a given partner.
  Args:
    dag_id: DAG ID
    default_args: default_args for the DAG
    config_path: The config file to generate the DAG from
    partner_id: the

  Returns:
    The generated DAG.
  """
  resource_loader = ResourceLoader()
  config = resource_loader.load_config(config_path)
  file_creation_date = yesterday()
  file_creation_date = file_creation_date.strftime('%Y%m%d')
  partner_id = partner_id
  dag_id = dag_id
  default_args = default_args
  dv_report_dataset_table = "%s.dv360_report_%s" % (dv_bq_dataset, partner_id)
  sdf_dv_join_dataset_table = "%s.sdf_dv360_join_%s" % (dv_bq_dataset, partner_id)

  # Pull advertisers for a partner from the Airflow Variables
  advertisers_per_partner = models.Variable.get('dv360_sdf_advertisers')
  advertisers_per_partner = json.loads(advertisers_per_partner)

  dag = DAG(dag_id=dag_id,
                default_args=default_args,
                schedule_interval=None)

  # If the partner is not in the advertiser list return an empty DAG
  # (this would happen if the partner has no active advertisers)
  if partner_id not in advertisers_per_partner:
    return_empty_dag = DummyOperator(
        task_id='return_empty_dag_%s' % partner_id,
        dag=dag)
    return dag

  # Pull in the advertisers for the given partner
  advertisers_list = advertisers_per_partner[partner_id]

  # DV360 report parameters
  report_params = {
      "title": "DV360 Data Report",
      "account_id": partner_id
  }

  # DV360 reporting tasks
  start_reporting_workflow = DummyOperator(
        task_id='start_reporting_workflow',
        dag=dag)

  delete_report = GoogleDisplayVideo360DeleteReportOperator(
      task_id="delete_report",
      gcp_conn_id=config["gcp_connection_id"],
      query_title=report_params["title"], # note -- not 100% sure about this one, test
      ignore_if_missing=True,
      dag=dag)

  create_report = GoogleDisplayVideo360CreateReportOperator(
      task_id="create_report",
      gcp_conn_id=config["gcp_connection_id"],
      profile_id=partner_id,
      report=resource_loader.get_report_path("dv360_data.json"),
      params=report_params,
      dag=dag)

  wait_for_report = GoogleDisplayVideo360ReportSensor(
      task_id="wait_for_report",
      gcp_conn_id=config["gcp_connection_id"],
      query_id=_xcom_pull(create_report, "query_id"),
      file_id=_xcom_pull(create_report, "file_id"),
      dag=dag)

  download_report = GoogleDisplayVideo360DownloadReportOperator(
      task_id="download_report",
      gcp_conn_id=config["gcp_connection_id"],
      query_id=_xcom_pull(create_report, "query_id"),
      file_id=_xcom_pull(create_report, "file_id"),
      destination_bucket=gcs_temp_bucket,
      chunk_size=100 * 1024 * 1024,
      report_url=_xcom_pull(wait_for_report, 'report_url'),
      dag=dag)

  load_bq_data = GoogleCloudStorageToBigQueryOperator(
      task_id="load_bq_data",
      google_cloud_storage_conn_id=config["gcp_connection_id"],
      bigquery_conn_id=config["gcp_connection_id"],
      bucket=_xcom_pull(download_report, "destination_bucket"),
      source_objects=[_xcom_pull(download_report, "destination_object")],
      destination_project_dataset_table="%s.dv360_report_%s" % (
          dv_bq_dataset, partner_id),
      schema_fields=resource_loader.get_schema("dv360_data.json"),
      max_bad_records=100,
      write_disposition="WRITE_TRUNCATE",
      dag=dag)


  # Create SDF tables if they do not already exist
  create_sdf_bq_table_if_not_exist = """
    bq show --format=none {0}.{1}
    if [ $? -ne 0 ]; then
      bq mk --table {0}.{1}
    fi
  """

  create_LI_table_task = BashOperator(
      task_id='create_SDF_LineItem_table',
      bash_command=create_sdf_bq_table_if_not_exist.format(
          sdf_bq_dataset,
          "SDFLineItem"),
      dag=dag)

  create_IO_table_task = BashOperator(
      task_id='create_SDF_InsertionOrder_table',
      bash_command=create_sdf_bq_table_if_not_exist.format(
           sdf_bq_dataset,
          "SDFInsertionOrder"),
      dag=dag)

  # SDF reporting Tasks
  tasks = []
  advertiser_groups = group_advertisers(advertisers_list, group_size)
  for group_number, group in enumerate(advertiser_groups):
      if partner_id.isdigit():
        message = 'RUNNING REQUESTS FOR A PARTNER: %s, ADVERTISERS: %s' % (
            partner_id, group)
        logger.info(message)
        task_id = 'upload_sdf_%s_%s_%s' % (partner_id, group_number, group[0])
        write_disposition = 'WRITE_APPEND'
        task = GoogleDisplayVideo360SDFToBigQueryOperator(
            task_id=task_id,
            gcp_conn_id=conn_id,
            cloud_project_id=cloud_project_id,
            gcs_bucket=gcs_bucket,
            bq_dataset=sdf_bq_dataset,
            write_disposition=write_disposition,
            ignoreUnknownValues="True",
            filter_ids=group,
            api_version=api_version,
            file_types=file_types,
            filter_type=filter_type,
            dag=dag)
        tasks.append(task)

  # Query and join tasks
  run_sdf_dv360_join_query = BigQueryOperator(
      task_id="run_sdf_dv360_join_query",
      bigquery_conn_id=config["gcp_connection_id"],
      use_legacy_sql=False,
      sql=resource_loader.get_query_path("sdf_dv360_join.sql"),
      params={
          "sdf_report_dataset": sdf_bq_dataset,
          "dv_report_dataset": dv_bq_dataset,
          "partner_id": partner_id
      },
      destination_dataset_table="%s.sdf_dv360_join_%s" % (
          dv_bq_dataset, partner_id),
      create_disposition="CREATE_IF_NEEDED",
      write_disposition="WRITE_TRUNCATE",
      allow_large_results=True,
      dag=dag)

  run_scoring_query = BigQueryOperator(
      task_id="run_scoring_query",
      sql=resource_loader.get_query_path("scoring.sql"),
      use_legacy_sql=False,
      allow_large_results=True,
      params={
          "report_dataset": dv_bq_dataset,
          "partner_id": partner_id
      },
      destination_dataset_table="%s.scoring_data_%s" % (
          dv_bq_dataset, partner_id),
      create_disposition="CREATE_IF_NEEDED",
      write_disposition="WRITE_TRUNCATE",
      bigquery_conn_id=config["gcp_connection_id"],
      dag=dag)

  # Bash operator tasks to create final view
  create_view_if_not_exists = """
    bq show --format=none {0}:{1}.{2}
    if [ $? -ne 0 ]; then
      bq mk \
      --use_legacy_sql=false \
      --description "View to hold all DV360 data" \
      --view 'SELECT
        partner_id,
        partner,
        advertiser_id, 
        advertiser,
        campaign_id, 
        insertion_order,
        insertion_order_id,
        line_item,
        line_item_id, 
        line_item_type,
        li_bid_strategy,
        io_performance_goal,
        CPC_Score, 
        CPA_Score, 
        CPI_Score, 
        AV_Score, 
        CIVA_Score, 
        TOS_Score,
         CASE
            WHEN (io_performance_goal = "CPC" AND CPC_Score in ("GREEN", "ORANGE")) THEN "Opportunity" 
            WHEN (io_performance_goal = "CPA" AND CPA_Score in ("GREEN", "ORANGE")) THEN "Opportunity" 
            WHEN (io_performance_goal = "CTR" AND CPC_Score in ("GREEN", "ORANGE")) THEN "Opportunity" 
            WHEN (io_performance_goal = "CPIAVC" AND CIVA_Score in ("GREEN", "ORANGE")) THEN "Opportunity" 
            WHEN (io_performance_goal = "CPV" AND (CIVA_Score in ("GREEN", "ORANGE") OR TOS_Score in ("GREEN", "ORANGE"))) THEN "Opportunity"
            WHEN (io_performance_goal = "% Viewability" AND AV_Score in ("GREEN", "ORANGE")) THEN "Opportunity" 
            ELSE "Not Opportunity"
         END AS opportunity,
         CASE 
          WHEN li_bid_strategy = "CPM" AND io_performance_goal = "CPM" THEN "Matched"
          WHEN li_bid_strategy = "CPA" AND io_performance_goal = "CPA" THEN "Matched"
          WHEN li_bid_strategy = "CPC" AND io_performance_goal = "CPC" THEN "Matched"
          WHEN (li_bid_strategy = "CPV" OR li_bid_strategy = "vCPM" OR li_bid_strategy = "IVO_TEN") AND io_performance_goal = "CPV" THEN "Matched"
          WHEN li_bid_strategy = "CIVA" AND io_performance_goal = "CPIAVC" THEN "Matched"
          WHEN li_bid_strategy = "CPC" AND io_performance_goal = "CTR" THEN "Matched"
          WHEN io_performance_goal = "% Viewability" OR io_performance_goal = "None" OR io_performance_goal = "Other" THEN "N/A"
          WHEN li_bid_strategy = "Fixed" THEN "Fixed"
          ELSE "Mismatched"
         END AS match_type,
        sum(media_cost_past_7_days) media_cost_past_7_days,
        sum(imps) impressions,
        sum(clicks) clicks, 
        sum(conversions) conversions,
        sum(viewable_imps) viewable_imps,
        sum(audible_visible_imps) audible_visible_imps,
        sum(viewable_10s_imps) viewable_10s_imps
      FROM `{0}.{1}.scoring_data_*`
      GROUP BY
      partner_id,
        partner,
        advertiser_id, 
        advertiser,
        campaign_id, 
        insertion_order,
        insertion_order_id,
        line_item,
        line_item_id, 
        line_item_type,
        li_bid_strategy,
        io_performance_goal,
        CPC_Score, 
        CPA_Score, 
        CPI_Score, 
        AV_Score, 
        CIVA_Score, 
        TOS_Score,
        opportunity,
        match_type;' \
        --project_id {0} \
      {1}.{2}
    fi
  """
  create_view_task = BashOperator(
      task_id='create_' + dv_bq_dataset + '_view',
      bash_command=create_view_if_not_exists.format(
          cloud_project_id,
          dv_bq_dataset,
          view_name),
      dag=dag)

  end_reporting_workflow = DummyOperator(
        task_id='end_reporting_workflow',
        dag=dag)

  # Set task dependencies
  create_LI_table_task >> create_IO_table_task >> tasks[0] >> tasks[1:]
  start_reporting_workflow \
  >> delete_report \
  >> create_report \
  >> wait_for_report \
  >> download_report \
  >> load_bq_data
  load_bq_data >> run_sdf_dv360_join_query
  (tasks[-1]) >> run_sdf_dv360_join_query
  run_sdf_dv360_join_query >> run_scoring_query >> create_view_task >> end_reporting_workflow

  return dag
