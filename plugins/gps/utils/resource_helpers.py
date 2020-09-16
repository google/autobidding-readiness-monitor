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

"""Set of utility classes for the Algo Readiness solution.
"""

import json


class ResourceLoader(object):
  """Set of helper functions for loading DAG resources.
  """

  def __init__(self, airflow_gcs_root='/home/airflow/gcs'):
    self._airflow_gcs_root = airflow_gcs_root
    self._schema_cache = {}

  def load_config(self, config_file_name):
    """Loads the configuration data from the given path.

    Args:
      config_file_name: The name of the configuration file to load.

    Returns:
      The contents of the configuration file as a JSON object.
    """
    config_path = ('%s/dags/%s' % (self._airflow_gcs_root, config_file_name))
    with open(config_path, 'r') as config_file:
      return json.load(config_file)

  def get_report_path(self, report_file_name):
    """Retrieves the path to the report with the given file name.

    Args:
      report_file_name: The name of the report.

    Returns:
      The path to the report.
    """
    # return './dags/resources/reports/%s' % report_file_name -- mine
    return './resources/reports/%s' % report_file_name


  def get_query_path(self, query_file_name):
    """Retrieves the path to the query with the given file name.

    Args:
      query_file_name: The name of the query.

    Returns:
      The path to the query file.
    """
    return './resources/queries/%s' % query_file_name

  def get_schema(self, schema_file_name):
    """Loads the JSON table schema from the given file.

    Args:
      schema_file_name: The name of the schema file to load.

    Returns:
      The table schema as a JSON object.
    """
    if schema_file_name not in self._schema_cache:
      schema_file_path = ('%s/dags/resources/schemas/%s' % (
          self._airflow_gcs_root, schema_file_name))
      with open(schema_file_path) as schema_file:
        self._schema_cache[schema_file_name] = json.load(schema_file)

    return self._schema_cache[schema_file_name]

