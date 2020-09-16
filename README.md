# Algo Readiness Monitor

**This is not an officially supported Google product. It is a reference
implementation.**

## Overview

This is a reference implementation for a Cloud Composer solution that will help
ad traders identify which Display and Video 360 Line Items are ideal candidates
for autobidding. The autobidding algorithm requires a certain threshold of
historical data before it can be turned on. However, each line item type has
different thresholds for the number of conversion events to be eligible to turn
on the feature. It is not feasible for partners with large numbers of objects to
manually check for eligibility each day. This solution creates a data pipeline
and dashboard to allow an easy view of all of this.

The solution pulls SDF data and transactional data from Display & Video 360. The
DAG joins the data and runs scoring logic for each line item to create the data
model. A Data Studio visuzalization layer can go on top of the data to create a
dashboard.

***ADD SCREENSHOTS***

## Tech Notes

Composer is a Google Cloud managed version of Apache Airflow, an open source
project for managing ETL workflows. We use it for this solution as you are able
to deploy your code to production simply by moving files to Google Cloud
Storage. It also provides Monitoring, Logging and software installation, updates
and bug fixes for Airflow are fully managed. We recommend familiarising yourself
with Composer [here](https://cloud.google.com/composer/docs/).

Orchestra is an open source project, built on top of Composer, that is custom
operators for Airflow designed to solve the needs of Advertisers. Learn more
about Orchestra [here](https://github.com/google/orchestra).

## Installation

Please speak to your Google representative if you would like a more detailed
installation guide or a deployment consultataion.

In this reference code the variables have been declared in the Airflow UI as
Airflow variables. Note that you could declare all of these variables in the
dashboard.config file if you prefer.

### Composer environment setup

Follow the steps
[here](https://github.com/google/orchestra/blob/master/README.md) to set up your
Composer environment if you do not already have one. Make sure that your DV360
Service account is set up correctly and includes each of the partners you want
to run for.

Enable the DV360 API in your Cloud Project.

***NOTE: You will not need the ERF variables outlined in the Orchestra README.
The necessary variables for this solution are detailed below. ***

### Variables

This project will require several variables to run.

These can be set via the **Admin** section in the **Airflow UI** (accessible
from the list of Composer Environments, clicking on the corresponding link under
"Airflow Web server").

NOTE: You could also declare these variables in the config.json file, **other
than the 'dv360_sdf_advertisers' variable, which is generated as a variable by
the DAG**

### Set the variables below in your Airflow admin

-   `cloud_project_id`: The name of your Cloud project ID
-   `dv360_sdf_advertisers`: Set this to '{"partner": ["adv1"]}' when you start.
    This will be automatically populated by the algo_readiness_scheduler_dag
-   `dv_bq_dataset`: The name of the DV360 BQ dataset. This will hold all DV360
    reporting data and the final joined tables and view
-   `gcs_bucket`: The name of your Cloud storage bucket
-   `number_of_advertisers_per_sdf_api_call`: 10 is recommended, but you can
    edit
-   `partner_ids`: the partners to run the report for, comma-separated
-   `sdf_api_version`: 5
-   `sdf_bq_dataset`: The name of the dataset you'd like to save your SDF data
    to in BigQuery
-   `sdf_file_types`: INSERTION_ORDER,LINE_ITEM

### Set the variables below in the dashboard.config file

-   "gcp_connection_id": Your Airflow connection ID

## Solution Architecture

The solution features 3 pieces:

* algo_readiness_scheduler 

1. Records all the
advertiser IDs for each partner (needed for SDF reporting) and stores them in an
Airflow Variable. 

2. Triggers one reporting DAG for each partner. The reporting
DAGs are built by the algo_readiness_factory.


*   algo_readiness_factory

    1.  This DAG factory generates the workflows needed to produce the SDF and
        DV360 report files. It generates one DAG per partner in the partner_ids
        variable.

*   algo_readiness_reporting

    1.  Pulls SDF Insertion Order and Line Item data into BigQuery for the given
        partner
    2.  Pulls DV360 reporting into BigQuery for the given partner
    3.  Runs joins to create "sdf_dv360_join" tables that have impression data and configurational data in a single place
    4.  Runs a scoring query to calculate if a line item's historical data meets the correct threshold and stores this in the scoring_data tables. Scores can be green (ready), orange (almost ready), or red (not ready).
    5.  Creates the BigQuery view "Final View" that is necessary for the dashboard. This view adds match and opportunity columns to the data for visualization. The match column determines if an IO performance goal and LI bid strategy are matched. It will return "Fixed" for a Fixed LI bid strategy and "N/A" if the IO has a performance goal that doesn't allow LI bid strategy to be set.

    This DAG creates the following tables and views.
    
    In the SDF dataset:
    
    * `SDFLineItem`: Holds line item data for all partners
    * `SDFInsertionOrder`: Holds insertion order data for all partners
    
    In the DV360 dataset:
    
    * `dv360_report_[PARTNERID]`: holds the DV360 reporting data (one table per partner)
    * `sdf_dv360_join_[PARTNERID]`: holds the joined DV360 and SDF data (one table per partner)
    * `scoring_data_[PARTNERID]`: holds the scored DV360 and SDF data (one table per partner)
    * `Final_View`: a single view to hold all data needed for the final dashboard, including Opportunity scores

# Additional info

### Deleting an environment

Full details can be found
[here](https://cloud.google.com/composer/docs/how-to/managing/updating#deleting_an_environment).
Please note that files created by Composer are not automatically deleted and you
will need to remove them manually or they will still incur. Same thing applies
to the BigQuery datasets.

## Data & Privacy

Orchestra is a Framework that allows powerful API access to your data. Liability
for how you use that data is your own. It is important that all data you keep is
secure and that you have legal permission to work and transfer all data you use.
Orchestra can operate across multiple Partners, please be sure that this access
is covered by legal agreements with your clients before implementing Orchestra.
This project is covered by the Apache License.

