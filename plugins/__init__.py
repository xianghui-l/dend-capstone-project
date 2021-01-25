# from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class CapstoneProjectPlugin(AirflowPlugin):
    name = "capstone_project_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadTableOperator,
        # operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
