import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf
from pyflink.table.window import Tumble, Session


def sql_operations_on_data(data):

    data.sql_query("SELECT * FROM %s" % data).execute().print()

    return data


