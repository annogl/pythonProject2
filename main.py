#python -m pip install apache-flink==1.15.0
from pyflink.common import JsonRowDeserializationSchema, Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment

from read_kafka import reading_kafka_file
from sql_operations import sql_operations_on_data
from write_kafka import writing_kafka_file


def main():
    reading_kafka_file()
    #my_data = {"num":10}
    sql_operations_on_data(reading_kafka_file())
    #writing_kafka_file(sql_operations_on_data(reading_kafka_file()))


if __name__ == "__main__":
    main()
