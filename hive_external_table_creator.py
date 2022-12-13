from pyspark.sql import SparkSession
import argparse
import json
import subprocess
import sys
import xml.etree.ElementTree as ET




def get_from_gcs_to_hdfs(bucket_name, data_file_path):
    try:
        command = "hadoop distcp -overwrite gs://" + bucket_name + data_file_path + " " + data_file_path
        print("[INFO] Command : " + command)
        subprocess.check_output(["hdfs", "dfs", "-rm", "-r", "-f", data_file_path])
        subprocess.check_output(
            ["hadoop", "distcp", "-overwrite", "gs://" + bucket_name + data_file_path, data_file_path])
        print("[INFO] Successfully Pulled Data Files from GCS to HDFS ")
    except Exception as error:
        print("[ERROR] Failed to Pull Data Files from GCS to HDFS ")
        sys.exit(-100)


def parse_hdfs_site_config():
    try:
        tree = ET.parse('/etc/hadoop/conf/hdfs-site.xml')
        root = tree.getroot()
        name_node_address = ''
        for row in root.findall('property'):
            if row.find('name').text == 'dfs.namenode.http-address':
                name_node_address = row.find('value').text
                break
                # print(row.find('name').text, " ", row.find('value').text)

        return name_node_address
    except Exception as error:
        print('[ERROR] Failed to Parse Hdfs Site Config : ' + str(error))


def get_partition_columns(data_file_path):
    try:
        partition_columns_identified = []

        host = parse_hdfs_site_config()
        fs = pyhdfs.HdfsClient(host)

        dir_generator = fs.walk(data_file_path)
        root, directories, files = next(dir_generator)
        # 1st Partition Column Check
        if directories:
            partition_columns_identified.append(directories[0].split("=")[0])

        while len(directories) != 0:
            root, directories, files = next(dir_generator)
            # print(root,dir,files)
            if directories:
                partition_columns_identified.append(directories[0].split("=")[0])

        return partition_columns_identified

    except Exception as error:
        print("[ERROR] Failed to Identify Partition Columns from Path : " + str(error))
        sys.exit(-100)


def convert_spark_to_hive_datatype(spark_datatype, column_metadata={}):
    try:
        hive_datatype = ''
        if 'decimal' in spark_datatype:
            hive_datatype = spark_datatype
        elif spark_datatype == 'array':
            if 'type' in column_metadata['elementType']:
                hive_datatype = 'array<' + convert_spark_to_hive_datatype(column_metadata['elementType']['type'],
                                                                          column_metadata['elementType']) + '>'
            else:
                hive_datatype = 'array <' + str(convert_spark_to_hive_datatype(column_metadata['elementType'])) + '>'
        elif spark_datatype == 'map':
            if 'type' in column_metadata['valueType']:
                hive_datatype = 'map <' + convert_spark_to_hive_datatype(
                    column_metadata['keyType']) + ',' + convert_spark_to_hive_datatype(
                    column_metadata['valueType']['type'], column_metadata['valueType']) + '>'
            else:
                hive_datatype = 'map <' + convert_spark_to_hive_datatype(
                    column_metadata['keyType']) + ',' + convert_spark_to_hive_datatype(
                    column_metadata['valueType']) + '>'
        elif spark_datatype == 'struct':
            hive_datatype = 'struct < '
            for row in column_metadata['fields']:
                if 'type' in row['type']:
                    hive_datatype = hive_datatype + row['name'] + ':' + convert_spark_to_hive_datatype(
                        row['type']['type'],
                        row['type']) + ','
                else:
                    hive_datatype = hive_datatype + row['name'] + ':' + convert_spark_to_hive_datatype(
                        row['type']) + ','
            hive_datatype = hive_datatype[:-1] + '>'
        else:
            hive_datatype = Spark_to_Hive_DataTypes[spark_datatype]

        return hive_datatype
    except Exception as err:
        print("[ERROR] Failed to convert to target datatype : " + str(err))


def generate_hive_create_statement(dataframe, database_name, table_name, partition_columns, file_format, path):
    try:
        print("[INFO] Generating Hive Create Table Script")
        create_table_section = "CREATE EXTERNAL TABLE IF NOT EXISTS `" + database_name + "`.`" + table_name + "`"
        column_section = ""
        partitioned_section = ""
        format_location_section = "STORED AS " + file_format.upper() + "  LOCATION '" + path + "'"
        final_partition_columns = []
        for p_column in partition_columns:
            if p_column in dataframe.columns:
                final_partition_columns.append(p_column)
            else:
                print("[FAILED]'" + p_column + "' Column not found in the given file")

        for column in dataframe.schema:
            if 'type' in column.dataType.json():
                column_metadata = json.loads(column.dataType.json())
                column_datatype = column_metadata['type']
                target_datatype = convert_spark_to_hive_datatype(column_datatype, column_metadata).upper()
            else:
                column_datatype = column.dataType.json().replace('"', '')
                target_datatype = convert_spark_to_hive_datatype(column_datatype).upper()

            if column.name in final_partition_columns:
                partitioned_section = partitioned_section + "`" + column.name + "` " + target_datatype + ","
            else:
                column_section = column_section + "`" + column.name + "`  " + target_datatype + ",\n"

        if len(final_partition_columns) > 0:
            partitioned_section = "PARTITIONED BY (" + partitioned_section[:-1] + ") "
            partition_repair_flag = True
        else:
            partitioned_section = ""
            partition_repair_flag = False
        hiveCreateStatement = create_table_section + "( \n" + column_section[
                                                              :-2] + " ) " + partitioned_section + format_location_section

        return hiveCreateStatement, partition_repair_flag
    except Exception as err:
        print("[ERROR] Failed to generate Hive Create Table Statement : " + str(err))
        sys.exit(-100)


if __name__ == "__main__":
    try:
        try:
            import pyhdfs
        except ModuleNotFoundError as modduleerror:
            print("[ERROR] Pyhdfs module not found : "+str(modduleerror))
            subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                                   'pyhdfs'])
            import pyhdfs

        parser = argparse.ArgumentParser(description='Generates Hive Create Table Script from given file')
        parser.add_argument('--file_format', type=str, help='File Type')
        parser.add_argument('--path', metavar='PATH', type=str, help='The Path of the .parquet file', required=True)
        parser.add_argument('--database_name', type=str, help='The Name of the Database where the table to be created',
                            required=True)
        parser.add_argument('--table_name', type=str, help='The Name of the Table', required=True)
        # parser.add_argument('--partition_columns', type=str, help='The Columns used to partition the data', default='')
        parser.add_argument('--gcs_bucket', type=str, help='The Bucket Name of Google Storage where the files are '
                                                           'located', default='', required=True)

        args = parser.parse_args()

        file_format = args.file_format
        path = args.path
        databasename = args.database_name
        tablename = args.table_name
        gcs_bucket = args.gcs_bucket

        get_from_gcs_to_hdfs(gcs_bucket, path)

        # destination_path = args.destination_path
        '''
        if args.partition_columns == '':
            partition_columns = []
        else:
            partition_columns = [column.strip() for column in args.partition_columns.split(',')]
        '''
        Spark_to_Hive_DataTypes = {
            'byte': 'TinyInt',
            'short': 'SmallInt',
            'integer': 'Int',
            'long': 'BigInt',
            'float': 'Float',
            'double': 'Double',
            'decimal': 'Decimal',
            'string': 'String',
            'binary': 'Binary',
            'boolean': 'Boolean',
            'timestamp': 'Timestamp',
            'date': 'Date',
            'array': 'Array',
            'map': 'Map',
            'struct': 'Struct'
        }

        spark = SparkSession.builder.appName("Hive_ExternalTable_Creation"). \
            enableHiveSupport(). \
            config("spark.sql.warehouse.dir", "/apps/hive/warehouse"). \
            getOrCreate()

        DF = ''
        if file_format == "parquet":
            DF = spark.read.parquet(args.path)
        elif file_format == "orc":
            DF = spark.read.orc(args.path)

        partition_columns = get_partition_columns(path)
        # print(partition_columns)

        generated_script, repair_flag = generate_hive_create_statement(DF, databasename, tablename, partition_columns,
                                                                       file_format,
                                                                       path)
        print("[INFO] Generated Hive Create Table Script Successfully")
        spark.sql("CREATE SCHEMA IF NOT EXISTS " + databasename)
        print(generated_script)
        spark.sql(generated_script)
        print("[INFO] Successfully Created the Table : " + databasename + "." + tablename)

        if repair_flag:
            partition_repair_sql = "MSCK REPAIR TABLE " + databasename + "." + tablename
            spark.sql(partition_repair_sql)

    except Exception as error:
        print('[ERROR] Failed to Create Hive Table : ' + str(error))
        sys.exit(-100)
