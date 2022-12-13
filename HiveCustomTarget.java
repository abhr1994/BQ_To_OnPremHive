package com.infoworks;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.infoworks.awb.extensions.api.spark.ProcessingContext;
import io.infoworks.awb.extensions.api.spark.SparkCustomTarget;
import io.infoworks.awb.extensions.api.spark.UserProperties;
import io.infoworks.platform.common.encryption.AES256;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import sun.misc.BASE64Decoder;

public class HiveCustomTarget implements SparkCustomTarget, AutoCloseable {
    private Logger LOGGER = LoggerFactory.getLogger(HiveCustomTarget.class);
    private String HIVE_DRIVER_NAME = "cdata.jdbc.apachehive.ApacheHiveDriver";
    private String jdbc_url;
    private String table;
    private String externalTablePath;
    private String fileStorageFormat;

    private List<String> partitionColumns = new ArrayList<>();
    private Map<String, String> derivedPartitionColumns = new HashMap<>();
    private List<String> bucketingColumns = new ArrayList<>();

    private List<String> mergeKeys = new ArrayList<>();
    private String numberOfBuckets;
    private TargetSyncMode syncMode;
    private int numOfPartitions = 10;
    private Connection jdbConnection;

    private String partitionByClause;
    private String bucketingClause;
    private List<String> columnNames;
    private LinkedHashMap<String, String> hiveColumnsDataTypeMapping;
    private List<String> arrayStructColumns =  new ArrayList<>();
    SparkSession spark;
    UserProperties userProperties;

    private void validateAndInitConfigs(UserProperties userProperties) throws IOException {
        this.jdbc_url = userProperties.getValue("jdbcUrl");
        Preconditions.checkNotNull(this.jdbc_url, String.format("Pass the JDBC URL. Refer https://cdn.cdata.com/help/FIH/jdbc/pg_connectionj.htm. Please set property : %s ", "jdbcUrl"));
        this.table = userProperties.getValue("table");
        Preconditions.checkNotNull(this.table, String.format("Table name can not be null. Please set property : %s ", "table"));
        String mode = userProperties.getValue("syncMode");
        if (mode == null || (!"overwrite".equals(mode) && !"append".equals(mode) && !"merge".equals(mode))) {
            this.syncMode = HiveCustomTarget.TargetSyncMode.valueOf("overwrite");
        } else {
            this.syncMode = HiveCustomTarget.TargetSyncMode.valueOf(mode);
        }
        this.externalTablePath = userProperties.getValue("externalTablePath");
        // Preconditions.checkNotNull(this.externalTablePath, String.format("External Table path can not be null. Please set property : %s ", "externalTablePath"));
        this.fileStorageFormat = userProperties.getValue("fileStorageFormat");
        if (this.fileStorageFormat == null) {
            this.fileStorageFormat = "ORC";
        }
        String partitionColumnsStr = userProperties.getValue("partitionColumns");
        if (partitionColumnsStr != null) {
            List<String> list = Arrays.asList(partitionColumnsStr.split(","));
            this.partitionColumns = new ArrayList<>(list);
        }
        String derivedPartitionColumnsStr = userProperties.getValue("derivedPartitionColumns");
        if (derivedPartitionColumnsStr != null) {
            String[] myData = derivedPartitionColumnsStr.split(",");
            for(String item:myData) {
                String[] temp = item.split(":");
                this.derivedPartitionColumns.put(temp[1], temp[0] +" AS "+temp[1]);
                this.partitionColumns.add(temp[1]);
            }
        }

        String bucketingColumnsStr = userProperties.getValue("bucketingColumns");
        if (bucketingColumnsStr != null) {
            this.bucketingColumns = Arrays.asList(bucketingColumnsStr.split(","));
        }
        this.numberOfBuckets = userProperties.getValue("numberOfBuckets");

        String mergeKeyStr = userProperties.getValue("mergeKeys");
        if (mergeKeyStr != null) {
            this.mergeKeys = Arrays.asList(mergeKeyStr.split(","));
        }

        String no_of_partitions = userProperties.getValue("numOfRePartitions");
        if (no_of_partitions != null) {
            this.numOfPartitions = Integer.parseInt(userProperties.getValue("numOfRePartitions"));
        }
    }

    public Connection getJDBCConnection() throws SQLException {
        try
        {
            Class.forName(this.HIVE_DRIVER_NAME);
        }
        catch (Exception ex)
        {
            System.err.println("Driver not found");
        }
        //System.out.println("Trying to connect to "+this.jdbc_url);
        return DriverManager.getConnection(this.jdbc_url);
    }

    private void overwrite(Dataset tempDataset) {
        LOGGER.info("Writing to Hive in overwrite mode");
        String internalHiveTable = this.table + "_iwx_internal";
        if (this.externalTablePath != null) {
            // Create external hive table
            if (this.partitionColumns.size() == 0) {
                // Overwrite table without partition column
                LOGGER.info("Writing to Hive table in overwrite mode. External Table is being created without any partitions");
                // Set table properties to drop the data on delete
                this.enablePurgeExternalTable(this.table);
                this.createExternalHiveTable(this.table, this.externalTablePath, this.fileStorageFormat, false);
                tempDataset.write()
                        .format("jdbc")
                        .option("url", this.jdbc_url)
                        .option("dbtable", this.table)
                        .option("driver", this.HIVE_DRIVER_NAME)
                        .mode("append")
                        .save();
            } else {
                LOGGER.info("Writing to Hive table in overwrite mode. External Table is being created with partitions");
                this.createTable(internalHiveTable, true, true, true);
                tempDataset.write()
                        .format("jdbc")
                        .option("url", this.jdbc_url)
                        .option("dbtable", internalHiveTable)
                        .option("driver", this.HIVE_DRIVER_NAME)
                        .mode("append")
                        .save();
                this.createExternalHiveTable(this.table, this.externalTablePath, this.fileStorageFormat, false);
                String sqlToInsert = getSqlToInsertToExternalHiveTable(this.table, internalHiveTable, "overwrite");
                this.executeDDLStatement(sqlToInsert);
            }

        } else {
            // Internal table
            if (this.partitionColumns.size() == 0) {
                LOGGER.info("Writing to Hive table in overwrite mode. Internal Managed Hive Table is being created without any partitions");
                this.createTable(this.table, false, false, false);
                tempDataset.write()
                        .format("jdbc")
                        .option("url", this.jdbc_url)
                        .option("dbtable", this.table)
                        .option("driver", this.HIVE_DRIVER_NAME)
                        .mode("append")
                        .save();
            } else {
                LOGGER.info("Writing to Hive table in overwrite mode. Internal Managed Hive Table is being created with partitions");
                this.createTable(internalHiveTable, true, true, true);
                tempDataset.write()
                        .format("jdbc")
                        .option("url", this.jdbc_url)
                        .option("dbtable", internalHiveTable)
                        .option("driver", this.HIVE_DRIVER_NAME)
                        .mode("append")
                        .save();
                this.createTable(this.table, false, false, false);
                String sqlToInsert = getSqlToInsertToExternalHiveTable(this.table, internalHiveTable, "overwrite");
                this.executeDDLStatement(sqlToInsert);
            }
        }
    this.dropTableIfExists(internalHiveTable);
    }
    @Override
    public void writeToTarget(Dataset dataset) {
        //JdbcDialects.registerDialect(new HiveDialect());
        this.hiveColumnsDataTypeMapping = this.getHiveColumnsDataTypeMapping(dataset);
        this.setPartitionAndBucketing();
        Dataset tempDataset = dataset;
        String schemaName = this.table.split("[.]")[0];
        this.createSchemaIfNotExists(schemaName);

        for (String column : this.arrayStructColumns) {
            tempDataset = tempDataset.withColumn(column, functions.to_json(functions.col(column)));
        }
        for (String c : tempDataset.columns()) {
            if (!this.arrayStructColumns.contains(c)) {
                tempDataset = tempDataset.withColumn(c, tempDataset.col(c).cast("string"));
            }
        }
        tempDataset = tempDataset.repartition(10);
        LOGGER.info("preparing to write into Hive");
        if (this.syncMode == HiveCustomTarget.TargetSyncMode.overwrite) {
            this.overwrite(tempDataset);
        }
        else if (this.syncMode == HiveCustomTarget.TargetSyncMode.append) {
            LOGGER.info("Writing to Hive in append mode");
            // Check if hive table exists. If not run in overwrite mode
            try {
                if (!this.checkIfTableExists(this.table)) {
                    this.LOGGER.info("Running in overwrite mode as the table does not exist");
                    this.overwrite(tempDataset);
                } else {
                    if (this.partitionColumns.size() == 0) {
                        tempDataset.write()
                                .format("jdbc")
                                .option("url", this.jdbc_url)
                                .option("dbtable", this.table)
                                .option("driver", this.HIVE_DRIVER_NAME)
                                .mode("append")
                                .save();
                    } else {
                        String internalHiveTable = this.table + "_iwx_cdc_internal";
                        this.createTable(internalHiveTable, true, true, true);
                        tempDataset.write()
                                .format("jdbc")
                                .option("url", this.jdbc_url)
                                .option("dbtable", internalHiveTable)
                                .option("driver", this.HIVE_DRIVER_NAME)
                                .mode("append")
                                .save();
                        String sqlToInsert = getSqlToInsertToExternalHiveTable(this.table, internalHiveTable, "append");
                        this.executeDDLStatement(sqlToInsert);
                        this.dropTableIfExists(internalHiveTable);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            // Merge mode
            try {
                if (!this.checkIfTableExists(this.table)) {
                    this.LOGGER.info("Running in overwrite mode as the table does not exist");
                    this.overwrite(tempDataset);
                } else {
                    LOGGER.info("Writing to Hive in merge mode");
                    String internalHiveTable = this.table + "_iwx_cdc_internal";
                    String mergedHiveTable = this.table + "_merged";
                    this.createTable(internalHiveTable, false, true, true);
                    tempDataset.write()
                            .format("jdbc")
                            .option("url", this.jdbc_url)
                            .option("dbtable", internalHiveTable)
                            .option("driver", this.HIVE_DRIVER_NAME)
                            .mode("append")
                            .save();

                    StringBuilder mergeKeyStatement = new StringBuilder(" ");
                    List<String> mergeKeysTemp = new ArrayList<>();
                    for (String i: this.mergeKeys) {
                        mergeKeysTemp.add(String.format("SRC.%s=TGT.%s",i,i));
                    }
                    mergeKeyStatement.append(String.join("AND",mergeKeysTemp));

                    if (this.externalTablePath!=null)
                    {
                        String mergeStatement = this.getMergeStatement(internalHiveTable, this.table, String.valueOf(mergeKeyStatement), false);
                        this.createTable(mergedHiveTable, false, true, true);
                        this.executeStatement(mergeStatement);
                        String sqlToInsert = getSqlToInsertToExternalHiveTable(this.table, mergedHiveTable, "overwrite");
                        this.executeDDLStatement(sqlToInsert);
                        this.dropTableIfExists(mergedHiveTable);

                    } else {
                        String mergeStatement = this.getMergeStatement(internalHiveTable, this.table, String.valueOf(mergeKeyStatement), true);
                        this.createTable(mergedHiveTable, false, false, false);
                        this.executeStatement(mergeStatement);
                        this.dropTableIfExists(this.table);
                        this.renameTable(mergedHiveTable,this.table);
                    }
                    this.dropTableIfExists(internalHiveTable);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }
        LOGGER.info("successfully written into HIVE");
    }

    private boolean checkIfTableExists(String tableName) throws SQLException {
        String db = tableName.split("[.]", 0)[0];
        String table = tableName.split("[.]", 0)[1];
        DatabaseMetaData table_meta = this.jdbConnection.getMetaData();
        ResultSet rs=table_meta.getTables(null, db, table, null);
        boolean returnValue=false;
        while(rs.next()){
            System.out.println(rs.getString("TABLE_NAME"));
            if (rs.getString("TABLE_NAME").equals(table.toLowerCase())) {
                returnValue = true;
                break;
            }
        }
        return returnValue;
    }

    private String getSqlToInsertToExternalHiveTable(String externalTable, String internalTable, String Mode) {
        String sqlToInsert;
        if (Mode.equals("overwrite")){
            sqlToInsert = String.format("INSERT OVERWRITE TABLE %s ",externalTable);
        } else {
            sqlToInsert = String.format("INSERT INTO TABLE %s ",externalTable);
        }
        // TO-DO handle partition and bucketing columns. Reorder the columns
        if (this.partitionColumns.size() != 0) {
            sqlToInsert = sqlToInsert + " PARTITION("+String.join(",", this.partitionColumns)+") ";
        }
        if (this.partitionColumns.size() != 0) {
            for (String column : this.partitionColumns) {
                this.columnNames.remove(column);
            }
            this.columnNames.addAll(this.partitionColumns);
        }
        List<String> columnsWithTicks = new ArrayList<>();
        for (String column:this.columnNames) {
            if (this.derivedPartitionColumns.containsKey(column))
                columnsWithTicks.add(this.derivedPartitionColumns.get(column));
            else
                columnsWithTicks.add("`" + column + "`");
        }

        sqlToInsert = sqlToInsert + String.format("SELECT %s FROM %s",String.join(",", columnsWithTicks),internalTable);
        return sqlToInsert;
    }

    private String getMergeStatement(String cdcTable, String srcTable, String mergeKeyStatement, Boolean isInternalTableMerge) {
        String tempTable = srcTable + "_merged";
        StringBuilder mergeStatement = new StringBuilder("WITH CTE AS ( SELECT ");
        if (isInternalTableMerge && this.partitionColumns.size() != 0)
        {
            for (String column : this.partitionColumns) {
                this.columnNames.remove(column);
            }
            this.columnNames.addAll(this.partitionColumns);
        }
        List<String> columnsWithTicks = new ArrayList<>();
        for (int i = 0; i < this.columnNames.size(); i++) {
            String column = this.columnNames.get(i);
            columnsWithTicks.add("`" + column + "`");
            if (i == this.columnNames.size()-1) {
                mergeStatement.append(String.format(" CASE WHEN TGT.%s IS NULL THEN SRC.%s ELSE TGT.%s END AS %s ", column, column, column, column));
            } else {
                mergeStatement.append(String.format(" CASE WHEN TGT.%s IS NULL THEN SRC.%s ELSE TGT.%s END AS %s ,", column, column, column, column));
            }
        }
        if (this.derivedPartitionColumns.size()==0){
            mergeStatement.append(String.format(" FROM %s AS SRC FULL OUTER JOIN %s AS TGT ON %s )",srcTable,cdcTable,mergeKeyStatement));
        } else
            mergeStatement.append(String.format(" FROM %s AS SRC FULL OUTER JOIN (SELECT * , %s FROM %s ) AS TGT ON %s )",srcTable,String.join(",",this.derivedPartitionColumns.values()),cdcTable,mergeKeyStatement));

        if (isInternalTableMerge && this.partitionColumns.size() != 0) {
            String sqlToInsert = String.format(" INSERT OVERWRITE TABLE %s ",tempTable);
            sqlToInsert = sqlToInsert + " PARTITION("+String.join(",", this.partitionColumns)+") ";
            sqlToInsert = sqlToInsert + String.format(" SELECT %s FROM %s",String.join(",", columnsWithTicks),"CTE");
            mergeStatement.append(sqlToInsert);
        } else {
            mergeStatement.append(String.format(" INSERT OVERWRITE TABLE %s SELECT * FROM CTE",tempTable));
        }
        return String.valueOf(mergeStatement);
    }

    @Override
    public void initialiseContext(SparkSession sparkSession, UserProperties userProperties, ProcessingContext processingContext) {
        this.spark = sparkSession;
        this.userProperties = userProperties;
        try {
            this.validateAndInitConfigs(userProperties);
            this.jdbConnection = this.getJDBCConnection();
        } catch (Exception var5) {
            var5.printStackTrace();
            Throwables.propagate(var5);
        }
    }

    protected LinkedHashMap<String, String> getHiveColumnsDataTypeMapping(Dataset dataset) {
        LinkedHashMap<String, String> dataframeColumnsDataMapping = new LinkedHashMap();
        LinkedHashMap<String, String> hiveColumnsDataMapping = new LinkedHashMap();
        Arrays.stream(dataset.schema().fields()).forEach((field) -> {
            dataframeColumnsDataMapping.put(field.name(), field.dataType().sql());
        });
        //Map<String, String> spark2HiveDataTypeMapping = this.getSpark2HiveDataTypeMapping();

        String columnName;
        String dataTypeModified;
        List<String> columnNames = new ArrayList<>();
        for(Iterator var5 = dataframeColumnsDataMapping.keySet().iterator(); var5.hasNext(); hiveColumnsDataMapping.put(columnName, dataTypeModified)) {
            columnName = (String)var5.next();
            columnNames.add(columnName);
            String sparkDataType = (String)dataframeColumnsDataMapping.get(columnName);
            dataTypeModified = sparkDataType;
            if (sparkDataType.toLowerCase().startsWith("struct") || sparkDataType.toLowerCase().startsWith("array") || sparkDataType.toLowerCase().startsWith("map")){
                this.arrayStructColumns.add(columnName);
                dataTypeModified = "string";
            }
//            String columnTypeWithoutPrecision = sparkDataType;
//            String columnTypePrecision = "";
//            int precisionIndex = sparkDataType.indexOf("(");
//            if (precisionIndex != -1) {
//                columnTypeWithoutPrecision = sparkDataType.substring(0, precisionIndex);
//                columnTypePrecision = sparkDataType.substring(precisionIndex);
//            }
//            String sqlType = (String)spark2HiveDataTypeMapping.get(columnTypeWithoutPrecision);
//            dataTypeModified = String.format("%s%s", sqlType, columnTypePrecision);
        }
        this.columnNames = columnNames;
        return hiveColumnsDataMapping;
    }

    private void setPartitionAndBucketing() {
        Iterator var4 = this.hiveColumnsDataTypeMapping.keySet().iterator();
        StringBuilder partitionBy = new StringBuilder();
        StringBuilder clusteredBy = new StringBuilder();
        List<String> tempPartitionColumns = new ArrayList<>(partitionColumns);
        while (var4.hasNext()) {
            String columnName = (String) var4.next();
            if (this.partitionColumns.contains(columnName)) {
                //partitionBy
                partitionBy.append(String.format("%s %s,", columnName, this.hiveColumnsDataTypeMapping.get(columnName)));
                tempPartitionColumns.remove(columnName);
            } else if (this.bucketingColumns.contains(columnName)) {
                clusteredBy.append(String.format("%s,",columnName));
            }
        }
        if (tempPartitionColumns.size()!=0) {

            for (String i:tempPartitionColumns)
                partitionBy.append(String.format("%s %s,", i, "String"));
        }

        if (! partitionBy.toString().equals("")) {
            String res = partitionBy.toString();
            res = res.substring(0, res.length()-1);
            this.partitionByClause = String.format(" PARTITIONED BY (%s) ", res);
        }
        if (! clusteredBy.toString().equals("")) {
            String res = clusteredBy.toString();
            res = res.substring(0, res.length()-1);
            this.bucketingClause = String.format(" CLUSTERED BY (%s) INTO %s BUCKETS ", res,this.numberOfBuckets);
        }
    }

    private List<String> getCreateTableColumnsWithDataType(boolean skipDataType) {
        List<String> columnDataTypeList = new ArrayList();
        Iterator var4 = this.hiveColumnsDataTypeMapping.keySet().iterator();
        while (var4.hasNext()) {
            String columnName = (String) var4.next();
            if (skipDataType) {
                columnDataTypeList.add(String.format("`%s` %s", columnName, "string"));
            } else {
                columnDataTypeList.add(String.format("`%s` %s", columnName, this.hiveColumnsDataTypeMapping.get(columnName)));
            }
        }
        return columnDataTypeList;
    }


    private void createTable(String tableName, boolean skipDatatype, boolean createWithoutPartitionBucket, boolean isTempTable) {
        this.dropTableIfExists(tableName);
        List<String> columnsWithDataType = this.getCreateTableColumnsWithDataType(skipDatatype);
        List<String> columnsWithDataType_temp = new ArrayList<>(columnsWithDataType);
        String createTableStatement = null;
        if (!isTempTable) {
            for (String c: columnsWithDataType_temp) {
                String columnName = c.split("\\s+")[0];
                columnName = columnName.replaceAll("`","");
                if (this.partitionColumns.contains(columnName)) {
                    columnsWithDataType.remove(c);
                }
            }
        }
        createTableStatement = String.format("CREATE TABLE %s (%s)", tableName, String.join(",", columnsWithDataType));
        if (createWithoutPartitionBucket) {
            this.LOGGER.info("Skipping adding of partition and bucketing details to the table");
        } else {
            if (this.partitionByClause != null) {
                createTableStatement = createTableStatement + this.partitionByClause;
            }
            if (this.bucketingClause != null) {
                createTableStatement = createTableStatement + this.bucketingClause;
            }
        }
        this.executeDDLStatement(createTableStatement);
    }

    private void createExternalHiveTable(String tableName, String externalTablePath, String fileStorageFormat, Boolean shouldDeleteDataOnDrop) {
        this.dropTableIfExists(tableName);
        List<String> columnsWithDataType  = this.getCreateTableColumnsWithDataType(false);
        List<String> columnsWithDataType_temp = new ArrayList<>(columnsWithDataType);
        String createTableStatement = null;
        for (String c: columnsWithDataType_temp) {
            String columnName = c.split("\\s+")[0];
            columnName = columnName.replaceAll("`","");
            if (this.partitionColumns.contains(columnName)) {
                columnsWithDataType.remove(c);
            }
        }
        createTableStatement = String.format("CREATE EXTERNAL TABLE %s (%s)", tableName, String.join(",", columnsWithDataType));
        if (this.partitionByClause != null) {
            createTableStatement = createTableStatement + this.partitionByClause;
        }
        if (this.bucketingClause != null) {
            createTableStatement = createTableStatement + this.bucketingClause;
        }
        createTableStatement = createTableStatement + String.format(" STORED AS %s LOCATION '%s'",fileStorageFormat, externalTablePath);
        if (shouldDeleteDataOnDrop) {
            createTableStatement = createTableStatement + " TBLPROPERTIES ('external.table.purge'='true') ";
        }
        this.executeDDLStatement(createTableStatement);
    }

    private void renameTable(String oldTable, String newTable) {
        String renameTableStatement = String.format("ALTER TABLE %s RENAME TO %s",oldTable,newTable);
        this.executeDDLStatement(renameTableStatement);
    }

    private void enablePurgeExternalTable(String tableName) {
        try {
            String renameTableStatement = String.format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='False') ", tableName);
            this.executeDDLStatement(renameTableStatement);
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    private void dropTableIfExists(String tableName) {
        try {
            String dropTableQuery = String.format("DROP TABLE %s ", tableName);
            this.executeDDLStatement(dropTableQuery);
        } catch (Exception exp)
        {
            exp.printStackTrace();
        }
    }

    private void createSchemaIfNotExists(String schemaName)
    {
        try {
            String query = String.format("CREATE SCHEMA IF NOT EXISTS %s ", schemaName);
            this.executeDDLStatement(query);
        } catch (Exception exp)
        {
            exp.printStackTrace();
        }
    }

    private void executeDDLStatement(String sqlStatement) {
        try {
            this.LOGGER.info("Executing query : \n " + sqlStatement);
            Statement statement = this.jdbConnection.createStatement();
            statement.executeUpdate(sqlStatement);
        } catch (Exception var3) {
            var3.printStackTrace();
            Throwables.propagate(var3);
        }

    }

    private void executeStatement(String sqlStatement) {
        try {
            this.LOGGER.info("Executing query : \n " + sqlStatement);
            Statement statement = this.jdbConnection.createStatement();
            statement.execute(sqlStatement);
        } catch (Exception var3) {
            var3.printStackTrace();
            Throwables.propagate(var3);
        }

    }

    @Override
    public void close() throws Exception {
        if (this.jdbConnection != null) {
            this.jdbConnection.close();
        }
    }

    public static enum TargetSyncMode {
        overwrite,
        append,
        merge;

        private TargetSyncMode() {
        }
    }


    public static void main(String[] args) {
        HiveCustomTarget cstmtgt = new HiveCustomTarget();
        SparkSession sparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate();
        Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("/Users/abhishek.raviprasad/Downloads/hiveTargetFiles/full.csv");
        //Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("/Users/abhishek.raviprasad/Downloads/hiveTargetFiles/append.csv");
        //Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("/Users/abhishek.raviprasad/Downloads/hiveTargetFiles/merge.csv");
        dataset = dataset.withColumn("insert_ts",functions.to_timestamp(functions.col("insert_ts"),"yyyy-MM-dd HH:mm:ss"));
        dataset = dataset.withColumn("temp_date",functions.to_date(functions.col("temp_date"),"yyyy-MM-dd"));
        dataset = dataset.withColumn("salary",functions.col("salary").cast(DataTypes.createDecimalType(20,7)));
        dataset = dataset.withColumn("isActive",functions.col("isActive").cast(DataTypes.BooleanType));

        // Dataset dataset = sparkSession.read().orc("/Users/abhishek.raviprasad/Downloads/orc-file-11-format.orc");
        // Dataset dataset = sparkSession.read().option("header", "true").option("inferschema", "true").csv("/Users/abhishek.raviprasad/Downloads/Customer_CDC.csv");
        dataset.show();
        Map<String, String> userProps = new HashMap<>();
        String ServerConfigurations = "hive.exec.dynamic.partition=true,hive.exec.dynamic.partition.mode=nonstrict,hive.enforce.bucketing=true";
        String cdata_hive_url = "jdbc:apachehive:Server=10.18.1.23;port=10000;User=;Password=;Database=default;Other=QueryPassthrough=True;ServerConfigurations="+ServerConfigurations;
        userProps.put("jdbcUrl", cdata_hive_url);
        userProps.put("table", "ar_demo.customer_rtk");
        //userProps.put("externalTablePath", "/demo_poc/customer_testCase_extDerivedPartition");
        userProps.put("syncMode", "overwrite");
        userProps.put("numOfRePartitions", "10");
        //userProps.put("partitionColumns","country");
        //userProps.put("derivedPartitionColumns", "year(temp_date):year_date");
        //userProps.put("mergeKeys", "id");
        //userProps.put("bucketingColumns", "country,state");
        //userProps.put("numberOfBuckets", "3");
        UserProperties userProperties = new UserProperties(userProps);
        cstmtgt.initialiseContext(sparkSession, userProperties, (ProcessingContext)null);
        cstmtgt.writeToTarget(dataset);
//        Dataset jdfcDf = sparkSession.read().format("jdbc")
//                .option("url", "jdbc:apachehive:Server=10.18.1.23;port=10000;User=;Password=;Database=default;")
//                .option("dbtable", "default.employee")
//                .option("user", "")
//                .option("password", "")
//                //.option("driver", "org.apache.hive.jdbc.HiveDriver")
//                .option("driver", "cdata.jdbc.apachehive.ApacheHiveDriver")
//                .load();
//        jdfcDf.show();
    }
}

