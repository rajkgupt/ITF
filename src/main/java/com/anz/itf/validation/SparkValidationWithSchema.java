package com.anz.itf.validation;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.anz.itf.utils.ParseOptions;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;


public final class SparkValidationWithSchema {


    public static void main(String[] args)  {

        Options options = ParseOptions.OPTIONS;




        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("JavaWordCount")
                .getOrCreate();

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        StructType schema = new StructType(new StructField[]{
                new StructField("State/Territory"     ,DataTypes.StringType ,false, Metadata.empty()),
                new StructField("Capital"   ,DataTypes.StringType  ,false, Metadata.empty()),
                new StructField("City Population"     ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("State/Territory Population"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Percentage"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Established"   ,DataTypes.DateType  ,false, Metadata.empty()),
                new StructField("_corrupt_record"   ,DataTypes.StringType  ,false, Metadata.empty())
        });

        Dataset<Row> inputFileDF  = spark.read().format("csv")
                .option("header", "true")
                .schema(schema)

                //corrupt record - all null - schema validation does not handle
                .load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals-dupes.csv");

                //corrupt record - all not null as entire col is extra, plus invalid date in one rec
                //.load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals-addition.csv");

                //3 corrupt record - only catching seven in pct col. Pending capital col with null value & invalid month parsing
                //.load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals-invalid-3.csv");

                //corrupt record - all not null as entire col is missing
                //.load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals-missing.csv");

                //corrupt record - all null
                //.load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals.csv");

        inputFileDF.registerTempTable("ausInputFileTable");

        //Query for primarky key violation check
            long primaryKeyCount = spark.sql("WITH " +
                    "t AS (select `State/Territory`,count(*) as counts from ausInputFileTable group by `State/Territory` having count(*) > 1)," +
            " t2 AS ( SELECT count(*) FROM t where counts <> 1 )" +
            "SELECT * FROM t2").count();

            if (primaryKeyCount != 0) {
                System.out.println("Primary Key Count violated");
                System.exit(1);
            }

        //Query for duplicate record violation check
        long dupesRecordCountCheck = spark.sql("WITH " +
                "t AS (select `State/Territory`,Capital,City Population,`State/Territory Population`,Percentage,Established," +
                "count(*) as counts from ausInputFileTable " +
                "group by `State/Territory`,Capital,City Population,`State/Territory Population`,Percentage,Established " +
                "having count(*) > 1)," +
                " t2 AS ( SELECT count(*) FROM t where counts <> 1 )" +
                "SELECT * FROM t2").count();

        if (dupesRecordCountCheck != 0) {
            System.out.println("Duplicate Record Count violated");
            System.exit(1);
        }


        inputFileDF.show();






        spark.stop();
    }
}
