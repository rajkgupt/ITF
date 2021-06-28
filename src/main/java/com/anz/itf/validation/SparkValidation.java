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

import java.util.HashMap;
import java.util.regex.Pattern;


public final class SparkValidation {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        Options options = ParseOptions.OPTIONS;
        HashMap<String, String > inputArgsMap = null;

        if (args.length < 4) {
            ParseOptions.printHelp(options);
            System.exit(1);
        } else {
            inputArgsMap = ParseOptions.parseOptions(options, args);
        }

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("JavaWordCount")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("sep",",")
                .option("inferSchema","true")
                .option("header","true")
                .load("cars.csv");

        String schema = inputArgsMap.get("schema");
        String data = inputArgsMap.get("data");
        String tag = inputArgsMap.get("tag");
        String output = inputArgsMap.get("output");







        //df.write().mode(SaveMode.Overwrite).csv("newcars.csv");


        spark.stop();
    }
}
