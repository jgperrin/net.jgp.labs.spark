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

package net.jgp.labs.spark.l600_ml.l100_spark_distributions;

// $example on$
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.PolynomialExpansion;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

public class PolynomialExpansionApp {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("RandomForestRegressorApp")
      .master("local[*]")
      .getOrCreate();

    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> df = spark.read().format("libsvm")
        .load("data/sample-ml/simplegauss.txt");
    df.show(20, false);
    
    PolynomialExpansion polyExpansion = new PolynomialExpansion()
        .setInputCol("features")
        .setOutputCol("polyFeatures")
        .setDegree(3);
    Dataset<Row> polyDF = polyExpansion.transform(df);
    polyDF.show(false);

    
   

   



    spark.stop();
  }
}
