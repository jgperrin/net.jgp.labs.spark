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

package net.jgp.labs.spark.l600_ml.l300_gradient_boosted_tree;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Inspired by Apache Spark's example.
 * 
 * @author jgp
 */
public class GradientBoostedTreeRegressorApp {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("GradientBoostedTreeRegressorApp")
      .master("local[*]")
      .getOrCreate();

    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> df = spark.read().format("libsvm").load("data/sample-ml/simplegauss.txt");

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    VectorIndexerModel featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(df);

    // Train a GBT model
    GBTRegressor gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      ;

    // Train model
    GBTRegressionModel model = gbt.fit(df);

    // Make predictions
    Dataset<Row> predictions = model.transform(df);

    // Select example rows to display
    predictions.show(20, false);

    // Select (prediction, true label) and compute test error.
    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictions);
    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    System.out.println("Learned regression GBT model:\n" + model.toDebugString());

    Double feature = 2.0;
    Vector features = Vectors.dense(feature);
    double p = model.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p +
        " (expected: 2)");

    feature = 11.0;
    features = Vectors.dense(feature);
    p = model.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p +
        " (expected: 2)");
    
    spark.stop();
  }
}
