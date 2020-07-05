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

public class RandomForestRegressorInPipelineApp {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("RandomForestRegressorApp")
      .master("local[*]")
      .getOrCreate();

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> df = spark.read().format("libsvm")
        //.load("data/mllib/sample_libsvm_data.txt");
        .load("data/sample-ml/simplegauss.txt");
    df.show(20, false);

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    VectorIndexerModel featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(df);

    // Split the data into training and test sets (30% held out for testing)
    Dataset<Row>[] splits = df.randomSplit(new double[] {1, 0});
//    Dataset<Row>[] splits = df.randomSplit(new double[] {0.7, 0.3});
    Dataset<Row> trainingData = df;//splits[0];
    //Dataset<Row> testData = splits[1];

    Dataset<Row> testData = spark.read().format("libsvm")
        //.load("data/mllib/sample_libsvm_data.txt");
        .load("data/sample-ml/simplegauss_test.txt");
    
    // Train a RandomForest model.
    RandomForestRegressor rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures");

    // Chain indexer and forest in a Pipeline
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {featureIndexer, rf});

    // Train model. This also runs the indexer.
    PipelineModel model = pipeline.fit(trainingData);
    
    // Make predictions.
    Dataset<Row> predictions = model.transform(testData);

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5);

    // Select (prediction, true label) and compute test error
    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictions);
    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
    System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
    // $example off$

    Double feature = 2.0;
    Vector features = Vectors.dense(feature);
    double p = rfModel.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p+ " (expected: 2)");

    feature = 11.0;
    features = Vectors.dense(feature);
    p = rfModel.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p + " (expected: 2)");


    spark.stop();
  }
}
