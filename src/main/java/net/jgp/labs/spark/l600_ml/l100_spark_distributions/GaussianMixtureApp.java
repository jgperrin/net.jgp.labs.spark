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

import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An example demonstrating Gaussian Mixture Model. Run with
 * 
 */
public class GaussianMixtureApp {

  public static void main(String[] args) {

    // Creates a SparkSession
    SparkSession spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("GaussianMixtureApp")
        .getOrCreate();

    // Loads data
    Dataset<Row> df = spark.read()
        .format("libsvm")
//        .load("data/mllib/sample_kmeans_data.txt");
        .load("data/sample-ml/simplegauss.txt");    df.show(10, false);

    // Trains a GaussianMixture model
    GaussianMixture gmm = new GaussianMixture().setK(1);
    GaussianMixtureModel model = gmm.fit(df);

    // Output the parameters of the mixture model
    for (int i = 0; i < model.getK(); i++) {
      System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
          i, model.weights()[i], model.gaussians()[i].mean(),
          model.gaussians()[i].cov());
    }
    
    Double feature = 2.0;
    Vector features = Vectors.dense(feature);
    double p = model.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p);

    feature = 11.0;
    features = Vectors.dense(feature);
    p = model.predict(features);
    System.out.println("Prediction for feature " + feature + " is " + p);

    spark.stop();
  }
}
