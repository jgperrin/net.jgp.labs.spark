package net.jgp.labs.spark.l600_ml.l100_spark_distributions;

import java.util.HashMap;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
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

import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel;
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier;

import static org.apache.spark.sql.functions.*;

public class XgboostApp {
  public static void main(String[] args) {

    XgboostApp app = new XgboostApp();
    dataFile = "data/sample-ml/simplegauss.txt";
//    app.startSpark();
    try {
      app.startNative();
    } catch (XGBoostError e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static String dataFile;

  private void startNative() throws XGBoostError {
     DMatrix trainMat = new DMatrix(dataFile);

    //set params
    HashMap<String, Object> params = new HashMap<String, Object>();

    params.put("eta", 1.0);
    params.put("max_depth", 3);
    params.put("silent", 1);
    params.put("nthread", 6);
    //params.put("objective", "binary:logistic");
    params.put("gamma", 1.0);
    params.put("eval_metric", "error");

    //do 5-fold cross validation
    int round = 2;
    int nfold = 5;
    //set additional eval_metrics
    String[] metrics = null;

    String[] evalHist = XGBoost.crossValidation(trainMat, params, round, nfold, metrics, null,
            null);
    for (String s: evalHist) {
      System.out.println(s);
     
    }
  }
  

  private void startSpark() {
    SparkSession spark = SparkSession
        .builder()
        .appName("XGBoostClassifier")
        .master("local[*]")
        .getOrCreate();

    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> df = spark.read().format("libsvm")
        .load(dataFile);
    df.show(20, false);

    XGBoostClassifier booster = new XGBoostClassifier()
        .setSilent(1)
        .setNumWorkers(2)
        .setNumRound(100)
        .setMaxDepth(6)
        .setMissing(0f)
        // .setObjective("survival:cox")
        // .setEta(0.1f)

        .setLabelCol("label")
        .setFeaturesCol("features");
    // .setNumClass(1)
    // .setEta(1.0)
    // .setPredictionCol("prediction2")
    // .setRawPredictionCol("prediction")
    // .setObjective("binary:logistic");
    // .setObjective("multi:softprob");

    XGBoostClassificationModel model = booster.fit(df);
    Dataset<Row> tDf = model.transform(df);
    tDf.printSchema();
    // tDf = tDf.withColumn("pred2",
    // col("rawPrediction.values").getItem(0));
    tDf.show(20, false);

    MulticlassClassificationEvaluator evaluator =
        new MulticlassClassificationEvaluator();
    evaluator.setLabelCol("label");
    evaluator.setPredictionCol("prediction");
    double accuracy = evaluator.evaluate(tDf);
    System.out.println("The model accuracy is : " + accuracy);

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

    Vector v = (Vector) df.head().getAs("features");
    double r = model.predict(v);
    System.out.println("Prediction for feature " + v + " is " + r +
        " (expected: 2)");

    spark.stop();
  }
}
