package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(10.0);
        inputData.add(20.0);
        inputData.add(10.0);
        inputData.add(20.0);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-1")
                .setMaster("local[*]");

        // context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // javaRDD using doubles
        JavaRDD<Double> javaRDD = javaSparkContext.parallelize(inputData);

        // reduce: add values
        Double result = javaRDD.reduce((value1, value2) -> value1 + value2);

        // print the result
        System.out.println("result = " + result);

        // close context
        javaSparkContext.close();
    }
}