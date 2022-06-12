package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.*;

public class MainMap {

    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(10);
        inputData.add(20);
        inputData.add(10);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-map")
                .setMaster("local[*]");

        // context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        // javaRDD using doubles
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(inputData);

        // reduce: add values
        Integer result = javaRDD.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = javaRDD.map( value -> sqrt(value) );

        //sqrtRdd.foreach( value -> System.out.println(value)); // print each value
        sqrtRdd.collect().forEach(System.out::println); // print each value - gets exception!!

        // print the result
        System.out.println("result = " + result);

        // close context
        javaSparkContext.close();
    }
}