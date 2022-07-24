package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class ReduceExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Reduce Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runReduceExample(sc);
    }

    private static void runReduceExample(JavaSparkContext sc) {
        List<Double> doubles = new ArrayList<>();
        doubles.add(32.1);
        doubles.add(30.3);
        doubles.add(31.5);
        doubles.add(36.7);
        doubles.add(29.4);

        JavaRDD<Double> rdd = sc.parallelize(doubles);
        double result = rdd.reduce((a, b) -> a + b);
        System.out.println(format("Sum of doubles using Reduce:%s\n", result));
    }
}
