package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class MapExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Map Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runMapExample(sc);
    }

    private static void runMapExample(JavaSparkContext sc) {
        List<Integer> integers = new ArrayList<>();
        integers.add(2);
        integers.add(3);
        integers.add(1);
        integers.add(6);
        integers.add(9);
        integers.add(-1);

        JavaRDD<Integer> rdd = sc.parallelize(integers);
        rdd.foreach(value -> System.out.println(format("Value in Original RDD:%s", value)));
        System.out.println();
        JavaRDD<Double> resultRdd = rdd.map(Math::sqrt);
        resultRdd.foreach(value -> System.out.println(format("Result of Square root of integers using Map: %s", value)));
        System.out.println();
        System.out.println(format("Number of items in the resulting RDD:%s\n", resultRdd.count()));
        long count = resultRdd.map(value -> 1)
                .reduce((a, b) -> a + b);
        System.out.println(format("Number of items in the resulting RDD using Map-Reduce:%s\n", count));
    }
}
