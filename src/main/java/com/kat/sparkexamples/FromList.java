package com.kat.sparkexamples;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class FromList {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Load List").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runReduceExample(sc);
        runMapExample(sc);
        runJavaPairReduceByKeyExample(sc);
        runJavaPairGroupByKeyExample(sc);
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

    // Not advised
    private static void runJavaPairGroupByKeyExample(JavaSparkContext sc) {
        List<String> logs = new ArrayList<>();
        logs.add("WARN: 24 September 2022");
        logs.add("ERROR: 21 October 2019");
        logs.add("WARN: 24 March 2022");
        logs.add("FATAL: 23 May 2018");
        logs.add("WARN: 24 September 2021");
        logs.add("ERROR: 12 June 2022");

        JavaRDD<String> rdd = sc.parallelize(logs);
        rdd.foreach(value -> System.out.println(format("Value in Original RDD:%s", value)));
        System.out.println();
        JavaPairRDD<String, String> resultRdd = rdd.mapToPair(rawValue -> {
            String[] pair = rawValue.split(":");
            return new Tuple2<>(pair[0], pair[1]);
        });
        JavaPairRDD<String, Iterable<String>> groupedByKey = resultRdd.groupByKey();

        groupedByKey.map(tuple -> new Tuple2(tuple._1, Iterables.size(tuple._2)))
                .foreach(tuple -> System.out.println(format("Result of of Log processing [level=%s, occurrences=%s] using Group By Key", tuple._1, tuple._2)));
        System.out.println();
    }

    private static void runJavaPairReduceByKeyExample(JavaSparkContext sc) {
        List<String> logs = new ArrayList<>();
        logs.add("WARN: 24 September 2022");
        logs.add("ERROR: 21 October 2019");
        logs.add("WARN: 24 March 2022");
        logs.add("FATAL: 23 May 2018");
        logs.add("WARN: 24 September 2021");
        logs.add("ERROR: 12 June 2022");

        JavaRDD<String> rdd = sc.parallelize(logs);
        rdd.foreach(value -> System.out.println(format("Value in Original RDD:%s", value)));
        System.out.println();
        JavaPairRDD<String, Long> resultRdd = rdd.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .reduceByKey((value1, value2) -> value1 + value2);
        resultRdd.foreach(tuple -> System.out.println(format("Result of Log Processing:[level=%s, occurrences=%s] using Reduce by Key.",
                tuple._1, tuple._2)));
        System.out.println();
        System.out.println(format("Number of items in the resulting RDD:%s\n", resultRdd.count()));
    }
}
