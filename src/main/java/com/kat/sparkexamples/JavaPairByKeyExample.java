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

public class JavaPairByKeyExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("JavaPair By Key App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runJavaPairReduceByKeyExample(sc);
        runJavaPairGroupByKeyExample(sc);
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
