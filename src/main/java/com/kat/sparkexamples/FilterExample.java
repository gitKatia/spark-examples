package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class FilterExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Filter Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runFilterExample(sc);
    }

    private static void runFilterExample(JavaSparkContext sc) {

        sc.textFile("src/main/resources/sentences.txt")
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(item -> item.length() > 1)
                .collect()
                .forEach(System.out::println);
    }
}
