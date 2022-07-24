package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("FlatMap App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runFlatMapExample(sc);
    }

    private static void runFlatMapExample(JavaSparkContext sc) {
        List<String> sentences = new ArrayList<>();
        sentences.add("How are you doing");
        sentences.add("That is great");
        sentences.add("Is it going to be tomorrow");
        sentences.add("Could you tell me more");
        sentences.add("It is impressive");
        sentences.add("I cannot find out the reason for that");
        sentences.add("Time flies when you are having fun");
        sentences.add("It is a work of art");

        JavaRDD<String> sentenceItem = sc.parallelize(sentences)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        sentenceItem.collect().forEach(System.out::println);

    }
}
