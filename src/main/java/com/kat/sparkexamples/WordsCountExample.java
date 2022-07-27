package com.kat.sparkexamples;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordsCountExample {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Count Words Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runCountWords(sc);
    }

    private static void runCountWords(JavaSparkContext sc) {
        JavaRDD<String> inputDataRdd = sc.textFile("src/main/resources/countWordsData.txt");
        JavaRDD<String> wordsRdd = inputDataRdd.map(line -> line.replaceAll("[^a-zA-Z(\\s){1}]", "").toLowerCase())
                .filter(line -> StringUtils.isNotBlank(line))
                .map(line -> line.replaceAll(",|!|\\.", " "))
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        List<String> wordsList = wordsRdd
                .take(30);
        wordsList.forEach(System.out::println);
        System.out.println();

        JavaPairRDD<String, Long> countsPairRdd = wordsRdd
                .filter(word -> !Util.isToThrowAway(word))
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(tuple -> new Tuple2(tuple._2, tuple._1))
                // not in ascending order
                .sortByKey(false);

        List<Tuple2<String, Long>> wordsCountList = countsPairRdd.take(50);
        wordsCountList.forEach(System.out::println);
    }
}
