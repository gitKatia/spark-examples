package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Filter Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runJoinExample(sc);
    }

    private static void runJoinExample(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> comments = new ArrayList<>();
        comments.add(new Tuple2<>(5, 10));
        comments.add(new Tuple2<>(8, 9));
        comments.add(new Tuple2<>(15, 4));
        comments.add(new Tuple2<>(4, 3));

        List<Tuple2<Integer, String>> users = new ArrayList<>();
        users.add(new Tuple2<>(1, "Sam"));
        users.add(new Tuple2<>(2, "Robert"));
        users.add(new Tuple2<>(3, "Luke"));
        users.add(new Tuple2<>(4, "Mary"));
        users.add(new Tuple2<>(5, "Dan"));
        users.add(new Tuple2<>(6, "William"));
        users.add(new Tuple2<>(7, "Kate"));
        users.add(new Tuple2<>(8, "Tom"));

        JavaPairRDD<Integer, Integer> commentsRDD = sc.parallelizePairs(comments);
        JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);

        // Inner Join
        JavaPairRDD<Integer, Tuple2<String, Integer>> userCommentsRDD = usersRDD.join(commentsRDD);
        // Multi-threaded way:Ordering is not predictable
        userCommentsRDD.foreach(rdd -> System.out.println(rdd));
    }
}
