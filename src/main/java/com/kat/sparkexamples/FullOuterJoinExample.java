package com.kat.sparkexamples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.List;

import static com.kat.sparkexamples.Util.comments;
import static com.kat.sparkexamples.Util.users;

public class FullOuterJoinExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Filter Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runFullOuterJoinExample(sc);
    }

    private static void runFullOuterJoinExample(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> comments = comments();
        List<Tuple2<Integer, String>> users = users();

        JavaPairRDD<Integer, Integer> commentsRDD = sc.parallelizePairs(comments);
        JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);

        // Full Outer Join: Contains both Left and Right Keys
        JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> userCommentsRDD = usersRDD.fullOuterJoin(commentsRDD);
        // Multi-threaded way:Ordering is not predictable
        userCommentsRDD.foreach(rdd -> System.out.println(rdd));
    }
}
