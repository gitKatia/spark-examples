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
import static java.lang.String.format;

public class LeftOuterJoinExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Filter Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runLeftOuterJoinExample(sc);
    }

    private static void runLeftOuterJoinExample(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> comments = comments();
        List<Tuple2<Integer, String>> users = users();

        JavaPairRDD<Integer, Integer> commentsRDD = sc.parallelizePairs(comments);
        JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);

        // Left Outer Join: Contains Left Keys only
        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> userCommentsPairRDD = usersRDD.leftOuterJoin(commentsRDD);
        // Multi-threaded way:Ordering is not predictable
        userCommentsPairRDD.foreach(pairRdd -> System.out.println(format("(UserId=%s, UserName=%s, UserComments=%s)", pairRdd._1,
                pairRdd._2._1, pairRdd._2._2.orElse(0))));
    }
}
