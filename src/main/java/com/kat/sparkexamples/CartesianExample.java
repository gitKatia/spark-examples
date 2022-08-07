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

public class CartesianExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Filter Example App").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        runCartesianExample(sc);
    }

    private static void runCartesianExample(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> comments = comments();
        List<Tuple2<Integer, String>> users = users();

        JavaPairRDD<Integer, Integer> commentsRDD = sc.parallelizePairs(comments);
        JavaPairRDD<Integer, String> usersRDD = sc.parallelizePairs(users);

        //Cartesian: Contains all the combinations
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Integer>> userCommentsRDD = usersRDD.cartesian(commentsRDD);
        // Multi-threaded way:Ordering is not predictable
        userCommentsRDD.foreach(rdd -> System.out.println(rdd));
    }
}
