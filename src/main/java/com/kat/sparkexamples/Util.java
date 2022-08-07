package com.kat.sparkexamples;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Util {

	public static Set<String> throwAway = new HashSet<String>();
	
	static {
		InputStream is = Util.class.getResourceAsStream("/throwAway.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		br.lines().forEach(throwAway::add);
	}

	public static boolean isToThrowAway(String word)
	{
		return throwAway.contains(word);
	}

	public static List<Tuple2<Integer, Integer>> comments() {
		List<Tuple2<Integer, Integer>> comments = new ArrayList<>();
		comments.add(new Tuple2<>(5, 10));
		comments.add(new Tuple2<>(8, 9));
		comments.add(new Tuple2<>(15, 4));
		comments.add(new Tuple2<>(4, 3));
		return comments;
	}

	public static List<Tuple2<Integer, String>> users() {
		List<Tuple2<Integer, String>> users = new ArrayList<>();
		users.add(new Tuple2<>(1, "Sam"));
		users.add(new Tuple2<>(2, "Robert"));
		users.add(new Tuple2<>(3, "Luke"));
		users.add(new Tuple2<>(4, "Mary"));
		users.add(new Tuple2<>(5, "Dan"));
		users.add(new Tuple2<>(6, "William"));
		users.add(new Tuple2<>(7, "Kate"));
		users.add(new Tuple2<>(8, "Tom"));
		return users;
	}
}
