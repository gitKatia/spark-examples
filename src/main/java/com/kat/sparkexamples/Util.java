package com.kat.sparkexamples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
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
}
