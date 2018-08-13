package com.hh.demospark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountSpark {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount"));

        LongAccumulator accumLines = sc.sc().longAccumulator();
        LongAccumulator accumWords = sc.sc().longAccumulator();
        JavaRDD<String> textFile = sc.textFile("hdfs://localhost:9000/user/abc/demohadoop.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> {
                    accumLines.add(1L);
                    return Arrays.asList(s.split(" ")).iterator();})
                .mapToPair(word -> {
                    accumWords.add(1l);
                    return new Tuple2<>(word, 1);})
                .reduceByKey((a, b) -> a + b);
        System.out.println(counts.collect());
        System.out.println("Number of lines : "+accumLines);
        System.out.println("Number of sword : "+accumWords);
    }
}
