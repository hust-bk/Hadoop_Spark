package com.hh.demospark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class K_means {

    public static void main(String[] args) {

        Double[][] centroid = {{2.0, 2.0},
                                {8.0, 3.0 },
                                {3.0, 6.0}};
        List<Tuple2<Double, Double>> list1 = new ArrayList<>();
        List<Tuple2<Double, Double>> list2 = new ArrayList<>();
        List<Tuple2<Double, Double>> list3 = new ArrayList<>();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("K-Means"));

        // Load and parse data
        JavaRDD<Tuple2<Double, Double>> data = sc.textFile("hdfs://localhost:9000/user/abc/testkmeans.txt").flatMap(s -> {
            String[] s1 =s.split(" ");
            return Arrays.asList(new Tuple2<>(Double.parseDouble(s1[0]), Double.parseDouble(s1[1]))).iterator();
        });

        while (true) {
            JavaPairRDD<String, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> parsedData = data.mapToPair(new PairFunction<Tuple2<Double, Double>, String, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>>() {
                @Override
                public Tuple2<String, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> call(Tuple2<Double, Double> s) throws Exception {
                    int label = label(s, centroid);
                    switch (label){
                        case 1:
                            list1.add(s);
                            return new Tuple2<>(label + "", new Tuple3<>(1, s, list1));
                        case 2:
                            list2.add(s);
                            return new Tuple2<>(label + "", new Tuple3<>(1, s, list2));
                        case 3:
                            list3.add(s);
                            return new Tuple2<>(label + "", new Tuple3<>(1, s, list3));
                        default:
                            return null;
                    }
                }
            }).reduceByKey(new Function2<Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>>() {
                @Override
                public Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>> call(Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>> a, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>> b) throws Exception {
                    return new Tuple3<>(a._1()+b._1(), new Tuple2<>((a._1()*a._2()._1 + b._1()*b._2()._1)/(a._1()+b._1()),
                                            (a._1()*a._2()._2 + b._1()*b._2()._2)/(a._1()+b._1())), a._3());
                }
            });

            System.out.println(parsedData.collect());
            for (Map.Entry<String, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> entry : parsedData.collectAsMap().entrySet()) {
                System.out.println("Cluster "+entry.getKey() + ":    " + entry.getValue());
            }

            Map<String, Tuple3<Integer, Tuple2<Double, Double>, List<Tuple2<Double, Double>>>> map = parsedData.collectAsMap();
            if ((map.get("1")._2()._1 - centroid[0][0])==0 && (map.get("1")._2()._2 - centroid[0][1])==0 &&
                    (map.get("2")._2()._1 - centroid[1][0])==0 && (map.get("2")._2()._2 - centroid[1][1]==0) &&
                    (map.get("3")._2()._1 - centroid[2][0])==0 && (map.get("3")._2()._2 - centroid[2][1])==0){
                break;
            }else {
                centroid[0][0] = map.get("1")._2()._1;
                centroid[0][1] = map.get("1")._2()._2;
                centroid[1][0] = map.get("2")._2()._1;
                centroid[1][1] = map.get("2")._2()._2;
                centroid[2][0] = map.get("3")._2()._1;
                centroid[2][1] = map.get("3")._2()._2;
            }
        }

        sc.stop();
        sc.close();

    }

    private static int label(Tuple2<Double, Double> d, Double[][] centroid){
        Double a = Math.sqrt(Math.pow(centroid[0][0] - d._1, 2) + Math.pow(centroid[0][1] - d._2, 2));
        Double b = Math.sqrt(Math.pow(centroid[1][0] - d._1, 2) + Math.pow(centroid[1][1] - d._2, 2));
        Double c = Math.sqrt(Math.pow(centroid[2][0] - d._1, 2) + Math.pow(centroid[2][1] - d._2, 2));
        if (a <= b && a <= c)
            return 1;
        if (b <= a && b <= c)
            return 2;
        if (c <= a && c <= b)
            return 3;
        return 0;
    }


}
