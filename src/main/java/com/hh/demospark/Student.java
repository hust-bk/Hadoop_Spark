package com.hh.demospark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Student {

    public static void main(String[] args) {
//        createDataHdfs();

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount").setMaster("local"));

        JavaPairRDD<String, Tuple2<Integer, Float>> file1 = sc.textFile("hdfs://localhost:9000/user/abc/file1.txt")
                .mapToPair(line -> {
                    String[] data = line.split("    ");
                    return new Tuple2<>(data[0]+"    "+data[2], new Tuple2<>(typeCheck(data[3]),Float.parseFloat(data[1])));})
                .reduceByKey((a, b) -> {
                    return new Tuple2<>(a._1+b._1,(a._2*a._1 + b._2*b._1)/(a._1+b._1));
                });

        for (Map.Entry<String, Tuple2<Integer, Float>> entry : file1.collectAsMap().entrySet()) {
            System.out.println(entry.getKey() + "    " + entry.getValue()._2);
        }

        // The schema is encoded in a string
        String schemaString = "name subject point";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        SparkSession spark = SparkSession.builder().getOrCreate();

        JavaRDD<Row> rdd = file1.map(new Function<Tuple2<String, Tuple2<Integer, Float>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Float>> data) throws Exception {
                String[] attributes = data._1.split("    ");
                return RowFactory.create(attributes[0],attributes[1],data._2._2.toString());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> write = spark.createDataFrame(rdd, schema);
        write.write().parquet("hdfs://localhost:9000/user/abc/fileparquet.parquet");
        Dataset<Row> read = spark.read().parquet("hdfs://localhost:9000/user/abc/fileparquet.parquet");
        read.show();
    }

    private static int typeCheck(String s){
        switch (s){
            case "15phut":
                return 1;
            case "1tiet":
                return 2;
            case "cuoiky":
                return 5;
        }
        return 0;
    }

    private static void createDataHdfs(){
        Configuration configuration = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get( new URI( "hdfs://localhost:9000" ), configuration );
            Path file = new Path("hdfs://localhost:9000/user/abc/file1.txt");
            if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
            OutputStream os = hdfs.create( file);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            Random rd = new Random();
            Set<Integer> set = new HashSet<>();
            while(set.size() < 10000){
                set.add(rd.nextInt(20000)+1);
            }
            String[] subject = {"Math","Chemistry","Physics","History"};
            String[] typeCheck = {"15phut","1tiet","cuoiky"};

            for(Integer i : set){
                String name = Long.toString(Math.abs(rd.nextLong() % 3656158440062976L), 36);
                for(int j = 0; j < rd.nextInt(4)+1; j++){
                    br.write(name+" - "+i);
                    br.write("    "+(rd.nextInt(10)+1)+"    "+subject[rd.nextInt(4)]+"    "+typeCheck[rd.nextInt(3)]);
                    br.write("\n");
                }
            }
            br.close();
            hdfs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
