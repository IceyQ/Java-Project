package com.ncut.bus.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.ncut.bus.util.Config;

import scala.Tuple2;

import java.io.IOException;


public class Hdfs2Hbase_Wrong {
    public static void main(String []args) throws IOException {
    	// 初始化Spark: create SparkContext
//        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BusSpark");
    	SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BusSpark");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
       
        // 获取hdfs上的文件路径
        String fsPath = Config.getFSPath();
        System.out.println(fsPath);
       
        // 通过javaSparkContext对象来创建RDD
        JavaPairRDD<String, String> inputRDD = javaSparkContext.wholeTextFiles(fsPath);  
        
        
        JavaRDD<Put> HBaseRDD = inputRDD.map(new Function<Tuple2<String, String>, Put>() {
            @Override
            public Put call(Tuple2<String, String> ss) throws Exception {
                String path =  ss._1.toString();
                String date = path.substring(path.length()-12, path.length()-4);
                //System.out.println(date);
                
                String[] strs = ss._2.split("\n");
                Put put = null;
                
                for (String lines:strs) {
                	String[] line = lines.split(",");
                	put = new Put(Bytes.toBytes(date + "_" + line[1] + "_" + line[2]));
                    put.addColumn("f1".getBytes(), line[3].getBytes(), lines.getBytes());
                }
                return put;
            }
        });
        

        // ========================== Write out to HBase ==============================
        Configuration outConf = HBaseConfiguration.create();
        JavaHBaseContext hbaseContext3 = new JavaHBaseContext(javaSparkContext, outConf);
        TableName tableNameOut = TableName.valueOf("bus");
        hbaseContext3.bulkPut(HBaseRDD, tableNameOut, new Function<Put, Put>() {
            private static final long serialVersionUID = -8178588216657834982L;
            @Override
            public Put call(Put arg0) throws Exception {
                return arg0;
            }
        });
        
        javaSparkContext.stop();

    }
}
