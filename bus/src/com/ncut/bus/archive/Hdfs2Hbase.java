package com.ncut.bus.archive;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.ncut.bus.util.Config;

import java.io.IOException;


public class Hdfs2Hbase {
    public static void main(String []args) throws IOException {
    	// 初始化Spark
        // create SparkContext
        // 每个Spark应用都由一个驱动器程序来发起集群上的各种并行操作，驱动器程序通过SparkContext对象来访问Spark
        // 这个对象代表对计算集群的一个连接
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("BusSpark");
    	//SparkConf sparkConf = new SparkConf().setAppName("BusSpark");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // 获取hdfs上的文件路径列表
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path filePath = new Path(Config.getFSPath());
        FileStatus[] fStatus = hdfs.listStatus(filePath);
        for (FileStatus fs: fStatus) {
            // get csvPath
            String csvPath = fs.getPath().toString();
            // get date
            final String date = csvPath.substring(csvPath.length()-12,csvPath.length()-4);
            // 通过javaSparkContext对象来创建RDD
            JavaRDD<String> inputRDD = javaSparkContext.textFile(csvPath);
            //System.out.println("Date:"+date+"; lines:"+inputRDD.count());

            JavaRDD<Put> HBaseRDD = inputRDD
                    .map(new Function<String, Put>() {
                             public Put call(String s) throws Exception {
                                 String[] strs = s.split(",");
                                 Put put = new Put(Bytes.toBytes(date+"_"+strs[0]+"_"+strs[1]));
                                 put.addColumn("f1".getBytes(),strs[2].getBytes(),s.getBytes());
                                 return put;
                             }
                         }
                    );
            // ========================== Write out to HBase ==============================
            Configuration outConf = HBaseConfiguration.create();
            JavaHBaseContext hbaseContext3 = new JavaHBaseContext(javaSparkContext, outConf);
            TableName tableNameOut = TableName.valueOf("Bus");
            hbaseContext3.bulkPut(HBaseRDD, tableNameOut, new Function<Put, Put>() {
                private static final long serialVersionUID = -8178588216657834982L;
                @Override
                public Put call(Put arg0) throws Exception {
                    return arg0;
                }
            });
        }
        javaSparkContext.stop();

    }
}
