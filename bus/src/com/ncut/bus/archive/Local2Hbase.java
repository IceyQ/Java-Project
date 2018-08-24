package com.ncut.bus.archive;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.ncut.bus.util.HbaseUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// hhhhh... 边读边存就不会Out of memory 了。。。。
public class Local2Hbase {
    public static void main(String[] args) throws IOException {
        //get table
        String tableName = "bus";
        Table table = HbaseUtil.getTable(tableName);

        // root of data set
        String dir = "G:\\bus\\data";
        File file = new File(dir);
        // get data files
        String[] dirs = null;
        if (file.isDirectory()){
            dirs = file.list();
        }

        for (String fileName:dirs) {
            // print
            System.out.println("\n[INFO]starting reading data files...");
            System.out.println("dataDate:"+fileName.substring(5,13));

            List<String> list = new ArrayList<String>();
            Set<String> set = new HashSet<String>();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(dir + '/' + fileName));
            String line = null;
            int n = 0;
            while (null != (line = bufferedReader.readLine()) && !line.equals("")) {
                n++;
                if (n > 1) {
                    String[] strs = line.split(",");
                    set.add(strs[0]+"_"+strs[1]);
                    //list.add(line);
                }
            }
            bufferedReader.close();
            // print
            //System.out.println("\n[INFO]starting comparing");
            for (String No:set){
                // print
                //System.out.println("\nrow<key>: "+ No + "_"+fileName.substring(5,13));
                // row
                Put put = new Put(Bytes.toBytes(No + "_"+fileName.substring(5,13)));

                BufferedReader bufferedReader1 = new BufferedReader(new FileReader(dir + '/' + fileName));
                String line1 = null;
                int n1 = 0;
                while (null != (line1 = bufferedReader1.readLine()) && !line1.equals("")) {
                    n1++;
                    if (n1 > 1) {
                        String[] temp = line1.split(",");
                        String lineNo = No.substring(0,No.length()-7);
                        String terminalNo = No.substring(No.length()-6,No.length());

                        if (lineNo.equals(temp[0]) && terminalNo.equals(temp[1])){
                            // column
                            //System.out.println("column<key>: "+temp[2]);
                            //System.out.println("column<value>: "+line1);
                            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes(temp[2]), Bytes.toBytes(line1));
                        }
                    }
                }
                bufferedReader1.close();

                table.put(put);
            }
        }
        table.close();
    }
}