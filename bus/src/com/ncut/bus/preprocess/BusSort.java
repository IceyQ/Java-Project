package com.ncut.bus.preprocess;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


//Exception in thread "main" java.lang.OutOfMemoryError: GC overhead limit exceeded
public class BusSort {
    public static void main(String []args) throws IOException{
        String dir = "G:\\bus\\data";
        File file = new File(dir);
        String[] dirs = null;
        if (file.isDirectory()){
            dirs = file.list();
        }

        for (String fileName:dirs){
            System.out.println(fileName);
            List list = new ArrayList();

            BufferedReader bufferedReader = new BufferedReader(new FileReader(dir + '/' +fileName));

            String line = null;
            int n = 0;
            while(null!=(line=bufferedReader.readLine()) && !line.equals("")){
                n ++;
                if(n>1){
                    String[] strs = line.split(",");
                    list.add(line);
                }
            }
            bufferedReader.close();

            Collections.sort(list, new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    if(o1.toString().substring(6,12).equals(o2.toString().substring(6,12))){
                        return  o1.toString().substring(13,21).compareTo(o2.toString().substring(13,21));
                    }else {
                        return o1.toString().substring(6,12).compareTo(o2.toString().substring(6,12));
                    }
                }
            });
            System.out.println(list);
        }
    }
}
