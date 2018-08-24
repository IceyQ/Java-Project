package com.ncut.bus.preprocess;
import java.io.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class TerminalNoExtraction {
    public static  void main(String []args) throws IOException {
        String dir = "G:\\bus\\data";
        File file = new File(dir);
        String[] dirs=null;
        if(file.isDirectory()){
             dirs= file.list();
        }
        Set set = new HashSet();
        for(String fileName:dirs){
            System.out.println(fileName);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(dir + '/' +fileName));

            String line = null;
            int n = 0;
            while(null!=(line=bufferedReader.readLine()) && !line.equals("")){
                n ++;
                if(n>1){
                    String[] strs = line.split(",");
                    System.out.println(strs);
                    set.add(strs[1]);
                }
            }
            bufferedReader.close();
        }
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("G:\\bus\\bus.csv"));
        Iterator<String> iterator = set.iterator();
        while (iterator.hasNext()){
           bufferedWriter.write(iterator.next()+"\r\n");
        }
        bufferedWriter.close();
    }
}
