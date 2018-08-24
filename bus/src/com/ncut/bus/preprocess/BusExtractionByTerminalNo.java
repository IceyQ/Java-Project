package com.ncut.bus.preprocess;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BusExtractionByTerminalNo {
    public static void main(String []args) throws IOException{
        String dir = "G:\\bus\\train";
        File file = new File(dir);
        String[] dirs = null;
        if(file.isDirectory()){
            dirs = file.list();
        }
        String terminalNo = null;
        BufferedReader bufferedReader = new BufferedReader(new FileReader("G:\\bus\\bus.csv"));

        while (null!=(terminalNo = bufferedReader.readLine()) && !terminalNo.equals("")) {
            System.out.println(terminalNo);
            //break;

            for(String fileName:dirs){
                List list = new ArrayList();
                System.out.println(fileName);
                BufferedReader bufferedReader1 = new BufferedReader(new FileReader(dir + '/' + fileName));
                String line = null;
                int n = 0;
                while (null!=(line = bufferedReader1.readLine()) && !line.equals("")){
                    n ++;
                    if(n>1){
                        String[] strs = line.split(",");
                        if(strs[1].equals(terminalNo)){
                             list.add(line);
                        }
                    }
                }
                bufferedReader1.close();
                //System.out.println(list);
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("G:\\bus\\train_extraction\\"+terminalNo+"_"+fileName));
                Iterator<String> iterator = list.iterator();
                while (iterator.hasNext()){
                    bufferedWriter.write(iterator.next()+"\r\n");
                }
                bufferedWriter.close();
            }
        }
        bufferedReader.close();
    }
}
