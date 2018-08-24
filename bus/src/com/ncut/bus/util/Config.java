package com.ncut.bus.util;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static Properties properties;

    static {
        properties = new Properties();
        ClassLoader classLoader = Config.class.getClassLoader();
        if (null != classLoader) {
            InputStream inputStream = classLoader.getResourceAsStream("config.properties");
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static  String getFSPath(){
        return  properties.getProperty("hdfspath");
    }
}
