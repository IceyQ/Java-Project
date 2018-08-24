package com.ncut.bus.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.net.URL;

public class HbaseUtil {
    private static Connection connection = null;

    public static Configuration getConfiguration(){
        Configuration config = HBaseConfiguration.create();
        URL hbaseUrl = HbaseUtil.class.getResource("/hbase-site.xml");
        config.addResource(hbaseUrl);
        return config;
    }

    public static Connection getConnection() throws IOException {
        Configuration config = getConfiguration();
        config.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 180000);
        connection = ConnectionFactory.createConnection(config);
        return connection;
}

    public static Table getTable(String tableName) throws IOException {
        if (connection == null) {
            connection = getConnection();
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }
}