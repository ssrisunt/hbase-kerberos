package com.srisunt.hive;

import com.srisunt.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class HiveClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main( String[] args )throws Exception{
        try {
            Configuration conf = HbaseUtils.loginKeyTab(args);
            System.out.println("Before getting connection");
            Class.forName(driverName);

            Connection connection=  DriverManager.getConnection("jdbc:hive2://sandbox.hortonworks.com:10000/default;principal=hive/sandbox.hortonworks.com@EXAMPLE.COM;auth-kerberos");
            System.out.println("After getting connection " + connection);

            ResultSet resultSet = connection.createStatement().executeQuery("select * from default.sample_07");

            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
