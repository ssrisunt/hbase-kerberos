package com.srisunt.spark;

import com.srisunt.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;



/**
 * Created by ssrisunt on 11/7/16.
 */
public class SparkHive {

    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_CONNECTION_URL = "jdbc:hive2://sandbox:10000/default";

    public static void main(String[] args) throws Exception {
        try {
            Configuration conf = HbaseUtils.loginKeyTab(args);

            System.out.println("Before getting connection");


            JavaSparkContext sc =
                    new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));


            HiveContext hc = new HiveContext(sc);

            long count = hc.sql("select * from sample_07").count();

            System.out.println("--------- Count:"+count);

            sc.stop();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }

}
