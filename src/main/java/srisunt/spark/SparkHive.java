package com.srisunt.spark;

import com.srisunt.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ssrisunt on 11/7/16.
 */
public class SparkHive {

    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_CONNECTION_URL = "jdbc:hive2://sandbox:10000/default";

    public static void main(String[] args) throws IOException {

        passPrincipalKeyTab(args);
        //passKerberosUGI();

    }

    public static void passPrincipalKeyTab(String[] args) throws IOException {

        Configuration conf = HbaseUtils.loginKeyTab(args);

        String principal = System.getProperty("kerberosPrincipal", "ssrisunt@EXAMPLE.COM");
        String keytabLocation = System.getProperty("kerberosKeytab", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/ssrisunt.keytab");



        SparkConf sparkconf = new SparkConf().set("spark.master", "yarn-client")
                .set("spark.app.name", "spark11111")
                .set("spark.cores.max", "1").set("spark.executor.memory", "512m")
                .set("spark.hadoop.yarn.resourcemanager.hostname", "sandbox.hortonworks.com")
                .set("spark.hadoop.yarn.resourcemanager.address","sandbox.hortonworks.com:8050")
                .set("spark.yarn.keytab",keytabLocation)
                .set("spark.yarn.principal",principal)
                .set("yarn.resourcemanager.principal",principal);


//        sparkconf.set("spark.hadoop.yarn.application.classpath",
//                "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
//                        + "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
//                        + "$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,"
//                        + "$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*");

        sparkconf.set("spark.hadoop.yarn.application.classpath",
                "/etc/hadoop/conf,"
                        + "/usr/hdp/current/hadoop/*,"
                        + "/usr/hdp/current/hadoop/lib/*,"
                        + "/usr/hdp/current/hadoop-hdfs/*,"
                        + "/usr/hdp/current/hadoop-hdfs/lib/*,"
                        + "/usr/hdp/current/hadoop-mapreduce/*,"
                        + "/usr/hdp/current/hadoop-mapreduce/lib/*,"
                        + "/usr/hdp/current/hadoop-yarn-client/*,"
                        + "/usr/hdp/current/hadoop-yarn-client/lib/*,"
                        + "/usr/hdp/current/spark-client/*,"
                        + "/usr/hdp/current/spark-client/lib/*,"
                        + "/usr/hdp/current/spark-client/lib/*"
        );//.set("spark.driver.host","localhost");

        conf.set("yarn.resourcemanager.principal",principal);

        conf.set("spark.yarn.jar","/usr/hdp/2.4.0.0-169/hadoop/lib/spark-yarn-shuffle.jar");

        JavaSparkContext sc = new JavaSparkContext(sparkconf);


        HiveContext hc = new HiveContext(sc);

        long count = hc.sql("select * from default.sample_07").count();


        System.out.println("--------:"+ count);

        sc.stop();


    }

    public static void passKerberosUGI() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");

        // Point to the krb5.conf file. Alternatively this could be setup when running the program using: -Djava.security.krb5.conf=<full path to krb5.conf>
        System.setProperty("java.security.krb5.conf", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");

        String principal = System.getProperty("kerberosPrincipal", "ssrisunt@EXAMPLE.COM");
        String keytabLocation = System.getProperty("kerberosKeytab", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/ssrisunt.keytab");

        conf.set("hbase.master", "sandbox.hortonworks.com:16000");
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hbase-secure");


        System.setProperty("java.security.krb5.realm","EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc","sandbox.hortonworks.com");

        UserGroupInformation.setConfiguration(conf);

        UserGroupInformation ugi= null;
        try {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal,keytabLocation);
            UserGroupInformation.setLoginUser(ugi);
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws IOException {
                    SparkConf sparkconf = new SparkConf().set("spark.master", "yarn-client").set("spark.app.name", "sparkhivesqltest")
                            .set("spark.cores.max", "1").set("spark.executor.memory", "512m")
                            .set("spark.hadoop.yarn.resourcemanager.hostname", "sandbox.hortonworks.com")
                            .set("spark.hadoop.yarn.resourcemanager.address","sandbox.hortonworks.com:8050")
                            .set("spark.yarn.keytab",keytabLocation)
                            .set("spark.yarn.principal",principal)
                            .set("yarn.resourcemanager.principal",principal);

                    conf.set("yarn.resourcemanager.principal",principal);

                    sparkconf.set("test","sammy");

                    JavaSparkContext sc = new JavaSparkContext(sparkconf);

                    SQLContext sqlContext = new SQLContext(sc);

                    //Data source options
                    Map<String, String> options = new HashMap<String, String>();
                    options.put("driver", HIVE_DRIVER);
                    options.put("url", HIVE_CONNECTION_URL);
                    options.put("dbtable", "(select * from default.sample_07) as employees_name");
                    DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
                    System.out.println("--------:"+jdbcDF.collect().length);
                    sc.sc().stop();
                    return null;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
