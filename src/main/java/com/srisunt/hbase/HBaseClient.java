package com.srisunt.hbase;

import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;


public class HBaseClient {
    private Connection connection;

    public HBaseClient(Connection connection) {
        this.connection = connection;
    }



    public static class HbaseConfig {
        private final Logger LOG = LoggerFactory.getLogger(HbaseConfig.class);

        public static org.apache.hadoop.conf.Configuration getHHConfig() {
            Configuration conf = HBaseConfiguration.create();
            InputStream confResourceAsInputStream = conf.getConfResourceAsInputStream("hbase-site.xml");
            int available = 0;
            try {
                available = confResourceAsInputStream.available();
            } catch (Exception e) {
                //for debug purpose
                //LOG.debug("configuration files not found locally");
            } finally {
                IOUtils.closeQuietly(confResourceAsInputStream);
            }
            if (available == 0) {
                conf = new Configuration();
                conf.addResource("core-site.xml");
                conf.addResource("hbase-site.xml");
                conf.addResource("hdfs-site.xml");
            }
            return conf;
        }
    }
    public static void main(String[] args) throws IOException {
        Configuration conf = HbaseUtils.loginKeyTab(args);
        //loginExistingKerberos(args);
        //loginKerberos(args);


        Connection connection = new ConnectionGetter(conf).getConnection();


        Admin admin = connection.getAdmin();
        TableName[] names = admin.listTableNames();
        for (TableName name : names) {
            System.out.println("name:\t" + name.getNameAsString());
        }


        System.out.println( HbaseUtils.find("song_test", "row1", "cf", Sets.newHashSet("name"),connection));


        System.exit(0);
        java.sql.Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String secure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-secure:ssrisunt@EXAMPLE.COM:/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/ssrisunt.keytab";

            //String unsecure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure";
            con = DriverManager.getConnection(secure);

            stmt = con.createStatement();

            //stmt.executeUpdate("create table test2 (mykey integer not null primary key, mycolumn varchar)");
            stmt.executeUpdate("upsert into test2 values (1,'Hello 2')");
            stmt.executeUpdate("upsert into test2 values (2,'World! 2')");
            con.commit();

            PreparedStatement statement = con.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            //logger.error("Connection fail: ", e);
        }
    }

    public static void loginKeyTab(String[] args) throws IOException {
        // Setting up the HBase configuration
        //Configuration conf = HbaseConfig.getHHConfig();
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");

        // Point to the krb5.conf file. Alternatively this could be setup when running the program using: -Djava.security.krb5.conf=<full path to krb5.conf>
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
        System.setProperty("sun.security.krb5.debug", "false");

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
        UserGroupInformation.loginUserFromKeytab(principal,keytabLocation);




        //dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        //logger.error("Initialized hbase");


    }

    public static void loginExistingKerberos(String[] args) throws IOException {
        // Setting up the HBase configuration
        //Configuration conf = HbaseConfig.getHHConfig();
        Configuration conf = HBaseConfiguration.create();

        System.setProperty("sun.security.krb5.debug", "true");

        conf.set("hbase.master", "sandbox.hortonworks.com:16000");
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hbase-secure");



        //System.setProperty("java.security.auth.login.config", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/jass.conf");
        System.setProperty("java.security.krb5.conf", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/krb5.conf");


        System.setProperty("java.security.krb5.realm","EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc","sandbox.hortonworks.com");
        //System.setProperty("javax.security.auth.useSubjectCredsOnly","true");


        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");


        UserGroupInformation.setConfiguration(conf);
         UserGroupInformation.getLoginUser();


        Connection connection = new ConnectionGetter(conf).getConnection();

        // Hbase

        Admin admin = connection.getAdmin();
        TableName[] names = admin.listTableNames();
        for (TableName name : names) {
            System.out.println("name:\t" + name.getNameAsString());
        }
        // Phoenix

        java.sql.Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            //String secure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-secure:admin/admin@EXAMPLE.COM:/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/admin.keytab";
            String secure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-secure:admin/admin@EXAMPLE.COM";

            //String unsecure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure";
            con = DriverManager.getConnection(secure);

            stmt = con.createStatement();

            //stmt.executeUpdate("create table test2 (mykey integer not null primary key, mycolumn varchar)");
            stmt.executeUpdate("upsert into test2 values (1,'Hello 2')");
            stmt.executeUpdate("upsert into test2 values (2,'World! 2')");
            con.commit();

            PreparedStatement statement = con.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }
            statement.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
            //logger.error("Connection fail: ", e);
        }

    }

    public static void loginKerberos(String[] args) throws IOException {
        Subject subject = login();
        Configuration conf = HBaseConfiguration.create();

        System.setProperty("sun.security.krb5.debug", "true");

        conf.set("hbase.master", "sandbox.hortonworks.com:16000");
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent", "/hbase-secure");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(subject);

        Connection connection = new ConnectionGetter(conf).getConnection();

        // Hbase

        Admin admin = connection.getAdmin();
        TableName[] names = admin.listTableNames();
        for (TableName name : names) {
            System.out.println("name:\t" + name.getNameAsString());
        }

    }


    public static Subject login() {
        String userName = "ssrisunt@EXAMPLE.COM";
        char[] password = "yong2904".toCharArray();
        System.setProperty("java.security.krb5.conf", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/save/jass.conf");

        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");
        System.setProperty("java.security.krb5.kdc", "sandbox.hortonworks.com");
        try {
            LoginContext lc = new LoginContext("client", new UserNamePasswordCallbackHandler(userName, password));
            lc.login();
            System.out.println("KerberosAuth.main: " + lc.getSubject());
            return lc.getSubject();
        } catch (LoginException le) {
            le.printStackTrace();
        }
        return null;
    }

    public static class UserNamePasswordCallbackHandler implements CallbackHandler {
        private String _userName;
        private char[] _password;

        public UserNamePasswordCallbackHandler(String userName, char[] password) {
            _userName = userName;
            _password = password;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback && _userName != null) {
                    ((NameCallback) callback).setName(_userName);
                } else if (callback instanceof PasswordCallback && _password != null) {
                    ((PasswordCallback) callback).setPassword(_password);
                }
            }
        }
    }





}