package com.srisunt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by ssrisunt on 11/7/16.
 */
public class HbaseUtils {

    public static Configuration loginKeyTab(String[] args) throws IOException {
        // Setting up the HBase configuration
        //Configuration conf = HbaseConfig.getHHConfig();
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
        UserGroupInformation.loginUserFromKeytab(principal,keytabLocation);

        return conf;

    }

    public static Configuration loginExistingKerberos(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("hbase-site.xml");

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


        return conf;

    }

    public static Configuration loginKerberos(String[] args) throws IOException {
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

        return conf;

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

    public void insert(String table, Map<String, ColumnFamily> columnFamilies, Connection connection) throws IOException {
        try (Table t = connection.getTable(TableName.valueOf(table))) {
            List<Put> puts = new ArrayList<>(columnFamilies.size());
            columnFamilies.forEach((rowKey, columnFamily) -> {
                final byte[] family = Bytes.toBytes(columnFamily.getName());
                Put put = new Put(Bytes.toBytes(rowKey));
                columnFamily.getColumns().forEach((column, value) -> put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value)));
                puts.add(put);
            });

            t.put(puts);
        }
    }

    public void insert(String table, String rowKey, ColumnFamily columnFamily, Connection connection) throws IOException {
        try (Table t = connection.getTable(TableName.valueOf(table))) {
            final byte[] family = Bytes.toBytes(columnFamily.getName());

            Put put = new Put(Bytes.toBytes(rowKey));
            columnFamily.getColumns().forEach((column, value) -> put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value)));

            t.put(put);
        }
    }

    public static Result find(String tableName, String rowKey, String columnFamily, Set<String> columns, Connection connection) throws IOException {
        try (Table table =  connection.getTable(TableName.valueOf(tableName))) {
            final byte[] family = Bytes.toBytes(columnFamily);
            Get get = new Get(Bytes.toBytes(rowKey));

            columns.forEach(c -> get.addColumn(family, Bytes.toBytes(c)));

            return table.get(get);
        }
    }

    public void delete(String tableName, String rowKey, Connection connection) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }
    }

    public void delete(String tableName, String rowKey, String columnFamily, Connection connection) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(Bytes.toBytes(columnFamily));

            table.delete(delete);
        }
    }
}
