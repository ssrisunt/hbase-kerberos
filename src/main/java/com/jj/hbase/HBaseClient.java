package com.jj.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class HBaseClient {
    private Connection connection;

    public HBaseClient(Connection connection) {
        this.connection = connection;
    }

    public void insert(String table, Map<String, ColumnFamily> columnFamilies) throws IOException {
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

    public void insert(String table, String rowKey, ColumnFamily columnFamily) throws IOException {
        try (Table t = connection.getTable(TableName.valueOf(table))) {
            final byte[] family = Bytes.toBytes(columnFamily.getName());

            Put put = new Put(Bytes.toBytes(rowKey));
            columnFamily.getColumns().forEach((column, value) -> put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value)));

            t.put(put);
        }
    }

    public Result find(String tableName, String rowKey, String columnFamily, Set<String> columns) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            final byte[] family = Bytes.toBytes(columnFamily);
            Get get = new Get(Bytes.toBytes(rowKey));

            columns.forEach(c -> get.addColumn(family, Bytes.toBytes(c)));

            return table.get(get);
        }
    }

    public void delete(String tableName, String rowKey) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        }
    }

    public void delete(String tableName, String rowKey, String columnFamily) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(Bytes.toBytes(columnFamily));

            table.delete(delete);
        }
    }

    public static class HbaseConfig {
        private  final Logger LOG = LoggerFactory.getLogger(HbaseConfig.class);
        public static   org.apache.hadoop.conf.Configuration getHHConfig() {
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
            if (available == 0 ) {
                conf = new Configuration();
                conf.addResource("core-site.xml");
                conf.addResource("hbase-site.xml");
                conf.addResource("hdfs-site.xml");
            }
            return conf;
        }
    }

    public static void main(String[] args) throws IOException {
        // Setting up the HBase configuration
        Configuration conf = HbaseConfig.getHHConfig();
        //conf.addResource("hbase-site.xml");

        // Point to the krb5.conf file. Alternatively this could be setup when running the program using: -Djava.security.krb5.conf=<full path to krb5.conf>
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");

        String principal = System.getProperty("kerberosPrincipal", "admin/admin@EXAMPLE.COM");
        String keytabLocation = System.getProperty("kerberosKeytab", "src/main/reources/admin.keytab");

        conf.set("hbase.master", "sandbox.hortonworks.com:16000");
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("zookeeper.znode.parent","/hbase-secure");


        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                "admin/admin@EXAMPLE.COM", "/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/admin.keytab");

        Connection  connection = new ConnectionGetter(conf).getConnection();
        Admin admin = connection.getAdmin();
        TableName[] names = admin.listTableNames();
        for (TableName name:names) {
            System.out.println("name:\t"+name.getNameAsString());
        }
        //HBaseClient client = new HBaseClient(new ConnectionGetter(configuration).getConnection());
        //client.insert("test", "test-java-1", new ColumnFamily("test-family").addColumn("col1", "val-java-1"));
       // System.out.println(client.find("test", "test-1", "test-family", Sets.newHashSet("col1")));

        java.sql.Connection con = null;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            String secure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-secure:admin/admin@EXAMPLE.COM:/Users/ssrisunt/workspace/hbase/hdp-test-examples/src/main/resources/admin.keytab";
            //String unsecure = "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure";
            con = DriverManager.getConnection(secure);

            stmt = con.createStatement();

            stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
            stmt.executeUpdate("upsert into test values (1,'Hello')");
            stmt.executeUpdate("upsert into test values (2,'World!')");
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

        //dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        //logger.error("Initialized hbase");


    }
}