package com.jj.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HBaseClientTest {
    static final String TABLE_NAME = "table";
    static final TableName TABLE_NAME_TB = TableName.valueOf(TABLE_NAME);

    Connection connection;
    HBaseClient client;

    @Before
    public void setUp() throws Exception {
        connection = mock(Connection.class);
        client = new HBaseClient(connection);
    }

    @Test(expected = NullPointerException.class)
    public void insertShouldProperlyFailOnNullColumnFamilies() throws Exception {
        Table t = mock(Table.class);

        when(connection.getTable(TABLE_NAME_TB)).thenReturn(t);

        client.insert(TABLE_NAME, null);
        verify(t).close();
    }

    @Test
    public void insertShouldProperlyProcessEmptyColumnFamilies() throws Exception {
        Table t = mock(Table.class);

        when(connection.getTable(TABLE_NAME_TB)).thenReturn(t);

        client.insert(TABLE_NAME, new HashMap<>());

        verify(t).put(new ArrayList<>(0));
        verify(t).close();
    }

    @Test
    public void insertShouldProperlyProcessNonEmptyColumnFamilies() throws Exception {
        Table t = mock(Table.class);

        when(connection.getTable(TABLE_NAME_TB)).thenReturn(t);

        HashMap<String, ColumnFamily> columnFamilies = new HashMap<>();
        columnFamilies.put("rowkey1", new ColumnFamily("colfam1").addColumn("col1", "value1"));
        columnFamilies.put("rowkey2", new ColumnFamily("colfam2").addColumn("col2", "value2"));
        client.insert(TABLE_NAME, columnFamilies);

        Put put1 = new Put(Bytes.toBytes("rowkey1"));
        put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));

        Put put2 = new Put(Bytes.toBytes("rowkey2"));
        put2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col2"), Bytes.toBytes("value2"));

        List<Put> puts = new ArrayList<>(2);
        puts.add(put1);
        puts.add(put2);

        verify(t).put((List<Put>) anyCollection()); // TODO: probably need to create a matcher?
        verify(t).close();
    }

    @Test
    public void insertShouldProperlyProcessNonEmptyColumnFamiliesAndCallAllEntriesInMap() throws Exception {
        Table t = mock(Table.class);

        when(connection.getTable(TABLE_NAME_TB)).thenReturn(t);

        Map<String, ColumnFamily> columnFamilies = mock(HashMap.class);
        client.insert(TABLE_NAME, columnFamilies);

        verify(columnFamilies).forEach(any());
        verify(t).put((List<Put>) anyCollection());
        verify(t).close();
    }
}
