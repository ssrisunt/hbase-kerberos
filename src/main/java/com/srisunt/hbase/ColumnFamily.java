package com.srisunt.hbase;

import java.util.HashMap;
import java.util.Map;

public class ColumnFamily {
    private String name;
    private Map<String, String> columns = new HashMap<>();

    public ColumnFamily(String name) {
        this.name = name;
    }

    public ColumnFamily addColumn(String name, String value) {
        columns.put(name, value);
        return this;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getColumns() {
        return columns;
    }
}
