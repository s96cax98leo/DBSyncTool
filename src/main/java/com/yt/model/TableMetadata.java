package com.yt.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableMetadata {
    private final String tableName;
    private final List<ColumnMetadata> columns;
    private final List<String> primaryKeys;
    private String tableComment;

    public TableMetadata(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.primaryKeys = new ArrayList<>();
    }

    public void addColumn(ColumnMetadata column) {
        columns.add(column);
    }

    public void addPrimaryKey(String columnName) {
        primaryKeys.add(columnName);
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadata that = (TableMetadata) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(primaryKeys, that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns, primaryKeys);
    }

    public static class ColumnMetadata {
        private final String name;
        private final String dataType;
        private final int dataLength;
        private final int dataPrecision;
        private final boolean nullable;
        private final int columnId;
        private String comment;

        public ColumnMetadata(String name, String dataType, int dataLength, 
                            int dataPrecision, boolean nullable, int columnId) {
            this.name = name;
            this.dataType = dataType;
            this.dataLength = dataLength;
            this.dataPrecision = dataPrecision;
            this.nullable = nullable;
            this.columnId = columnId;
        }

        public String getName() {
            return name;
        }

        public String getDataType() {
            return dataType;
        }

        public int getDataLength() {
            return dataLength;
        }

        public int getDataPrecision() {
            return dataPrecision;
        }

        public boolean isNullable() {
            return nullable;
        }

        public int getColumnId() {
            return columnId;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnMetadata that = (ColumnMetadata) o;
            return dataLength == that.dataLength &&
                    dataPrecision == that.dataPrecision &&
                    nullable == that.nullable &&
                    columnId == that.columnId &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(dataType, that.dataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, dataType, dataLength, dataPrecision, nullable, columnId);
        }
    }
} 