/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DBUtil {
    public static final String DRIVER = "oracle.jdbc.driver.OracleDriver";

    static {
        try {
            Class.forName(DRIVER);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection getConn(ConnectProperties connectProperties) throws SQLException {
        Connection conn = DriverManager.getConnection(connectProperties.getUrl(), connectProperties.getOwner(), connectProperties.getPassword());
        conn.setAutoCommit(false);
        return conn;
    }




    public static void executeStatement(Connection destConn, String sql) throws SQLException {
        Statement statement = destConn.createStatement();
        statement.execute(sql);
        statement.close();
    }

    public static boolean checkTableExists(Connection destConn, String tableName) {
        Statement stmt = null;
        ResultSet rset = null;
        try {
            stmt = destConn.createStatement();
            rset = stmt.executeQuery("SELECT  count(rowid) count FROM \"" + tableName + "\" WHERE 1 = 1");
            if (rset.next()) {
                return true;
            }
        } catch (SQLException sqlException) {
            if (sqlException.getMessage().contains("ORA-00942")) {
                return false;
            }
        } finally {
            try {
                if (rset != null) rset.close();
                if (stmt != null) stmt.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
        return false;
    }

    public static String getDBConnDesc(Connection connection) throws SQLException {
        String url = connection.getMetaData().getURL();
        return url.substring(url.lastIndexOf("/") + 1) + "_" + connection.getMetaData().getUserName();
    }

    public static Map<String, TableColumnProperties> getColumnSchema(Connection sourceConn, String tableName) {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        Map<String, TableColumnProperties> columSchema = new HashMap<>();
        try {
            stmt = sourceConn.prepareStatement("select Column_Name , Data_Type , Data_Length, DATA_PRECISION , Nullable ,COLUMN_ID  from user_tab_columns where table_name = ? order by COLUMN_ID");
            stmt.setString(1, tableName.toUpperCase());
            rset = stmt.executeQuery();
            while (rset.next()) {
                columSchema.put(rset.getString("Column_Name"), new TableColumnProperties(rset.getString("Column_Name"), rset.getString("DATA_TYPE"), rset.getInt("Data_Length"), rset.getInt("DATA_PRECISION"), rset.getString("Nullable"), rset.getInt("COLUMN_ID")));
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            try {
                if (rset != null) rset.close();
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return columSchema;
    }

    public static List<String> getSqlTableConstraintColumnList(Connection conn, String tableName) throws SQLException {
        List<String> pk = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT constraint_name, column_name from user_constraints natural join user_cons_columns where table_name =upper(?)  and CONSTRAINT_TYPE='P' ";
            pk = new ArrayList<>();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableName);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                pk.add(rs.getString("column_name"));
            }
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            rs.close();
            pstmt.close();
        }
        return pk;
    }

    public static boolean checkTablestructure(Connection source, Connection dest, String tableName) throws SQLException {
        boolean sourcetable = DBUtil.checkTableExists(source, tableName);
        boolean desttable = DBUtil.checkTableExists(dest, tableName);
        if (sourcetable && desttable) {
            List<String> sourcepk = DBUtil.getSqlTableConstraintColumnList(source, tableName);
            List<String> destpk = DBUtil.getSqlTableConstraintColumnList(dest, tableName);
            if ((DBUtil.getColumnSchema(source, tableName)).equals(DBUtil.getColumnSchema(dest, tableName))) {
                if (sourcepk.equals(destpk)) {//                    System.err.println(tableName + "\t表結構 相同   PK 欄位相同");
                    return true;
                } else {
                    throw new SQLException(tableName + "\t表結構:相同\t, PK:欄位不相同");
                }
            } else {
                if (sourcepk.equals(destpk)) {
                    throw new SQLException(tableName + "\t表結構:不相同\t, PK:欄位相同");
                } else {
                    throw new SQLException(tableName + "\t表結構:不相同\t, PK:欄位不相同");
                }
            }
        } else if (sourcetable && !desttable) {
            try {
                DBUtil.executeStatement(dest, getSqlCreateTableStatement(source, tableName.trim()));
                System.out.println(tableName + "表創建完成");
//            建立Table註解                addTableComment(dest, tableName, getTableComment(source, tableName));
//            建立Column註解                createColumnComments(source, dest, tableName);
            } catch (SQLException e) {
                throw new SQLException("表創建失敗");
            }
            return true;
        } else if (!sourcetable) {
            throw new SQLException("找不到來源表");
        }
        return false;
    }

    public static String getSqlCreateTableStatement(Connection conn, String tableName) throws SQLException {
        StringBuilder sql;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            sql = new StringBuilder();
            stmt = conn.createStatement();
            rset = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE 0 = 1");
            ResultSetMetaData rsmd = rset.getMetaData();
            sql.append("CREATE TABLE " + tableName + " (");
            for (int i = 1;
                 i <= rsmd.getColumnCount();
                 i++) {
                String nullable = "";
                int x = rsmd.isNullable(i);
                if (x == ResultSetMetaData.columnNoNulls) {
                    nullable = "NOT NULL";
                } else {
                    nullable = "NULL";
                }
                String colType = "";
                if (rsmd.getColumnType(i) == Types.BIGINT || rsmd.getColumnType(i) == Types.DECIMAL || rsmd.getColumnType(i) == Types.DOUBLE || rsmd.getColumnType(i) == Types.FLOAT || rsmd.getColumnType(i) == Types.INTEGER || rsmd.getColumnType(i) == Types.NUMERIC || rsmd.getColumnType(i) == Types.SMALLINT) {
                    if (rsmd.getColumnType(i) == Types.NUMERIC)
                        if (rsmd.getPrecision(i) == 0 && rsmd.getScale(i) == -127) colType = rsmd.getColumnTypeName(i);
                        else
                            colType = rsmd.getColumnTypeName(i) + "(" + rsmd.getPrecision(i) + "," + rsmd.getScale(i) + ")";
                } else if (rsmd.getColumnType(i) == Types.VARCHAR || rsmd.getColumnType(i) == Types.CHAR) {
                    colType = rsmd.getColumnTypeName(i) + "(" + rsmd.getPrecision(i) + ")";
                } else {
                    colType = rsmd.getColumnTypeName(i);
                }
                String eol = (i == rsmd.getColumnCount()) ? "" : ",";
                sql.append(rsmd.getColumnName(i) + " " + colType + " " + nullable + eol);
            }
            if (getSqlTableConstraintColumnList(conn, tableName).size() > 0)
                sql.append(getSqlTableConstraint(conn, tableName));
            sql.append(")");
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            if (rset != null) rset.close();
            if (stmt != null) stmt.close();
        }
        return sql.toString();
    }

    public static String getSqlTableConstraint(Connection conn, String tableName) throws SQLException {
        String constraint_sql = " , CONSTRAINT PK_" + tableName + " PRIMARY KEY (" +
                getSqlTableConstraintColumnList(conn, tableName).stream().collect(Collectors.joining(",")) +
                ")";
        return constraint_sql;
    }

    public static String getInsertStatement(String tableName, List<String> columnList) throws SQLException {
        if (columnList != null) {
            StringBuilder sql = new StringBuilder().append("insert into ").append(tableName).append(" ( ").append(String.join(",", columnList.stream().filter(e -> !e.replace("   ", "").isEmpty() || !e.equals("")).collect(Collectors.toList()))).append(")  values (");
            for (int i = 0;
                 i < columnList.stream().filter(e -> !e.replace("   ", "").isEmpty() || !e.equals("")).collect(Collectors.toList()).size();
                 i++) {
                sql.append("?,");
            }
            sql.delete(sql.lastIndexOf(","), sql.length());
            sql.append(")");
            return sql.toString().replace("\uFEFF", "");
        }
        throw new SQLException(tableName + "無Insert Statement ");
    }

    public static Map<String, String> getTriggerStatus(Connection desDB, String tableName) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        Map<String, String> map = new HashMap<>();
        try {
            stmt = desDB.prepareStatement("SELECT TRIGGER_NAME,STATUS FROM USER_TRIGGERS WHERE table_NAME =?");
            stmt.setString(1, tableName.toUpperCase());
            rset = stmt.executeQuery();
            while (rset.next()) {
                map.put(rset.getString("TRIGGER_NAME"), rset.getString("STATUS"));
            }
        } catch (SQLException sqlException) {
            throw new SQLException(tableName + " Trigger 取值有誤");
        } finally {
            if (rset != null) rset.close();
            if (stmt != null) stmt.close();
        }
        return map;
    }

    public static void setTriggerStatus(Connection destConn, String triggerName, String status) throws SQLException {
        if (triggerName == null || triggerName.equals("")) {
        } else {
            Statement stmt = destConn.createStatement();
            stmt.executeQuery("ALTER TRIGGER \"" + triggerName + "\" " + TriggerStatus(status));
            if (stmt != null) stmt.close();
        }
    }

    private static String TriggerStatus(String status) {
        if (status.equals("ENABLED")) return "ENABLE";
        else return "DISABLE";
    }

    public static void addTableComment(Connection destConn, String table, String comment) throws SQLException {
        Statement stmt = null;
        try {
            stmt = destConn.createStatement();
            stmt.executeQuery("COMMENT ON TABLE " + table + " IS  '" + comment + "' ");
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            if (stmt != null) stmt.close();
        }
    }

    public static void addColumnComment(Connection destConn, String table, String column, String comment) throws SQLException {
        if (comment == null || comment.equals("")) {
        } else {
            Statement stmt = null;
            try {
                stmt = destConn.createStatement();
                stmt.executeQuery("COMMENT ON COLUMN \"" + table + "\".\"" + column + "\" IS  '" + comment + "' ");
            } catch (SQLException e) {
                throw new SQLException(e);
            } finally {
                if (stmt != null) stmt.close();
            }
        }
    }

    public static String getTableComment(Connection destConn, String table) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        String comment = null;
        try {
            stmt = destConn.prepareStatement(" select comments from user_tab_comments where table_name  = ?");
            stmt.setString(1, table.toUpperCase());
            rs = stmt.executeQuery();
            comment = "";
            if (rs.next()) comment = rs.getString("comments");
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            if (stmt != null) stmt.close();
            if (rs != null) rs.close();
        }
        return comment == "" ? String.valueOf(System.currentTimeMillis()) : comment;
    }

    public static void createColumnComments(Connection sourceConn, Connection destConn, String tableName) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rset = null;
        try {
            stmt = sourceConn.prepareStatement("select table_name,column_name,comments  from user_col_comments where TABLE_NAME=?");
            stmt.setString(1, tableName.toUpperCase());
            rset = stmt.executeQuery();
            while (rset.next()) {
                String table_name = "";
                String column_name = "";
                String comments = "";
                table_name = rset.getString("table_name");
                column_name = rset.getString("column_name");
                comments = rset.getString("comments");
                DBUtil.addColumnComment(destConn, table_name, column_name, comments);
            }
        } catch (SQLException sqlException) {
            throw new SQLException(sqlException + "\t" + tableName + "欄位註解建立有誤");
        } finally {
            if (rset != null) rset.close();
            if (stmt != null) stmt.close();
        }
    }

    public static ResultSet getTableData(Connection sourceConn, String tableName, List<String> column) throws SQLException {
        Statement stmt = sourceConn.createStatement();
        ResultSet rset = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 1 order by " + column.stream().collect(Collectors.joining(",")));
        return rset;
    }

    public static int getDataCount(Connection sourceConn, String tableName) throws SQLException {
        int count = 0;
        Statement stmt = null;
        ResultSet rset = null;
        try {
            stmt = sourceConn.createStatement();
            rset = stmt.executeQuery("SELECT count(rowid) count  FROM " + tableName + " WHERE 1 = 1");
            if (rset.next()) {
                count = rset.getInt("count");
            }
        } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
            count = 0;
        } finally {
            if (rset != null) rset.close();
            if (stmt != null) stmt.close();
        }
        return count;
    }

    public static void recoverTrigger(Connection desDB, Map<String, String> triggerstatus) throws SQLException {
        for (String s : triggerstatus.keySet()) {
            DBUtil.setTriggerStatus(desDB, s, triggerstatus.get(s));
        }
    }

    public static int checkBothRowNum(Connection sourceConn, Connection destConn, String tableName, Long startTime) throws SQLException {
        int sdataCount = getDataCount(sourceConn, tableName);
        int ddataCount = getDataCount(destConn, tableName);
        if (sdataCount == ddataCount) {
            System.out.printf("將%20s 從 %20s 匯入 %20s\t完成 共%d分鐘\n", tableName, DBUtil.getDBConnDesc(sourceConn), DBUtil.getDBConnDesc(destConn), (((System.currentTimeMillis() - startTime) / 1000) / 60));
            return 0;
        } else {
            return 1;
        }
    }

    public static void insertLog(Connection sourceConn, Connection destConn, String insertTable, String errorCode, int status) throws SQLException {
        if (!DBUtil.checkTableExists(sourceConn, "DBSYNCLOG")) {
            String createTable = "CREATE TABLE DBSYNCLOG (" + "  SERIALNO NUMBER GENERATED ALWAYS AS IDENTITY INCREMENT BY 1 MINVALUE 1 ORDER NOT NULL ," + "  TABLE_NAME         VARCHAR2(200 BYTE)    NOT NULL ENABLE, " + "  DBSOURCE         VARCHAR2(200 BYTE)    NOT NULL ENABLE, " + "  DBDEST         VARCHAR2(200 BYTE)    NOT NULL ENABLE, " + "  STATUS     VARCHAR2(2 BYTE)    NOT NULL ENABLE, " + "  CREATETIME      TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP    NOT NULL ENABLE, " + "  ERRORCODE       VARCHAR2(200 BYTE) " + "  ,CONSTRAINT PK_DBSYNCLOG PRIMARY KEY ( SERIALNO,TABLE_NAME ) " + ")";
            Statement statement = sourceConn.createStatement();
            statement.execute(createTable);
            statement.close();
            statement = sourceConn.createStatement();
            statement.execute("COMMENT ON COLUMN DBSYNCLOG.STATUS IS '0:成功 1:數量有誤 9:執行錯誤'");
            statement.close();
        }
        if (DBUtil.checkTableExists(sourceConn, "DBSYNCLOG")) {
            PreparedStatement prepareStatement = sourceConn.prepareStatement("insert into " + "DBSYNCLOG" + " (TABLE_NAME,DBSOURCE,DBDEST,STATUS,CREATETIME,ERRORCODE) values (?,?,?,?,?,?)");
            String s = sourceConn.getMetaData().getURL();
            String d = destConn.getMetaData().getURL();
            prepareStatement.setString(1, insertTable);
            prepareStatement.setString(2, s.substring(s.lastIndexOf("/") + 1) + "_" + sourceConn.getMetaData().getUserName());
            prepareStatement.setString(3, d.substring(d.lastIndexOf("/") + 1) + "_" + destConn.getMetaData().getUserName());
            prepareStatement.setObject(4, status);
            prepareStatement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            prepareStatement.setObject(6, errorCode);
            prepareStatement.executeQuery();
            prepareStatement.clearParameters();
            sourceConn.commit();
            prepareStatement.close();
        }
    }
}