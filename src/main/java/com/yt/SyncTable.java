/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class SyncTable implements Callable<Object> {
    private Connection sourceConn = null;
    private Connection destConn = null;
    private String tableName;
    private boolean truncateFlag = false;
    private int fetchsize = 3000;

    public SyncTable(Connection sourceConn, Connection destConn, String tableName, boolean truncateFlag, int fetchsize) {
        this(sourceConn, destConn, tableName);
        this.fetchsize = fetchsize;
        this.truncateFlag = truncateFlag;
    }

    public SyncTable(Connection sourceConn, Connection destConn, String tableName, int fetchsize) {
        this(sourceConn, destConn, tableName);
        this.fetchsize = fetchsize;
    }

    public SyncTable(Connection sourceConn, Connection destConn, String tableName, boolean truncateFlag) {
        this(sourceConn, destConn, tableName);
        this.truncateFlag = truncateFlag;
    }

    public SyncTable(Connection sourceConn, Connection destConn, String tableName) {
        this.sourceConn = sourceConn;
        this.tableName = tableName;
        this.destConn = destConn;
    }

    public void process(Connection sourceDB, Connection desDB, String tableName, long startTime) throws SQLException {
        //        儲存Trigger 狀態
        Map<String, String> triggerstatus = DBUtil.getTriggerStatus(desDB, tableName);
        ResultSet resultSet = null;
        PreparedStatement preState = null;
        try {//            確認DB的構整一致
            if (DBUtil.checkTablestructure(sourceDB, desDB, tableName)) {
                //                是否先清除目地DB資料
                if (truncateFlag) {
                    DBUtil.executeStatement(desDB, "truncate  table " + tableName);
                }
                int sdataCount = DBUtil.getDataCount(sourceDB, tableName);
                int ddataCount = DBUtil.getDataCount(desDB, tableName);
//            如兩個DB資料量不同才更新
                if (sdataCount != ddataCount) {
                    //                    取得欄位資料
                    List<String> column = new ArrayList<>(DBUtil.getColumnSchema(sourceDB, tableName).keySet());
//                    建立Insert Statement
                    preState = desDB.prepareStatement(DBUtil.getInsertStatement(tableName, column));
//                    關閉觸發器
                    DBUtil.executeStatement(desDB, "alter table " + tableName + "  disable all triggers");
//                    開始匯入資料
                    resultSet = DBUtil.getTableData(sourceDB, tableName, column);
//                    調整從DB取資料的大小
                    resultSet.setFetchSize(fetchsize);
//                    開始匯入資料
                    importData(desDB, resultSet, preState, column, fetchsize, sdataCount, startTime);
                } else {
                    System.out.println("資料數量一致，無需匯入");
                }
            }
        } finally {
            //        恢複Trigger 狀態
            DBUtil.recoverTrigger(desDB, triggerstatus);
            if (resultSet != null) resultSet.close();
            if (preState != null) preState.close();
        }
    }

    private void printProgress(Connection destConn, long startTime, long total, long current, String insertTable) throws InterruptedException, IllegalArgumentException, SQLException {
        long current1 = current + 1;
        long eta = current1 == 0 ? 0 : (total - current1) * (System.currentTimeMillis() - startTime) / current1;
        String etaHms = current1 == 0 ? "N/A" : String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta), TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1), TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));
        StringBuilder string = new StringBuilder();
        int percent = (int) (current1 * 100 / total);
        string.append('\r').append(DBUtil.getDBConnDesc(destConn) + " ,table:" + insertTable).append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " "))).append(String.format(" %d%% [", percent)).append(String.join("", Collections.nCopies(percent, "="))).append('>').append(String.join("", Collections.nCopies(100 - percent, " "))).append(']').append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current1)), " "))).append(String.format(" %d/%d, ETA: %s", current1, total, etaHms));
        System.out.print(string);
    }

    private void importData(Connection desDB, ResultSet resultSet, PreparedStatement preState, List<String> column, int fetchsize, int sdataCount, long startTime) throws SQLException {
        int count = 0;
        while (resultSet.next()) {
            for (int i = 1;
                 i <= column.size();
                 i++) {
                preState.setObject(i, resultSet.getObject(column.get(i - 1)));
            }
            try {
                if (count % fetchsize == 0 || count == sdataCount - 1) {
                    preState.addBatch();
                    preState.executeBatch();
                    preState.clearBatch();
                    desDB.commit();
//                    3分鐘以上顯示進度
                    if (((System.currentTimeMillis() - startTime) / 1000) / 60 > 3) {
                        try {
                            printProgress(desDB, startTime, sdataCount, count, tableName);
                        } catch (Exception e) {
                        }
                    }
                } else {
                    preState.addBatch();
                }
            } catch (
                    SQLException exception) {
                preState.clearBatch();
                desDB.rollback();
                throw new SQLException(exception);
            }

            count++;
        }
    }

    @Override
    public Object call() throws SQLException {
        long startTime = System.currentTimeMillis();
        int state = 0;
        String errorCode = "";
        try {
            this.sourceConn.setAutoCommit(false);
            this.destConn.setAutoCommit(false);
            System.out.printf("將%20s 從 %20s 匯入 %20s\n", tableName, DBUtil.getDBConnDesc(sourceConn), DBUtil.getDBConnDesc(destConn));
            process(sourceConn, destConn, tableName, startTime);
            state = DBUtil.checkBothRowNum(sourceConn, destConn, tableName, startTime);
        } catch (SQLException e) {
            errorCode = e.getMessage().substring(0, Math.min(e.getMessage().length(), 200));
            state = 9;
            System.err.println("匯入" + DBUtil.getDBConnDesc(destConn) + " 發生錯誤");
            return false;
        } finally {
            try {
                if (!errorCode.isEmpty()) {
                    DBUtil.insertLog(sourceConn, destConn, tableName, errorCode, state);
                }
            } catch (SQLException e) {
                throw new SQLException("寫入 insertLog" + DBUtil.getDBConnDesc(destConn) + "發生錯誤");
            }
        }
        return true;
    }
}