/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SyncDB implements Callable<Object> {
    private boolean truncateFlag = false;
    int threadNum = 3;
    int fetchSize = 3000;
    private final ConnectProperties sourceConn;
    private final ConnectProperties destConn;
    private final List<String> tableList;

    public SyncDB(ConnectProperties sourceConn, ConnectProperties destConn, List<String> tableList) {
        this.sourceConn = sourceConn;
        this.destConn = destConn;
        this.tableList = tableList;
    }

    public SyncDB(ConnectProperties sourceConn, ConnectProperties destConn, List<String> tableList, int threadNum) {
        this(sourceConn, destConn, tableList);
        this.threadNum = threadNum;
    }

    public SyncDB(ConnectProperties sourceConn, ConnectProperties destConn, List<String> tableList, boolean truncateFlag) {
        this(sourceConn, destConn, tableList);
        this.truncateFlag = truncateFlag;
    }

    public SyncDB(ConnectProperties sourceConn, ConnectProperties destConn, List<String> tableList, boolean truncateFlag, int fetchSize) {
        this(sourceConn, destConn, tableList, truncateFlag);
        if (fetchSize > 3000) this.fetchSize = fetchSize;
    }

    public SyncDB(ConnectProperties sourceConn, ConnectProperties destConn, List<String> tableList, boolean truncateFlag, int fetchSize, int threadNum) {
        this(sourceConn, destConn, tableList, truncateFlag, fetchSize);
        this.threadNum = threadNum;
    }

    private void printProgress(long total, long current) {
        int percent = (int) (current * 100 / total);
        System.out.printf("%s 同步完成: %d / %d  = %d %% \n", destConn.getDBConnDesc(), current, total, percent);
    }

    @Override
    public Object call() throws Exception {
        ExecutorService excute = Executors.newFixedThreadPool(threadNum);
        LinkedList<Future<Object>> linkedList = new LinkedList<>();
        int finishTable = 0;
        Connection source;
        Connection dest;
        try {
            source = DBUtil.getConn(sourceConn);
        } catch (SQLException e) {
            try {
                source = DBUtil.getConn(sourceConn);
            } catch (SQLException se) {
                System.out.println("連線問題:" + sourceConn.getDBConnDesc());
                throw new SQLException(se + "連線問題:" + sourceConn.getDBConnDesc());
            }
        }
        try {
            dest = DBUtil.getConn(destConn);
        } catch (SQLException e) {
            try {
                dest = DBUtil.getConn(destConn);
            } catch (SQLException se) {
                System.out.println("連線問題:" + destConn.getDBConnDesc());
                throw new SQLException(se + "連線問題:" + destConn.getDBConnDesc());
            }
        }
        for (String insertTable : tableList) {
            linkedList.add(excute.submit(new SyncTable(source, dest, insertTable, truncateFlag, fetchSize)));
        }
        while (!linkedList.isEmpty()) {
            for (int i = 0;
                 i < linkedList.size();
                 i++) {
                Future<Object> o = linkedList.get(i);
                if (o.isDone()) {
                    Object ob = o.get();
                    if (ob instanceof Boolean) {
                        if ((boolean) ob) finishTable++;
                        printProgress(tableList.size(), finishTable);
                    }
                    synchronized (linkedList) {
                        linkedList.remove(i);
                    }
                }
            }
        }
        System.out.println();
        try {
            if (source != null) source.close();
            if (dest != null) dest.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        return 1;
    }
}