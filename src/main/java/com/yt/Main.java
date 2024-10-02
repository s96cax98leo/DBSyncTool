/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int fetchSize = 3000;
        try {
            if (args[0] != null) {
                fetchSize = Math.min(Integer.parseInt(args[0]), fetchSize);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        long startTime = System.currentTimeMillis();
//        ConnectionFactory
        List<ConnectProperties> DBList = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        System.out.print("請輸入同步時DB Thread 數量 :");
        ExecutorService excute = Executors.newFixedThreadPool(scanner.nextInt());
        System.out.print("請輸入同步時Table Thread 數量 :");
        int threadnum = scanner.nextInt();
        System.out.print("請確認是否清空目的DB的表  Y/N :");
        boolean truncateFlage = scanner.next().equalsIgnoreCase("Y");
        System.out.print("請輸入來源DB:");
        ConnectProperties sourceDB = new ConnectProperties(new File(scanner.next()));
        System.out.print("請輸入目的DB:");
        ConnectProperties destDB = new ConnectProperties(new File(scanner.next()));
        DBList.add(destDB);
        System.out.print("請輸入 絕對路徑或相對路徑檔案名稱 例:tablelist.txt 或 T:\\test\\tablelist.txt :");
        InputStreamReader isr = new InputStreamReader(Files.newInputStream(Paths.get(scanner.next())));
        //檔案讀取路徑
        BufferedReader reader = new BufferedReader(isr);
        List<String> tableList = reader.lines().filter(e -> !e.isEmpty()).map(String::toUpperCase).collect(Collectors.toList());
        LinkedBlockingQueue<Future<Object>> que = new LinkedBlockingQueue<>();
        for (ConnectProperties destConn : DBList) {
            SyncDB dbToDBThread = new SyncDB(sourceDB, destConn, tableList, truncateFlage, fetchSize, threadnum);
            que.add(excute.submit(dbToDBThread));
        }
        waitingThreadFinish(que);
        scanner.close();
        System.exit(1);
    }

    public static void waitingThreadFinish(LinkedBlockingQueue<Future<Object>> blockingQueue) throws InterruptedException, ExecutionException {
        while (!blockingQueue.isEmpty()) {
            synchronized (blockingQueue) {
                blockingQueue.take().get();
            }
        }
    }
}