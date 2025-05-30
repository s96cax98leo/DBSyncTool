/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

import com.yt.config.DatabaseConfig;
import com.yt.exception.DBSyncException;
import com.yt.service.SyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int DEFAULT_FETCH_SIZE = 3000;

    public static void main(String[] args) {
        try {
            // 解析命令行參數
            int fetchSize = args.length > 0 ? 
                Math.min(Integer.parseInt(args[0]), DEFAULT_FETCH_SIZE) : 
                DEFAULT_FETCH_SIZE;

            // 獲取用戶輸入
            Scanner scanner = new Scanner(System.in);
            
            System.out.print("請輸入同步時DB Thread 數量: ");
            int dbThreads = scanner.nextInt();
            
            System.out.print("請輸入同步時Table Thread 數量: ");
            int tableThreads = scanner.nextInt();
            
            System.out.print("請確認是否清空目的DB的表 Y/N: ");
            boolean truncateTarget = scanner.next().equalsIgnoreCase("Y");
            
            System.out.print("請輸入來源DB配置文件路徑: ");
            File sourceDbConfig = new File(scanner.next());
            
            System.out.print("請輸入目的DB配置文件路徑: ");
            File targetDbConfig = new File(scanner.next());
            
            System.out.print("請輸入表清單文件路徑: ");
            String tableListPath = scanner.next();

            // 初始化數據庫配置
            DatabaseConfig sourceDb = new DatabaseConfig(sourceDbConfig);
            DatabaseConfig targetDb = new DatabaseConfig(targetDbConfig);

            // 讀取表清單
            List<String> tables = readTableList(tableListPath);
            logger.info("讀取到 {} 個表需要同步", tables.size());

            // 創建同步服務
            SyncService syncService = new SyncService(
                sourceDb, targetDb, fetchSize, dbThreads, tableThreads, truncateTarget);

            // 開始同步
            long startTime = System.currentTimeMillis();
            logger.info("開始數據同步...");
            
            syncService.syncTables(tables);
            
            long endTime = System.currentTimeMillis();
            logger.info("數據同步完成，總耗時: {} 秒", (endTime - startTime) / 1000);

            // 關閉資源
            syncService.shutdown();
            sourceDb.close();
            targetDb.close();
            scanner.close();

        } catch (DBSyncException e) {
            logger.error("同步過程中發生錯誤: {} - {}", 
                e.getErrorCode().getDescription(), e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.error("發生未預期的錯誤", e);
            System.exit(1);
        }
    }

    private static List<String> readTableList(String path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path))) {
            return reader.lines()
                .filter(line -> !line.trim().isEmpty())
                .map(String::toUpperCase)
                .collect(Collectors.toList());
        }
    }
}