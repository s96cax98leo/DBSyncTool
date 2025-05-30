# DB 同步工具 (DBSyncTool)

這是一個強大的數據庫同步工具，用於在不同的 Oracle 數據庫實例之間同步數據。

## 功能特點

- 支持大規模數據同步
- 多線程並行處理
- 自動表結構同步
- 智能錯誤處理
- 數據完整性驗證
- 同步進度監控
- 支持斷點續傳
- 詳細的日誌記錄

## 系統要求

- Java 22 或更高版本
- Maven 3.6 或更高版本
- Oracle 數據庫（源和目標數據庫）

## 快速開始

1. 克隆倉庫：
   ```bash
   git clone [repository-url]
   ```

2. 編譯項目：
   ```bash
   mvn clean package
   ```

3. 準備配置文件：
   - 創建源數據庫配置文件（例如：source_db.properties）
   - 創建目標數據庫配置文件（例如：target_db.properties）
   - 創建表清單文件（例如：tablelist.txt）

4. 運行程序：
   ```bash
   java -jar target/DBSyncTool-1.0.jar [fetchSize]
   ```

## 配置文件格式

### 數據庫配置文件 (*.properties)
```properties
url=jdbc:oracle:thin:@//hostname:port/service_name
username=your_username
password=your_password
```

### 表清單文件 (tablelist.txt)
```
TABLE1
TABLE2
TABLE3
```

## 使用說明

1. 啟動程序後，系統會提示輸入：
   - DB 線程數量
   - Table 線程數量
   - 是否清空目標表
   - 源數據庫配置文件路徑
   - 目標數據庫配置文件路徑
   - 表清單文件路徑

2. 程序會自動：
   - 驗證表結構
   - 同步表結構（如果需要）
   - 同步數據
   - 驗證數據完整性

## 注意事項

- 建議在進行同步之前備份目標數據庫
- 確保有足夠的系統資源（CPU、內存、磁盤空間）
- 注意網絡帶寬限制
- 建議在非高峰期進行大規模數據同步

## 錯誤處理

程序會自動處理常見錯誤：
- 表結構不一致
- 網絡連接問題
- 數據類型不匹配
- 主鍵衝突

## 許可證

版權所有 © 2024 YAO-TANG WANG 