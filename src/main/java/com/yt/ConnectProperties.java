/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectProperties {
    String url;
    String owner;
    String password;

    ConnectProperties(File file) throws IOException {
        Properties properties = new Properties();
        properties.load(new BufferedInputStream(new FileInputStream(file)));
        this.url = properties.getProperty("url");
        this.owner = properties.getProperty("owner");
        this.password = properties.getProperty("password");
    }

    public  String getDBConnDesc() {
        return this.getUrl().substring(this.getUrl().lastIndexOf("/") + 1) + "_" + this.getOwner();
    }

    public String getUrl() {
        return url;
    }

    public String getOwner() {
        return owner;
    }

    public String getPassword() {
        return password;
    }
}
