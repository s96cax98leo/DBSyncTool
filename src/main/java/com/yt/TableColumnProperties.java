
/*
 * Copyright (c) 2024 YAO-TANG WANG. All rights reserved.
 * This software and associated documentation files (the "Software") are protected by copyright law and international treaties. Unauthorized reproduction or distribution of this Software, or any portion of it, may result in severe civil and criminal penalties, and will be prosecuted to the maximum extent possible under law.
 * YAO-TANG WANG reserves all rights not expressly granted to you in this copyright notice.
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.yt;

public class TableColumnProperties {
    String column_name;
    String data_type;
    int data_length;
    int data_precision;
    String nullable;
    int column_id;

    public TableColumnProperties(String column_name, String data_type, int data_length, int data_precision, String nullable, int column_id) {
        this.column_name = column_name;
        this.data_type = data_type;
        this.data_length = data_length;
        this.data_precision = data_precision;
        this.nullable = nullable;
        this.column_id = column_id;
    }

    public String getColumn_name() {
        return column_name;
    }

    public String getData_type() {
        return data_type;
    }

    public int getData_length() {
        return data_length;
    }

    public int getData_precision() {
        return data_precision;
    }

    public String getNullable() {
        return nullable;
    }

    public int getColumn_id() {
        return column_id;
    }
}
