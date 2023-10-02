package com.suitesoftware.dbcopy;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: Oct 2, 2010
 * Time: 1:00:47 AM
 * (c) Copyright Suite Business Software, ALl rights reserverd
 */
public class ColCopyDef {

    public static final int INTEGER_TYPE = 1;
    public static final int STRING_TYPE = 2;
    public static final int DECIMAL_TYPE = 3;

    String name;

    String asName;
    int type;
    boolean sortKey = false;
    int maxLen = -1;

    public ColCopyDef() {}

    public ColCopyDef(String name, int type) {
        this.name = name;
        this.type = type;
    }

    public ColCopyDef(String name, String asName, int type) {
        this.name = name;
        this.asName = asName;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }

    public int getType() {
        return type;
    }

    public void setMaxLen(int maxLen) {
        this.maxLen = maxLen;
    }

    public int getMaxLen() { return maxLen;}

    public void setType(int type) {
        this.type = type;
    }

    public boolean isSortKey() {
        return sortKey;
    }

    public void setSortKey(boolean sortKey) {
        this.sortKey = sortKey;
    }
}
