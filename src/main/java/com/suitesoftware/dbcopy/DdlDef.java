package com.suitesoftware.dbcopy;

/**
 * User: lrb
 * Date: 10/1/16
 * Time: 8:31 AM
 * (c) Copyright Suite Business Software
 */
public class DdlDef {
//    String tableName;
    String createDdl;
    String primaryKey;
    String [] indexes;

//    public String getTableName() {
//        return tableName;
//    }
//
//    public void setTableName(String tableName) {
//        this.tableName = tableName;
//    }

    public String getCreateDdl() {
        return createDdl;
    }

    public void setCreateDdl(String createDdl) {
        this.createDdl = createDdl;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String[] getIndexes() {
        return indexes;
    }

    public void setIndexes(String[] indexes) {
        this.indexes = indexes;
    }
}
