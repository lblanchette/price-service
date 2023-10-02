package com.suitesoftware.dbcopy;

/**
 * Created by IntelliJ IDEA.
 * User: Administrator
 * Date: Oct 2, 2010
 * Time: 12:54:30 AM
 * (c) Copyright Suite Business Software, ALl rights reserverd
 */
public class CopyDef {

    String srcTable;
    String dstTable;
    ColCopyDef [] columns;
    String prepSql;
    String selectSql;
    String whereClause;
    String orderByClause;

    DdlDef ddlDef;

    boolean useTemp = true;

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public boolean isUseTemp() {
        return useTemp;
    }

    public void setUseTemp(boolean useTemp) {
        this.useTemp = useTemp;
    }

    public String getDstTable() {
        return dstTable;
    }

    public void setDstTable(String dstTable) {
        this.dstTable = dstTable;
    }

    public String getPrepSql() {
        return prepSql;
    }

    public void setPrepSql(String prepSql) {
        this.prepSql = prepSql;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    public DdlDef getDdlDef() {
        return ddlDef;
    }

    public void setDdlDef(DdlDef ddlDef) {
        this.ddlDef = ddlDef;
    }

    public ColCopyDef[] getColumns() {
        return columns;
    }

    public void setColumns(ColCopyDef[] columns) {
        this.columns = columns;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String [] getDstColumnNames() {
        String [] asColumns = new String[columns.length];
        for(int colCount = 0; colCount < columns.length; colCount++) {
            ColCopyDef ccd = columns[colCount];
            asColumns[colCount] = ccd.getAsName() != null ? ccd.getAsName() : ccd.getName();
        }
        return asColumns;
    }
}
