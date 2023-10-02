package com.suitesoftware.dbcopy.bulk;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 11:09 AM
 * (c) Copyright Suite Business Software
 */

public class PgTextWriter implements AutoCloseable {
    private transient DataOutputStream buffer;


    public PgTextWriter() {
    }

    public void open(OutputStream out) {
        this.buffer = new DataOutputStream(new BufferedOutputStream(out));
        writeHeader();
    }

    private void writeHeader() {
    }

    public void startRow(int numColumns) {
        try {
            this.buffer.writeShort(numColumns);
        } catch (Exception e) {
            throw new BulkCopyFailedException(e);
        }
    }

    public void write(String value) {
        try {
            //System.out.println("'" + value + "'");
            buffer.write(value.getBytes("utf-8"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void close() {
        try {
            this.buffer.flush();
            this.buffer.close();
        } catch (Exception e) {
            throw new BulkCopyFailedException(e);
        }
    }
}
