package com.suitesoftware.dbcopy.bulk;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 2:56 PM
 * (c) Copyright Suite Business Software
 */
public class BulkCopyFailedException extends RuntimeException {
    public BulkCopyFailedException(String message) {
        super(message);
    }

    public BulkCopyFailedException() {
    }

    public BulkCopyFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public BulkCopyFailedException(Throwable cause) {
        super(cause);
    }

    public BulkCopyFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
