package com.suitesoftware.dbcopy;

/**
 * User: lrb
 * Date: 9/24/16
 * Time: 8:29 PM
 * (c) Copyright Suite Business Software
 */
public class OutParameter<E> {

    private E ref;

    public OutParameter() {
    }

    public E get() {
        return ref;
    }

    public void set(E e) {
        this.ref = e;
    }

    public String toString() {
        return ref.toString();
    }
}