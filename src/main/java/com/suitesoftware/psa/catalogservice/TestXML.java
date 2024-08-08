package com.suitesoftware.psa.catalogservice;

import com.suitesoftware.psa.catalogservice.dto.Catalog;

import jakarta.xml.bind.JAXB;
import java.io.File;

public class TestXML {

    public void x() {

        Catalog c = new Catalog();
        //JAXB.marshal(c,new File("Test.xml"))
        Catalog cu = (Catalog) JAXB.unmarshal(new File("x.xml"),Catalog.class);
        JAXB.marshal(cu,new File("Test.xml"));

        System.out.println("Here");
    }
    public static void main(String [] args) {
        try {
            TestXML t = new TestXML();
            t.x();
        } catch (Throwable x) {
            x.printStackTrace();
        }
    }
}
