package com.suitesoftware.psa.catalogservice;



import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.math.BigDecimal;
import java.util.Date;

/**
 * User: lrb
 * Date: 2/17/11
 * Time: 12:41 PM
 * (c) Copyright Suite Business Software
 */

@XmlType(namespace="http://com.psasecurity")
@XmlRootElement(name="part")
@XmlAccessorType(XmlAccessType.FIELD)

public class Part {

    @XmlAttribute
    private int id;

    @XmlElement(required=false, nillable = true)
    private String vendor;

    @XmlElement(required=false, nillable = true)
    private String manPartNo;

    @XmlElement(required=false, nillable = true)
    private String man;

    @XmlElement(required=true, nillable = false)
    private String partNo;

    @XmlElement(required=true, nillable = false)
    private BigDecimal price;

    @XmlElement(required=false, nillable = true)
    private String desc;

    @XmlElement(required=false, nillable = true)
    private BigDecimal msrp;

    @XmlElement(required=false, nillable = true)
    private boolean discontinue;

    @XmlElement(required=true, nillable = false)
    private Date lastModified;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getManPartNo() {
        return manPartNo;
    }

    public void setManPartNo(String manPartNo) {
        this.manPartNo = manPartNo;
    }

    public String getMan() {
        return man;
    }

    public void setMan(String man) {
        this.man = man;
    }

    public String getPartNo() {
        return partNo;
    }

    public void setPartNo(String partNo) {
        this.partNo = partNo;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public BigDecimal getMsrp() {
        return msrp;
    }

    public void setMsrp(BigDecimal msrp) {
        this.msrp = msrp;
    }

    public boolean isDiscontinue() {
        return discontinue;
    }

    public void setDiscontinue(boolean discontinue) {
        this.discontinue = discontinue;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public boolean changed(Part part) {
        if (this == part) return false;
        if (discontinue != part.discontinue) return true;
        if (id != part.id) return true;
        if (desc != null ? !desc.equals(part.desc) : part.desc != null) return true;
        if (man != null ? !man.equals(part.man) : part.man != null) return true;
        if (manPartNo != null ? !manPartNo.equals(part.manPartNo) : part.manPartNo != null) return true;
        if (msrp != null ? !msrp.equals(part.msrp) : part.msrp != null) return true;
        if (partNo != null ? !partNo.equals(part.partNo) : part.partNo != null) return true;
        if (price != null ? !price.equals(part.price) : part.price != null) return true;
        if (vendor != null ? !vendor.equals(part.vendor) : part.vendor != null) return true;
        return false;
    }

}
