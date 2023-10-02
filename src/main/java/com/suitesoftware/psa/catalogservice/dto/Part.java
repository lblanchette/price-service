package com.suitesoftware.psa.catalogservice.dto;



import javax.xml.bind.annotation.*;
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
@XmlAccessorType(XmlAccessType.PROPERTY)

public class Part {

    private int id;

    private String vendor;

    private String manPartNo;

    private String man;

    private String partNo;

    private BigDecimal basePrice;

    private String desc;

    private Double msrp;

    private boolean discontinue;

    private Date lastModified;

    private BigDecimal customerPrice;
    private Date customerPriceLastModified;

    @XmlAttribute
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
        return customerPrice != null ? customerPrice : basePrice;
    }

    public void setPrice(BigDecimal price) {
        this.basePrice = price;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Double getMsrp() {
        return msrp;
    }

    public void setMsrp(Double msrp) {
        this.msrp = msrp;
    }

    public boolean isDiscontinue() {
        return discontinue;
    }

    public void setDiscontinue(boolean discontinue) {
        this.discontinue = discontinue;
    }

    public Date getLastModified() {
        if(customerPriceLastModified != null) {
            if(lastModified != null) {
                return customerPriceLastModified.getTime() > lastModified.getTime() ? customerPriceLastModified : lastModified;
            }
            return customerPriceLastModified;
        }
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    @XmlTransient
    public BigDecimal getCustomerPrice() {
        return customerPrice;
    }
    public void setCustomerPrice(BigDecimal customerPrice) {
        this.customerPrice = customerPrice;
    }

    @XmlTransient
    public BigDecimal getBasePrice() {
        return basePrice;
    }
    public void setBasePrice(BigDecimal price) {
        this.basePrice = price;
    }

    @XmlTransient
    public Date getCustomerPriceLstModified() {
        return customerPriceLastModified;
    }
    public void setCustomerPriceLastModified(Date customerPriceLastModified) {
        this.customerPriceLastModified = customerPriceLastModified;
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
        if (basePrice != null ? !basePrice.equals(part.basePrice) : part.basePrice != null) return true;
        if (vendor != null ? !vendor.equals(part.vendor) : part.vendor != null) return true;
        return false;
    }
}
