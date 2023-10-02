package com.suitesoftware.psa.catalogservice.dto;

import java.math.BigDecimal;

public class PartPrice {

    Integer id;
    BigDecimal price;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }
}
