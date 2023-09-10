package com.cch.spark.rdd.sparkSql.jobs;

import java.util.Map;

public class CityRemarkBuffer {
    private Integer total;
    private Map<String, Integer> cityMap;

    public CityRemarkBuffer(Integer total, Map<String, Integer> cityMap) {
        this.total = total;
        this.cityMap = cityMap;
    }

    public CityRemarkBuffer() {
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Map<String, Integer> getCityMap() {
        return cityMap;
    }

    public void setCityMap(Map<String, Integer> cityMap) {
        this.cityMap = cityMap;
    }
}
