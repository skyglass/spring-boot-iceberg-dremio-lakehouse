package com.example.flinkdollarbar.domain.entities;

import java.sql.Timestamp;

public class DollarBar {
    private final String underlying;
    private final double open;
    private final double high;
    private final double low;
    private final double close;
    private final double volume;
    private final double dollarVolume;
    private final Timestamp startDate;
    private final Timestamp endDate;

    public DollarBar(String underlying, double open, double high, double low, double close,
                     double volume, double dollarVolume, Timestamp startDate, Timestamp endDate) {
        this.underlying = underlying;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
        this.dollarVolume = dollarVolume;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    // Getters
    public String getUnderlying() { return underlying; }
    public double getOpen() { return open; }
    public double getHigh() { return high; }
    public double getLow() { return low; }
    public double getClose() { return close; }
    public double getVolume() { return volume; }
    public double getDollarVolume() { return dollarVolume; }
    public Timestamp getStartDate() { return startDate; }
    public Timestamp getEndDate() { return endDate; }
}