package com.example.flinkdollarbar.domain.entities;

import java.sql.Timestamp;

public class PriceAction {
    private final double low;
    private final double high;
    private final double open;
    private final double close;
    private final double volume;
    private final String underlying;
    private final Timestamp date;

    // Constructor, getters, setters
    public PriceAction(double low, double high, double open, double close, double volume, String underlying, Timestamp date) {
        this.low = low;
        this.high = high;
        this.open = open;
        this.close = close;
        this.volume = volume;
        this.underlying = underlying;
        this.date = date;
    }

    // Getters
    public double getLow() { return low; }
    public double getHigh() { return high; }
    public double getOpen() { return open; }
    public double getClose() { return close; }
    public double getVolume() { return volume; }
    public String getUnderlying() { return underlying; }
    public Timestamp getDate() { return date; }
}