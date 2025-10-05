package com.example.flinkdollarbar.domain.services;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.example.flinkdollarbar.domain.entities.DollarBar;
import com.example.flinkdollarbar.domain.entities.PriceAction;

public class DollarBarCalculator {
    private final double dollarThreshold;
    private double accumulatedDollarVolume;
    private final List<PriceAction> priceActions;
    private Timestamp startDate;

    public DollarBarCalculator(double dollarThreshold) {
        this.dollarThreshold = dollarThreshold;
        this.accumulatedDollarVolume = 0.0;
        this.priceActions = new ArrayList<>();
        this.startDate = null;
    }

    public DollarBar addPriceAction(PriceAction priceAction) {
        priceActions.add(priceAction);
        if (startDate == null) {
            startDate = priceAction.getDate();
        }

        double dollarValue = priceAction.getClose() * priceAction.getVolume();
        accumulatedDollarVolume += dollarValue;

        if (accumulatedDollarVolume >= dollarThreshold) {
            double open = priceActions.get(0).getOpen();
            double close = priceActions.get(priceActions.size() - 1).getClose();
            double high = priceActions.stream().mapToDouble(PriceAction::getHigh).max().orElse(0.0);
            double low = priceActions.stream().mapToDouble(PriceAction::getLow).min().orElse(0.0);
            double volume = priceActions.stream().mapToDouble(PriceAction::getVolume).sum();
            DollarBar dollarBar = new DollarBar(
                priceAction.getUnderlying(),
                open,
                high,
                low,
                close,
                volume,
                accumulatedDollarVolume,
                startDate,
                priceAction.getDate()
            );

            // Reset state
            accumulatedDollarVolume = 0.0;
            priceActions.clear();
            startDate = null;

            return dollarBar;
        }
        return null;
    }
}