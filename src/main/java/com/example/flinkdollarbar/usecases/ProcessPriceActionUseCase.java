package com.example.flinkdollarbar.usecases;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.example.flinkdollarbar.domain.entities.DollarBar;
import com.example.flinkdollarbar.domain.entities.PriceAction;

public class ProcessPriceActionUseCase {

    private final double dollarThreshold;

    public ProcessPriceActionUseCase(double dollarThreshold) {
        this.dollarThreshold = dollarThreshold;
    }

    public DataStream<DollarBar> execute(DataStream<PriceAction> priceActionStream) {
        return priceActionStream
            .keyBy(PriceAction::getUnderlying)
            .process(new DollarBarProcessFunction(dollarThreshold))
            .name("calculate-dollar-bars");
    }
}