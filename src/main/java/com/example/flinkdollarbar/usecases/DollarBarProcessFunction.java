package com.example.flinkdollarbar.usecases;

import com.example.flinkdollarbar.domain.entities.DollarBar;
import com.example.flinkdollarbar.domain.entities.PriceAction;
import com.example.flinkdollarbar.domain.services.DollarBarCalculator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DollarBarProcessFunction extends KeyedProcessFunction<String, PriceAction, DollarBar> {
    private final double dollarThreshold;
    private transient ValueState<DollarBarCalculator> calculatorState;

    public DollarBarProcessFunction(double dollarThreshold) {
        this.dollarThreshold = dollarThreshold;
    }

    public void open(Configuration parameters) {
        ValueStateDescriptor<DollarBarCalculator> descriptor =
            new ValueStateDescriptor<>("dollarBarCalculator", DollarBarCalculator.class);
        calculatorState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(PriceAction priceAction, Context ctx, Collector<DollarBar> out) throws Exception {
        DollarBarCalculator calculator = calculatorState.value();
        if (calculator == null) {
            calculator = new DollarBarCalculator(dollarThreshold);
        }

        DollarBar dollarBar = calculator.addPriceAction(priceAction);
        if (dollarBar != null) {
            out.collect(dollarBar);
        }

        calculatorState.update(calculator);
    }
}