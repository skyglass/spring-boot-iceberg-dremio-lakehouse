package com.example.flinkdollarbar.infrastructure.kafka;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.example.flinkdollarbar.domain.entities.PriceAction;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PriceActionDeserializer implements DeserializationSchema<PriceAction> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public PriceAction deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, PriceAction.class);
    }

    @Override
    public boolean isEndOfStream(PriceAction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PriceAction> getProducedType() {
        return TypeInformation.of(PriceAction.class);
    }
}