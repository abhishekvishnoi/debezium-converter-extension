package com.av.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class MySqlDecimalConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    @Override
    public void configure(Properties properties) {
        String dbType = properties.getProperty("databaseType");
    }

    @Override
    public void converterFor(RelationalColumn relationalColumn, ConverterRegistration<SchemaBuilder> converterRegistration) {

        String sqlType = relationalColumn.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;

        if ("DECIMAL".equals(sqlType)) {
            log.info(String.format("Registering a converter for Decimal Type."));
            log.info(String.format("Creating the date time schema  with column length %s and scale %s ",
                    relationalColumn.length(), relationalColumn.scale()));

            schemaBuilder = SchemaBuilder
                    .map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional()
                    .name("com.example.MyIntegerSchema");

            converter = this::convertStructDecimal;

        }

        if (schemaBuilder != null) {
            converterRegistration.register(schemaBuilder, converter);
            log.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }

    }

    private HashMap<String, String> convertStructDecimal(Object input) {
        HashMap<String, String> map = new HashMap();
        if (input instanceof Number) {
            map.put("value", String.valueOf(input));
            map.put("type", Number.class.getName());
            return map;
        }
        return null;
    }
}

