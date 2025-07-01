package com.av.debezium.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Consumer;

@Slf4j
public class MySqlDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
    private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;

    private ZoneId timestampZoneId = ZoneId.systemDefault();

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
        readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
    }

   /* Schema mySchema = SchemaBuilder.struct()
            .name("com.example.MySchema")  // Fully qualified name
            .version(1)                // Schema version
            .doc("MY information")   // Documentation
            .field("value", Schema.STRING_SCHEMA)
            .field("type", Schema.STRING_SCHEMA)
            .build();*/

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.length() == 0) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            log.error("The \"{}\" setting is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;

        /*if ("DECIMAL".equals(sqlType)) {
            log.info(String.format(" creating the date time schema  with columen length %s and scale %s " , column.length() , column.scale() ));
            schemaBuilder = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().name("com.example.MyIntegerSchema");
            converter = this::convertStructDecimal;
        }*/

        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.av.debezium.date.string");

            converter = this::convertDate;
        }

        if ("TIME".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.av.debezium.time.string");
            converter = this::convertTime;
        }

        if ("DATETIME".equals(sqlType)) {
            log.info("----- creating the date time schema ");
            schemaBuilder = SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).optional().name("com.example.MySchema");
            converter = this::convertStructDateTime;
        }

        if ("TIMESTAMP".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional().name("com.av.debezium.timestamp.string");
            converter = this::convertTimestamp;
        }



        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
            log.info("register converter for sqlType {} to schema {}", sqlType, schemaBuilder.name());
        }
    }

   /* private HashMap<String, String> convertStructDecimal(Object input) {
        HashMap<String, String> map = new HashMap();
        if (input instanceof Number) {
            map.put("value", String.valueOf(input));
            map.put("type", Number.class.getName());
            return map;
        }
        return null;
    }*/


    private String convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) input);
        }
        return null;
    }

    private HashMap<String, String> convertStructDateTime(Object input) {
        HashMap<String, String> map = new HashMap();
        if (input instanceof LocalDateTime) {
            map.put("date", datetimeFormatter.format((LocalDateTime) input));
            map.put("type", LocalDateTime.class.getName());
            return map;
        }
        return null;
    }

    private String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        return null;
    }

    private HashMap convertStructDate(Object input) {

        HashMap<String, String> map = new HashMap();

        if (input instanceof LocalDate) {
            map.put("date", dateFormatter.format((LocalDate) input));
            map.put("type", dateFormatter.format((LocalDate) input));

            return map;
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            map.put("date", dateFormatter.format(date));
            map.put("type", dateFormatter.format(date));
            return map;
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        return null;
    }



    private String convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
            return timestampFormatter.format(localDateTime);
        }
        return null;
    }

}
