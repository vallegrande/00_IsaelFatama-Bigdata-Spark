package pe.edu.vallegrande.bigdata.api.repository.impl;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import pe.edu.vallegrande.bigdata.api.dto.request.PageQuery;
import pe.edu.vallegrande.bigdata.api.dto.response.PageResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class CsvPageReader {

    private static final char BOM = '﻿';

    private static final Pattern PLAIN_NUMBER = Pattern.compile("-?(0|[1-9]\\d{0,14})(\\.\\d{1,15})?");

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .get();

    private CsvPageReader() {
    }

    static PageResponse<Map<String, Object>> read(Path csv, PageQuery query) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        long total = 0;
        try (BufferedReader reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            skipBom(reader);
            try (CSVParser parser = FORMAT.parse(reader)) {
                List<String> headers = parser.getHeaderNames();
                for (CSVRecord record : parser) {
                    if (total >= query.offset() && rows.size() < query.size()) {
                        rows.add(toRow(headers, record));
                    }
                    total++;
                }
            }
        }
        return PageResponse.of(rows, query, total);
    }

    static Object typed(String value) {
        if (value.isEmpty()) {
            return null;
        }
        if ("true".equals(value) || "false".equals(value)) {
            return Boolean.valueOf(value);
        }
        if (PLAIN_NUMBER.matcher(value).matches()) {
            return value.contains(".") ? new BigDecimal(value) : Long.valueOf(value);
        }
        return value;
    }

    private static void skipBom(BufferedReader reader) throws IOException {
        reader.mark(1);
        if (reader.read() != BOM) {
            reader.reset();
        }
    }

    private static Map<String, Object> toRow(List<String> headers, CSVRecord record) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < headers.size() && index < record.size(); index++) {
            row.put(headers.get(index), typed(record.get(index)));
        }
        return row;
    }

}
