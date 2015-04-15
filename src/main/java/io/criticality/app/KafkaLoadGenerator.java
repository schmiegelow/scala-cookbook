package io.criticality.app;

import io.criticality.cookbook.scala.kafka.KafkaProducer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import io.criticality.app.value.ExchangeRate;

/**
 * Created by e.schmiegelow on 14/04/15.
 */
@Service
public class KafkaLoadGenerator {

    private Logger LOG = LoggerFactory.getLogger(KafkaLoadGenerator.class);

    @Autowired
    @Qualifier("recordProducer")
    KafkaProducer producer;

    public List<ExchangeRate> process(File file) {
        List<ExchangeRate> results = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));

            String line = reader.readLine();
            while (line != null) {
                ExchangeRate rate = parseLine(line);
                if (rate != null)
                    results.add(rate);
                line = reader.readLine();
            }
            reader.close();
        }
        catch (IOException e) {

        }
        return results;

    }

    public void dispatch(List<ExchangeRate> records) {
        for (int i = 0; i < records.size(); i++) {
            ExchangeRate record = records.get(i);
            producer.send(record.toString(), "1");
        }
        LOG.debug(String.format("dispatched %s records", records.size()));
    }

    public ExchangeRate parseLine(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        if (tokenizer.hasMoreTokens()) {
            return new ExchangeRate(tokenizer.nextToken(), Float.valueOf(tokenizer.nextToken()));
        }
        return null;
    }

}
