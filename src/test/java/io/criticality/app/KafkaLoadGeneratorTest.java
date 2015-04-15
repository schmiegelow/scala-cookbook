package io.criticality.app;

import io.criticality.cookbook.scala.kafka.KafkaProducer;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertFalse;

import io.criticality.app.value.ExchangeRate;

/**
 * Created by e.schmiegelow on 14/04/15.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/applicationContext.xml")
public class KafkaLoadGeneratorTest {


    @BeforeClass
    public static void init() {
        System.setProperty("broker", "localhost:9092");
    }


    @Autowired
        KafkaLoadGenerator loadGenerator;
        @Autowired
        @Qualifier("recordProducer")
        KafkaProducer producer;

        @Test
        public void testProcess() {
            try {
                File file = new File(getClass().getResource("/exchange.csv").toURI());
                List<ExchangeRate> records = loadGenerator.process(file);
                for (int i = 0; i < records.size(); i++) {
                    ExchangeRate record = records.get(i);
                    System.out.println(i + ":" + record);
                    producer.send(record.toString(), "1");
                }
                assertFalse(records.isEmpty());
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
    }
