package io.criticality.app;

import io.criticality.app.value.ExchangeRate;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * Created by e.schmiegelow on 14/04/15.
 */
public class LoadGenRunner {
    private static final Logger LOG = LoggerFactory.getLogger(LoadGenRunner.class);

    private Options options = new Options();

    private String file;
    private int repeat;
    private int delay;
    private int threads;

    private KafkaLoadGenerator loadGenerator;

    public LoadGenRunner() {
        Option optFile = new Option("f", true, "file to process");
        Option optRep = new Option("r", true, "amount of repeat loops");
        Option optDel = new Option("d", true, "delays between batches");
        Option optThreads = new Option("t", true, "amount of threads");
        optFile.setRequired(true);
        options.addOption(optFile);
        optRep.setRequired(true);
        options.addOption(optRep);
        optDel.setRequired(true);
        options.addOption(optDel);
        optThreads.setRequired(true);
        options.addOption(optThreads);

        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");
        loadGenerator = ctx.getBean(KafkaLoadGenerator.class);
    }

    public List<ExchangeRate> ingest() {

        try {

            return loadGenerator.process(new File(file));

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return Collections.emptyList();
        }

    }

    public void dispatch(List<ExchangeRate> rates) throws Exception {
        loadGenerator.dispatch(rates);
        if (delay > 0)
            Thread.sleep(delay);
    }


    public static void main(String[] args) {
        final LoadGenRunner service = new LoadGenRunner();
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(service.options, args);
            service.setValues(cmd);
            final List<ExchangeRate> rates = service.ingest();
            for (int a = 0; a < service.threads; a++) {
                final int threadNum = a;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < service.repeat; i++) {
                            try {
                                long start = System.currentTimeMillis();
                                service.dispatch(rates);
                                System.err.println(String.format("Thread %s | Pass | %s | %s ms", threadNum, i, System.currentTimeMillis() - start));

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }

                }).start();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("LoadGenRunner", service.options);
        }

    }

    private void setValues(CommandLine cmd) {
        file = cmd.getOptionValue("f");
        repeat = Integer.valueOf(cmd.getOptionValue("r"));
        delay = Integer.valueOf(cmd.getOptionValue("d"));
        threads = Integer.valueOf(cmd.getOptionValue("t"));
    }
}