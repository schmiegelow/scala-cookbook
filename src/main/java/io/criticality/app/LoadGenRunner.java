package io.criticality.app;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

/**
 * Created by e.schmiegelow on 14/04/15.
 */
public class LoadGenRunner {
    private static final Logger LOG = LoggerFactory.getLogger(LoadGenRunner.class);

    private Options options = new Options();

    private String file;
    private int repeat;
    private int delay;

    private KafkaLoadGenerator loadGenerator;

    public LoadGenRunner() {
        Option optFile = new Option("f", true, "file to process");
        Option optRep = new Option("r", true, "amount of repeat loops");
        Option optDel = new Option("d", true, "delays between batches");

        optFile.setRequired(true);
        options.addOption(optFile);
        optRep.setRequired(true);
        options.addOption(optRep);
        optDel.setRequired(true);
        options.addOption(optDel);

        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("/applicationContext.xml");
        loadGenerator = ctx.getBean(KafkaLoadGenerator.class);
    }

    public void ingest() {

        try {

            loadGenerator.process(new File(file));
            if (delay > 0)
                Thread.sleep(delay);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }


    public static void main(String[] args) {
        LoadGenRunner service = new LoadGenRunner();
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(service.options, args);
            service.setValues(cmd);
            for (int i = 0; i < service.repeat; i++) {
                long start = System.currentTimeMillis();
                service.ingest();
                System.err.println(String.format("Pass | %s | %s ms", i, System.currentTimeMillis() - start));
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
    }
}