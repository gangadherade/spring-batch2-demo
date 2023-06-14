package com.example.springbatch2demo.batchscheduler.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class SimpleStepExecutionListener implements StepExecutionListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Before Movie data-process step...");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info("After Movie data-process step...");
        return null;
    }

}
