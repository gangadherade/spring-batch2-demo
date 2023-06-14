package com.example.springbatch2demo.batchscheduler.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class SimpleJobExecutionListener implements JobExecutionListener {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Before - movie dataProcess job...");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        log.info("After - movie dataProcess job...");
    }
}
