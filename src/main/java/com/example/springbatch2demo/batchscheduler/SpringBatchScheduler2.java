package com.example.springbatch2demo.batchscheduler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableScheduling
public class SpringBatchScheduler2 {

    private final Logger logger = LoggerFactory.getLogger(SpringBatchScheduler2.class);

    private AtomicBoolean enabled = new AtomicBoolean(true);

    private AtomicInteger batchRunCounter = new AtomicInteger(0);

    private final Map<Object, ScheduledFuture<?>> scheduledTasks = new IdentityHashMap<>();

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Value("${file.input}")
    private String fileInput;

//     @Autowired
//     Job importUserJob;

    @Autowired
    @Qualifier("processMovieDataFromS3Job")
    Job processMovieDataFromS3Job;

    @Scheduled(fixedRate = 5000)
    public void launchJob() throws Exception {
        Date date = new Date();
        logger.debug("scheduler starts at " + date);
        if (enabled.get()) {
            logger.debug("Batch job starts on " + date);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("jobName", "processMovieDataFromS3Job")
                    .addDate("launchDate", date)
                    .toJobParameters();
            JobExecution  jobExecution =jobLauncher.run(processMovieDataFromS3Job, jobParameters);
//            JobExecution  jobExecution = jobLauncher.run(processMovieDataFromS3Job, new JobParametersBuilder().addDate("launchDate", date)
//                    .toJobParameters());

//            JobExecution jobExecution = jobLauncher.run(importUserJob(jobRepository,, new JobParametersBuilder().addDate("launchDate", date)
//                    .toJobParameters());
            batchRunCounter.incrementAndGet();
            logger.debug("Batch job ends with status as " + jobExecution.getStatus());
        }
        logger.debug("scheduler ends ");
    }

    public void stop() {
        enabled.set(false);
    }

    public void start() {
        enabled.set(true);
    }

    @Bean
    public TaskScheduler poolScheduler() {
        return new CustomTaskScheduler();
    }

    private class CustomTaskScheduler extends ThreadPoolTaskScheduler {

        private static final long serialVersionUID = -7142624085505040603L;

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
            ScheduledFuture<?> future = super.scheduleAtFixedRate(task, period);

            ScheduledMethodRunnable runnable = (ScheduledMethodRunnable) task;
            scheduledTasks.put(runnable.getTarget(), future);

            return future;
        }

    }

    public void cancelFutureSchedulerTasks() {
        scheduledTasks.forEach((k, v) -> {
            if (k instanceof SpringBatchScheduler2) {
                v.cancel(false);
            }
        });
    }

    public AtomicInteger getBatchRunCounter() {
        return batchRunCounter;
    }


}
