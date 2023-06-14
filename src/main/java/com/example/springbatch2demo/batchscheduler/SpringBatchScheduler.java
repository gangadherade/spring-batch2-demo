package com.example.springbatch2demo.batchscheduler;
import com.example.springbatch2demo.batchscheduler.reader.S3Resource;
import com.example.springbatch2demo.batchscheduler.reader.S3ResourceLoader;
import org.apache.commons.io.FileUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.example.springbatch2demo.batch.Coffee;
import com.example.springbatch2demo.batch.CoffeeItemProcessor;
import com.example.springbatch2demo.batchscheduler.listener.SimpleJobExecutionListener;
import com.example.springbatch2demo.batchscheduler.listener.SimpleStepExecutionListener;
import com.example.springbatch2demo.batchscheduler.model.Movie;
import com.example.springbatch2demo.batchscheduler.processor.MovieDataProcessor;
import com.example.springbatch2demo.batchscheduler.reader.MovieFieldSetMapper;
import com.example.springbatch2demo.batchscheduler.util.BatchUtil;
import com.example.springbatch2demo.batchscheduler.writer.NoOpItemWriter;
import com.example.springbatch2demo.batchscheduler.writer.SimpleAsyncItemWriter;
import com.example.springbatch2demo.entity.MoviesFromS3;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.*;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.ResourceUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Configuration

public class SpringBatchScheduler {

    private final Logger logger = LoggerFactory.getLogger(SpringBatchScheduler.class);

    private AtomicBoolean enabled = new AtomicBoolean(true);

    private AtomicInteger batchRunCounter = new AtomicInteger(0);

    private final Map<Object, ScheduledFuture<?>> scheduledTasks = new IdentityHashMap<>();

//    @Autowired
//    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Value("${file.input}")
    private String fileInput;

    @Autowired
    private JobCompletionNotificationListener listener;

    @Autowired
    private ResourceLoader resourceLoader;

    @Value("${batch.chunk.size}")
    private int chunkSize;

    @Value("${batch.threadPool.size}")
    private int threadPoolSize;

    @Value("${rawdata.s3bucket}")
    private String rawDataS3Bucket;

    @Value("${rawdata.s3object.prefix}")
    private String rawDataS3ObjectPrefix;

    @Value("${inputData.fileExtension}")
    private String inputDataFileExtension;

    @Autowired
    private AmazonS3 amazonS3Client;

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @PersistenceContext
    private EntityManager entityManager;


//    @Bean
//    public Job job(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
//        return new JobBuilder("job", jobRepository)
//                .start(readBooks(jobRepository, transactionManager))
//                .build();
//    }

//    @Bean
//    protected Step readBooks(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
//        return new StepBuilder("readBooks", jobRepository)
//                .<Book, Book> chunk(2, transactionManager)
//                .reader(reader2())
//                .writer(writer2())
//                .build();
//    }

//    @Bean
//    public FlatFileItemReader<Book> reader2() {
//        return new FlatFileItemReaderBuilder<Book>().name("bookItemReader")
//                .resource(new ClassPathResource("books.csv"))
//                .delimited()
//                .names(new String[] { "id", "name" })
//                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {
//                    {
//                        setTargetType(Book.class);
//                    }
//                })
//                .build();
//    }

//    @Bean
//    public ItemWriter<Book> writer2() {
//        return items -> {
//            logger.debug("writer..." + items.size());
//            for (Book item : items) {
//                logger.debug(item.toString());
//            }
//        };
//    }

    public AtomicInteger getBatchRunCounter() {
        return batchRunCounter;
    }

    @Bean
    public FlatFileItemReader<Coffee> reader() {
        return new FlatFileItemReaderBuilder<Coffee>().name("coffeeItemReader")
            .resource(new ClassPathResource(fileInput))
            .delimited()
            .names(new String[] { "brand", "origin", "characteristics" })
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Coffee>() {{
                setTargetType(Coffee.class);
             }})
            .build();
    }

    @Bean
    public CoffeeItemProcessor processor() {
        return new CoffeeItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Coffee> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Coffee>().itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO coffee (brand, origin, characteristics) VALUES (:brand, :origin, :characteristics)")
            .dataSource(dataSource)
            .build();
    }


  // schedule this job importUserJob every 5 minutes in a jobLauncher
//    @Bean(name = "importUserJob")
//    public Job importUserJob(JobRepository jobRepository, Step step1) {
//        return new JobBuilder("importUserJob", jobRepository)
//            .incrementer(new RunIdIncrementer())
//            .listener(listener)
//            .flow(step1)
//            .end()
//            .build();
//    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Coffee> writer) {
        return new StepBuilder("step1", jobRepository)
                .<Coffee, Coffee> chunk(10, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager, JdbcBatchItemWriter<Coffee> writer) {
        return new StepBuilder("step", jobRepository)
                .<Coffee, Coffee> chunk(10, transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean(name = "processMovieDataFromS3Job")
    @Transactional
    public Job processMovieDataFromS3Job(JobRepository jobRepository,PlatformTransactionManager transactionManager ) throws Exception {
        FlowBuilder<Flow> dataProcessFlowBuilder = new FlowBuilder<Flow>("dataProcess-flow");
        Flow dataProcessFlow = dataProcessFlowBuilder
                .start(movieDataProcessStep(jobRepository,transactionManager))
                .on(ExitStatus.COMPLETED.getExitCode()).end()
                .from(movieDataProcessStep(jobRepository,transactionManager))
                .on(BatchUtil.ANY_OTHER_EXIT_STATUS).fail()
                .end();

        return new JobBuilder("movieDataProcessJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(movieJobExecutionListener())
                .start(dataProcessFlow)
                .end()
                .build();
    }
    @Bean
    public Step movieDataProcessStep(JobRepository jobRepository,PlatformTransactionManager transactionManager ) throws Exception {


        return new StepBuilder("dataProcess-step", jobRepository)
                .<MoviesFromS3, MoviesFromS3> chunk(chunkSize, transactionManager)
                .listener(movieStepExecutionListener())
//                .reader(movieDataReader())
                .reader(movieDataReader())
                .processor(asyncMovieDataProcessor())
                .writer(asyncMovieItemWriter())
                .build();

    }
// new step with moviedataReader


    @Bean(destroyMethod="")
    @StepScope
    public SynchronizedItemStreamReader<MoviesFromS3> movieDataReader() throws IOException {
        SynchronizedItemStreamReader synchronizedItemStreamReader = new SynchronizedItemStreamReader();
        S3ResourceLoader s3ResourceLoader = new S3ResourceLoader(amazonS3Client);

        List<Resource> resourceList = s3ResourceLoader.loadResources(rawDataS3Bucket,rawDataS3ObjectPrefix);
        Resource[] resources = resourceList.toArray(new Resource[resourceList.size()]);
        MultiResourceItemReader<MoviesFromS3> multiResourceItemReader = new MultiResourceItemReader<>();
        multiResourceItemReader.setName("movie-multiResource-Reader");
        multiResourceItemReader.setResources(resources);
        multiResourceItemReader.setDelegate(movieFileItemReader());
        synchronizedItemStreamReader.setDelegate(multiResourceItemReader);
        return synchronizedItemStreamReader;
    }
    /** Movie data process Configuration - Start */

    // READER
    /*@Bean(destroyMethod="")
    @StepScope
    public SynchronizedItemStreamReader<MoviesFromS3> movieDataReader() throws IOException {
        SynchronizedItemStreamReader synchronizedItemStreamReader = new SynchronizedItemStreamReader();
        List<Resource> resourceList = new ArrayList<>();
        String sourceBucket = rawDataS3Bucket;
        String sourceObjectPrefix = rawDataS3ObjectPrefix
//                .concat("MOVIES")
                .concat(BatchUtil.FORWARD_SLASH);
        logger.info("sourceObjectPrefix::"+sourceObjectPrefix);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(sourceBucket)
                .withPrefix(sourceObjectPrefix);
        ObjectListing sourceObjectsListing;
        do{
            sourceObjectsListing = amazonS3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary sourceFile : sourceObjectsListing.getObjectSummaries()){

                if(!(sourceFile.getSize() > 0)
                        || (!sourceFile.getKey().endsWith(BatchUtil.DOT.concat(inputDataFileExtension)))
                ){
                    // Skip if file is empty (or) file extension is not "csv"
                    continue;
                }
                logger.info("Reading "+sourceFile.getKey());
//                resourceList.add(new S3Resource("s3://bucket-name/path/to/file1.csv")));
                S3Object s3object = amazonS3Client.getObject(new GetObjectRequest(sourceFile.getBucketName(), sourceFile.getKey()));
                InputStream inputStream = s3object.getObjectContent();
//                byte[] bytes = ;
                File file=new File(sourceFile.getKey().split("/")[1]);
                if (!file.exists()) {
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                FileCopyUtils.copy(IOUtils.toByteArray(inputStream),file);
                resourceList.add(new FileSystemResource(file));

//                S3Resource' is abstract; cannot be instantiatedresourceLoader.getResource(BatchUtil.S3_PROTOCOL_PREFIX.concat(sourceBucket).concat(BatchUtil.FORWARD_SLASH)
//                        .concat(sourceFile.getKey())));
            }
            listObjectsRequest.setMarker(sourceObjectsListing.getNextMarker());
        }while(sourceObjectsListing.isTruncated());

        Resource[] resources = resourceList.toArray(new Resource[resourceList.size()]);
        MultiResourceItemReader<MoviesFromS3> multiResourceItemReader = new MultiResourceItemReader<>();
        multiResourceItemReader.setName("movie-multiResource-Reader");
        multiResourceItemReader.setResources(resources);
        multiResourceItemReader.setDelegate(movieFileItemReader());
        synchronizedItemStreamReader.setDelegate(multiResourceItemReader);
        return synchronizedItemStreamReader;
    }*/
    /*@Bean
    public MultiResourceItemReader<Movie> multiResourceItemReader() {

        List<S3ObjectSummary> objectSummaries = amazonS3Client.listObjects(rawDataS3Bucket, rawDataS3ObjectPrefix).getObjectSummaries();
        List<Resource> resources = objectSummaries.stream()
                .map(s3ObjectSummary -> new S3Resource(amazonS3Client, s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()))
                .collect(Collectors.toList());
        String newFilename = "new_filename.txt"; // set the new filename

        for (Resource resource : resources) {
            try {
                File file = ResourceUtils.getFile(resource);
                File newFile = new File(file.getParent(), newFilename);
                if (file.renameTo(newFile)) {
                    System.out.println("File renamed successfully");
                } else {
                    System.out.println("Failed to rename file");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new MultiResourceItemReaderBuilder<Movie>()
                .name("multiResourceItemReader")
                .resources(resources.toArray(new Resource[0]))
                .delegate(movieFileItemReader())
                .build();
    }*/
    @Bean
    @StepScope
    public FlatFileItemReader<MoviesFromS3> movieFileItemReader()
    {
        FlatFileItemReader<MoviesFromS3> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
//        DefaultLineMapper<Movie> movieDataLineMapper = new DefaultLineMapper();
//        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//        tokenizer.setNames(new String[] {
//                "name","genre","releaseYear","releasePlatform"
//        });
//        movieDataLineMapper.setFieldSetMapper(movieFieldSetMapper());
//        movieDataLineMapper.setLineTokenizer(tokenizer);
//        reader.setLineMapper(movieDataLineMapper);
//        reader.setRecordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
        reader.setLineMapper(new DefaultLineMapper<MoviesFromS3>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] {"name", "genre", "releaseYear", "releasePlatform"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<MoviesFromS3>() {{
                setTargetType(MoviesFromS3.class);
            }});
        }});
        return reader;
    }

    @Bean
    public FieldSetMapper<MoviesFromS3> movieFieldSetMapper(){
        return new MovieFieldSetMapper();
    }

    // PROCESSOR
    // ASYNC ITEM PROCESSOR
    @Bean
    @StepScope
    public ItemProcessor asyncMovieDataProcessor(){
        AsyncItemProcessor<MoviesFromS3, MoviesFromS3> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(movieDataProcessor());
        asyncItemProcessor.setTaskExecutor(asyncMovieTaskExecutor());
        return asyncItemProcessor;
    }

    // DELEGATE ITEM PROCESSOR
    @Bean
    @StepScope
    public ItemProcessor<MoviesFromS3, MoviesFromS3> movieDataProcessor() {
        MovieDataProcessor movieDataProcessor = new MovieDataProcessor();
        return movieDataProcessor;
    }

    // TASK EXECUTOR

    @Bean
    public TaskExecutor asyncMovieTaskExecutor()
    {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threadPoolSize);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("movieExec-");
        return executor;
    }

    // WRITER
    @Bean
    @StepScope
    public ItemWriter asyncMovieItemWriter() throws Exception{
        SimpleAsyncItemWriter<MoviesFromS3> simpleAsyncItemWriter = new SimpleAsyncItemWriter<>();
        simpleAsyncItemWriter.setDelegate(myDataWriter());
        return simpleAsyncItemWriter;
    }

    @Bean
    @StepScope
    public ItemWriter movieNoOpItemWriter() {
        return new NoOpItemWriter();
    }

    // Step Listener
    @Bean
    public SimpleStepExecutionListener movieStepExecutionListener(){
        SimpleStepExecutionListener stepExecutionListener = new SimpleStepExecutionListener();
        return stepExecutionListener;
    }

    // Job Listener
    @Bean
    public JobExecutionListener movieJobExecutionListener() {
        return new SimpleJobExecutionListener();
    }

    @Bean
    public ItemWriter<MoviesFromS3> myDataWriter() {
        JpaItemWriter<MoviesFromS3> writer = new JpaItemWriter<MoviesFromS3>();
        writer.setEntityManagerFactory(entityManagerFactory);
        return writer;
    }

}
