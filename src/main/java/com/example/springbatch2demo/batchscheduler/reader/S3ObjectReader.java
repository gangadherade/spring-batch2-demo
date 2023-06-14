package com.example.springbatch2demo.batchscheduler.reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.example.springbatch2demo.batchscheduler.model.Movie;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

@Component
public class S3ObjectReader {

    public MultiResourceItemReader<Movie> multiResourceItemReader() throws Exception {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources("s3://bucket-name/path/to/files/*");

        MultiResourceItemReader<Movie> reader = new MultiResourceItemReader<>();
        reader.setResources(resources);
        reader.setDelegate(reader());
        return reader;
    }

    private FlatFileItemReader<Movie> reader() {
        FlatFileItemReader<Movie> reader = new FlatFileItemReader<>();
        reader.setLineMapper(new DefaultLineMapper<>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("name", "genre", "releaseYear", "releasePlatform");
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Movie>() {{
                setTargetType(Movie.class);
            }});
        }});
        return reader;
    }
}