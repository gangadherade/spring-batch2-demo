package com.example.springbatch2demo.batchscheduler.processor;


import com.example.springbatch2demo.batchscheduler.model.Movie;
import com.example.springbatch2demo.entity.MoviesFromS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class MovieDataProcessor implements ItemProcessor<MoviesFromS3, MoviesFromS3> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Override
    public MoviesFromS3 process(MoviesFromS3 item) throws Exception {
        log.info("Processing movie data {}",item);
        return item;
    }
}
