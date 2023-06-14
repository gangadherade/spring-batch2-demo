package com.example.springbatch2demo.batchscheduler.reader;

import com.example.springbatch2demo.batchscheduler.model.Movie;
import com.example.springbatch2demo.entity.MoviesFromS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class MovieFieldSetMapper implements FieldSetMapper<MoviesFromS3> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public MoviesFromS3 mapFieldSet(FieldSet fieldSet) throws BindException {
        MoviesFromS3 moviesFromS3 = new MoviesFromS3();
        moviesFromS3.setName(fieldSet.readString("name"));
        moviesFromS3.setGenre(fieldSet.readString("genre"));
        moviesFromS3.setReleaseYear(fieldSet.readInt("releaseYear"));
        moviesFromS3.setReleasePlatform(fieldSet.readString("releasePlatform"));
        return moviesFromS3;
    }
}
