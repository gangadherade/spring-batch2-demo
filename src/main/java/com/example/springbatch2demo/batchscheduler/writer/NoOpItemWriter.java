package com.example.springbatch2demo.batchscheduler.writer;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class NoOpItemWriter implements ItemWriter {
//    @Override
    public void write(List items) throws Exception {

    }

    @Override
    public void write(Chunk chunk) throws Exception {
        // This is No Operation item writer.
    }

}
