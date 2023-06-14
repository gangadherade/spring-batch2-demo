package com.example.springbatch2demo.batchscheduler.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleAsyncItemWriter<T> implements ItemStreamWriter<Future<T>>, ItemStream, InitializingBean {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private ItemWriter<T> delegate;

//    public void write(List<? extends Future<T>> items) throws Exception {
//        List<T> list = new ArrayList<>();
//        for (Future<T> future : items) {
//            try {
//                T item = future.get();
//
//                if(item != null) {
//                    list.add(future.get());
//                }
//            }
//            catch (ExecutionException e) {
//                Throwable cause = e.getCause();
//
//                if(cause != null && cause instanceof Exception) {
//                    log.info("An exception was thrown while processing an item {}",e);
//
//                    throw (Exception) cause;
//                }
//                else {
//                    throw e;
//                }
//            }
//        }
//
//        delegate.write(list);
//    }
    @Override
    public void write(Chunk<? extends Future<T>> items) throws Exception {
        Chunk<T> list = new Chunk<>();
        for (Future<T> future : items) {
            try {
                T item = future.get();

                if(item != null) {
                    list.add(future.get());
                }
            }
            catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if(cause != null && cause instanceof Exception) {
                    log.info("An exception was thrown while processing an item {}",e);

                    throw (Exception) cause;
                }
                else {
                    throw e;
                }
            }
        }

        delegate.write(list);
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(delegate, "You must set a delegate!");
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).update(executionContext);
        }
    }

    @Override
    public void close() {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).close();
        }
    }

    public void setDelegate(ItemWriter<T> delegate) {
        this.delegate = delegate;
    }


}
