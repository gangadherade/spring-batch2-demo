package com.example.springbatch2demo.batchscheduler.reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import org.springframework.core.io.AbstractResource;

import java.io.IOException;
import java.io.InputStream;

public class S3Resource extends AbstractResource {
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String key;

    private final String filename;

    public S3Resource(AmazonS3 s3Client, String bucketName, String key) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        this.filename = key;
    }

    @Override
    public String getDescription() {
        return "S3 bucket: " + bucketName + ", key: " + key;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        S3Object s3Object = s3Client.getObject(bucketName, key);
        return s3Object.getObjectContent();
    }
}