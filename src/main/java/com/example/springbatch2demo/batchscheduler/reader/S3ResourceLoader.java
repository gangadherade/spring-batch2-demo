package com.example.springbatch2demo.batchscheduler.reader;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
@Component
public class S3ResourceLoader {
    @Autowired
    private final AmazonS3 s3Client;

    public S3ResourceLoader(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public List<Resource> loadResources(String bucketName, String prefix) {
        List<Resource> resourceList = new ArrayList<>();
        List<S3ObjectSummary> objectSummaries = s3Client.listObjects(bucketName, prefix).getObjectSummaries();
        for (S3ObjectSummary objectSummary : objectSummaries) {
            String key = objectSummary.getKey();
            InputStreamResource inputStreamResource = new InputStreamResource(s3Client.getObject(bucketName, key).getObjectContent());
            resourceList.add(inputStreamResource);
        }
        return resourceList;
    }
}