package com.example.springbatch2demo.batchscheduler;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AmazonServiceConfig {
    @Value("${aws.access_key}")
    private String accessKey;
    @Value("${aws.secret_key}")
    private String secretKey;

    BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAZSNUWQ4V4RINPCCV","TbZGKV9qxg/Y+CK4Xa+0yhNOClc0iUjtDzn1iVut");
    @Bean
    @Primary
    public AmazonS3 amazonS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
//                .withRegion(Regions.US_WEST_2)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("s3.amazonaws.com", "us-east-1"))
                .build();
    }




}
