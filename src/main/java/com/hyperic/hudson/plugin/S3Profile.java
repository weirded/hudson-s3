package com.hyperic.hudson.plugin;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Profile {
  String name;
  String accessKey;
  String secretKey;

  public static final Logger LOGGER =
    Logger.getLogger(S3Profile.class.getName());

  public S3Profile() {

  }

  public S3Profile(String name, String accessKey, String secretKey) {
    this.name = name;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AmazonS3 connect() {
    return new AmazonS3Client(new BasicAWSCredentials(this.accessKey, this.secretKey));
  }

  public void checkAccess() {
    try {
      connect().doesBucketExist("never-never-land");
    } catch (RuntimeException e) {
      LOGGER.log(Level.SEVERE, e.getMessage());
      throw e;
    }
  }

  public void ensureBucket(String bucketName) {
    AmazonS3 s3 = connect();
    if (!s3.doesBucketExist(bucketName)) {
      s3.createBucket(bucketName);
    }
  }

  public TransferManager createTransferManager() {
    return new TransferManager(connect());
  }
}
