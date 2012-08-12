package com.hyperic.hudson.plugin;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Profile {
  String name;
  String accessKey;
  String secretKey;
  private AmazonS3 s3;

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

  public void login() {
    if (this.s3 != null) {
      return;
    }
    try {
      AWSCredentials creds = new BasicAWSCredentials(this.accessKey, this.secretKey);
      this.s3 = new AmazonS3Client(creds);
      this.s3.listBuckets();
    } catch (RuntimeException e) {
      LOGGER.log(Level.SEVERE, e.getMessage());
      throw e;
    }
  }

  public void check() {
    this.s3.listBuckets();
  }

  public void logout() {
    this.s3 = null;
  }

  public void ensureBucket(String bucketName) {
    List<Bucket> buckets = this.s3.listBuckets();
    for (Bucket bucket : buckets) {
      if (bucket.getName().equals(bucketName)) {
        return;
      }
    }

    this.s3.createBucket(bucketName);
  }

  public TransferManager createTransferManager() {
    AWSCredentials creds = new BasicAWSCredentials(this.accessKey, this.secretKey);
    return new TransferManager(creds);
  }
}
