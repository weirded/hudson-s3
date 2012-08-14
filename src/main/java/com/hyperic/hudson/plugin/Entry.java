package com.hyperic.hudson.plugin;

public final class Entry {

  /**
   * Destination bucket for the copy. Can contain macros.
   */
  public String bucket;

  /**
   * File name relative to the workspace root to upload.
   * Can contain macros and wildcards.
   */
  public String sourceFile;

  @Override
  public String toString() {
    return String.format("%s -> %s", sourceFile, bucket);
  }
}
