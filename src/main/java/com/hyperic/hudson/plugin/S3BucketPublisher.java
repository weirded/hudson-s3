package com.hyperic.hudson.plugin;

import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import hudson.Extension;
import hudson.FilePath;
import hudson.Launcher;
import hudson.Util;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.BuildListener;
import hudson.model.Result;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.BuildStepMonitor;
import hudson.tasks.Notifier;
import hudson.tasks.Publisher;
import hudson.util.CopyOnWriteList;
import hudson.util.FormValidation;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.kohsuke.stapler.StaplerRequest;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class S3BucketPublisher extends Notifier {

  private String profileName;

  public static final Logger LOGGER = Logger.getLogger(S3BucketPublisher.class.getName());

  private final List<Entry> entries = new ArrayList<Entry>();

  public S3BucketPublisher() {
  }

  public S3BucketPublisher(String profileName) {
    if (profileName == null) {
      // defaults to the first one
      S3Profile[] sites = DESCRIPTOR.getProfiles();
      if (sites.length > 0)
        profileName = sites[0].getName();
    }
    this.profileName = profileName;
  }


  public List<Entry> getEntries() {
    return entries;
  }

  public S3Profile getProfile() {
    S3Profile[] profiles = DESCRIPTOR.getProfiles();
    if (profileName == null && profiles.length > 0)
      // default
      return profiles[0];

    for (S3Profile profile : profiles) {
      if (profile.getName().equals(profileName))
        return profile;
    }
    return null;
  }

  public BuildStepMonitor getRequiredMonitorService() {
    return BuildStepMonitor.BUILD;
  }


  @Override
  public boolean perform(AbstractBuild<?, ?> build,
                         Launcher launcher,
                         BuildListener listener)
    throws InterruptedException, IOException {

    if (build.getResult() == Result.FAILURE) {
      // build failed. don't post
      return true;
    }

    S3Profile profile = getProfile();
    if (profile == null) {
      log(listener.getLogger(), "No S3 profile is configured.");
      build.setResult(Result.UNSTABLE);
      return true;
    }
    log(listener.getLogger(), "Using S3 profile: " + profile.getName());
    try {
      profile.login();
    } catch (RuntimeException e) {
      throw new IOException("Can't connect to S3 service: " + e);
    }

    try {
      Map<String, String> envVars = build.getEnvironment(listener);

      log(listener.getLogger(), "Entries: " + entries);

      TransferManager transferManager = profile.createTransferManager();
      List<Upload> uploads = new ArrayList<Upload>();

      for (Entry entry : entries) {
        String expanded = Util.replaceMacro(entry.sourceFile, envVars);
        FilePath ws = build.getWorkspace();
        FilePath[] paths = ws.list(expanded);

        if (paths.length == 0) {
          // try to do error diagnostics
          log(listener.getLogger(), "No file(s) found: " + expanded);
          String error = ws.validateAntFileMask(expanded);
          if (error != null)
            log(listener.getLogger(), error);
        }
        String bucket = Util.replaceMacro(entry.bucket, envVars);
        profile.ensureBucket(bucket);
        for (FilePath src : paths) {
          File file = new File(src.getRemote());
          log(listener.getLogger(), "bucket=" + bucket + ", file=" + file.getAbsolutePath());
          if (src.isDirectory()) {
            throw new IOException(file.getAbsolutePath() + " is a directory");
          }

          if (!src.exists()) {
            throw new IOException(file.getAbsolutePath() + " does not exist");
          }

          log(listener.getLogger(),
              String.format("START upload of %s to bucket %s (%,d bytes)",
                  src.getName(), bucket, file.length()));

          Upload upload = transferManager.upload(bucket, file.getName(), file);
          uploads.add(upload);
        }
      }

      int unfinishedUploads = uploads.size();
      log(listener.getLogger(), String.format("Waiting for %d S3 uploads to complete.", uploads.size()));
      while(unfinishedUploads > 0) {

        Thread.sleep(500);

        int newCount = 0;
        for (Upload upload : uploads) {
          Transfer.TransferState state = upload.getState();
          if (state == Transfer.TransferState.Canceled) {
            throw new IOException("Upload was cancelled!");
          }

          if (!upload.isDone()) {
            newCount +=1;
          }
        }

        unfinishedUploads = newCount;
      }

      log(listener.getLogger(), String.format("All %d S3 uploads are complete.", uploads.size()));

      transferManager.shutdownNow();

    } catch (IOException e) {
      e.printStackTrace(listener.error("Failed to upload files"));
      build.setResult(Result.UNSTABLE);
    } finally {
      if (profile != null) {
        profile.logout();
      }
    }

    return true;
  }

  @Override
  public BuildStepDescriptor<Publisher> getDescriptor() {
    return DESCRIPTOR;
  }

  @Extension
  public static final DescriptorImpl DESCRIPTOR = new DescriptorImpl();

  public static final class DescriptorImpl extends BuildStepDescriptor<Publisher> {

    public DescriptorImpl() {
      super(S3BucketPublisher.class);
      load();
    }

    protected DescriptorImpl(Class<? extends Publisher> clazz) {
      super(clazz);
    }

    private final CopyOnWriteList<S3Profile> profiles = new CopyOnWriteList<S3Profile>();

    public String getDisplayName() {
      return "Publish artifacts to S3 Bucket";
    }

    public String getShortName() {
      return "[S3] ";
    }

    @Override
    public String getHelpFile() {
      return "/plugin/s3/help.html";
    }

    @Override
    public boolean isApplicable(Class<? extends AbstractProject> jobType) {
      return true;
    }

    @Override
    public Publisher newInstance(StaplerRequest req, JSONObject formData) {
      S3BucketPublisher pub = new S3BucketPublisher();
      req.bindParameters(pub, "s3.");
      pub.getEntries().addAll(req.bindParametersToList(Entry.class, "s3.entry."));
      return pub;
    }

    public S3Profile[] getProfiles() {
      return profiles.toArray(new S3Profile[0]);
    }

    @Override
    public boolean configure(StaplerRequest req, JSONObject formData) {
      profiles.replaceBy(req.bindParametersToList(S3Profile.class, "s3."));
      save();
      return true;
    }


    public FormValidation doLoginCheck(final StaplerRequest request) {
      final String name = Util.fixEmpty(request.getParameter("name"));
      if (name == null) { // name is not entered yet
        return FormValidation.ok();
      }

      S3Profile profile = new S3Profile(name, request.getParameter("accessKey"), request.getParameter("secretKey"));

      try {
        profile.login();
        profile.check();
        profile.logout();
      } catch (RuntimeException e) {
        LOGGER.log(Level.SEVERE, e.getMessage());
        return FormValidation.error("Can't connect to S3 service: " + e.getMessage());
      }

      return FormValidation.ok();
    }


  }

  public String getProfileName() {
    return this.profileName;
  }

  public void setProfileName(String profileName) {
    this.profileName = profileName;
  }

  protected void log(final PrintStream logger, final String message) {
    logger.println(StringUtils.defaultString(DESCRIPTOR.getShortName()) + message);
  }
}
