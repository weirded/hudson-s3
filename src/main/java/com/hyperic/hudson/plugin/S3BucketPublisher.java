package com.hyperic.hudson.plugin;

import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.BinaryUtils;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class S3BucketPublisher extends Notifier {

    private String profileName;

    private static final int retries = 5;
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
            Map<Upload, UploadInfo> uploadInfoMap = new HashMap<Upload, UploadInfo>();

            log(listener.getLogger(), "Entries: " + entries);

            TransferManager transferManager = profile.createTransferManager();
            List<Upload> uploads = new ArrayList<Upload>(entries.size());

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
                    log(listener.getLogger(), "bucket=" + bucket + ", file=" + src.getName());

                    if (src.isDirectory()) {
                        throw new IOException(src.getRemote() + " is a directory");
                    }

                    if (!src.exists()) {
                        throw new IOException(src.getRemote() + " does not exist");
                    }

                    if (src.length() < 1) {
                        throw new IOException(src.getRemote() + " is 0 length!");
                    }

                    log(listener.getLogger(),
                            String.format("Calculating MD5 for %s (%,d bytes)",
                                    src.getName(), src.length()));

                    ObjectMetadata meta = new ObjectMetadata();
                    meta.setContentLength(src.length());
                    meta.setLastModified(new Date());
                    meta.setContentEncoding(Mimetypes.MIMETYPE_OCTET_STREAM);

                    // This uses Hudson's remote mechanism to calculate the MD5 of the file.
                    meta.setContentMD5(BinaryUtils.toBase64(Util.fromHexString(src.digest())));

                    log(listener.getLogger(),
                            String.format("Uploading %s to bucket %s (%,d bytes)",
                                    src.getName(), bucket, src.length()));

                    Upload upload = transferManager.upload(bucket, src.getName(), src.read(), meta);
                    uploadInfoMap.put(upload, new UploadInfo(bucket, src.getName(), src.read(), meta));
                    uploads.add(upload);
                }
            }

            log(listener.getLogger(), String.format("Waiting for %d S3 uploads to complete.", uploads.size()));
            for (Upload upload : uploads) {
                int tries = 1;
                while (tries <= retries) {
                    try {
                        upload.waitForUploadResult();
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, String.format("Unable to upload file: %s", upload.getDescription()), e);
                        UploadInfo uploadData = uploadInfoMap.get(upload);
                        upload = transferManager.upload(uploadData.getBucket(), uploadData.getName(),
                                uploadData.getRead(), uploadData.getMeta());
                        tries++;
                    }
                    log(listener.getLogger(), String.format("Upload result for %s was %s",
                            upload.getDescription(), upload.getState().name()));
                }
            }

            transferManager.shutdownNow();
            log(listener.getLogger(), String.format("All %d S3 uploads are complete.", uploads.size()));

        } catch (IOException e) {
            e.printStackTrace(listener.error("Failed to upload files"));
            build.setResult(Result.UNSTABLE);
        } finally {
            if (profile != null) {
                profile.logout();
            }
        }

          if (!src.exists()) {
            throw new IOException(src.getRemote() + " does not exist");
          }

          if (src.isDirectory()) {
            throw new IOException(src.getRemote() + " is a directory");
          }

          if (src.length() < 1) {
            throw new IOException(src.getRemote() + " is 0 length!");
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

    private class UploadInfo {
        private final String bucket;
        private final String name;
        private final InputStream read;
        private final ObjectMetadata meta;

        public UploadInfo(String bucket, String name, InputStream read, ObjectMetadata meta) {
            this.bucket = bucket;
            this.name = name;
            this.read = read;
            this.meta = meta;
        }

        public String getBucket() {
            return bucket;
        }

        public String getName() {
            return name;
        }

        public InputStream getRead() {
            return read;
        }

        public ObjectMetadata getMeta() {
            return meta;
        }
    }
}
