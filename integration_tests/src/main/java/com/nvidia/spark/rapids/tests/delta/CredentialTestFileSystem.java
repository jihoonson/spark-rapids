/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests.delta;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Test-only S3 filesystem backed by local disk.
 *
 * <p>The OSS Unity Catalog connector wraps this filesystem and supplies credentials in the Hadoop
 * configuration associated with a catalog table. Every filesystem access validates those
 * credentials before mapping the S3 path to local storage. This makes a path-only DeltaLog fail
 * while a catalog-aware DeltaLog succeeds.
 */
public class CredentialTestFileSystem extends RawLocalFileSystem {
  private static final String SCHEME = "s3:";
  private static final String EXPECTED_BUCKET = "test-bucket0";
  private static final String EXPECTED_ACCESS_KEY = "accessKey0";
  private static final String EXPECTED_SECRET_KEY = "secretKey0";
  private static final String EXPECTED_SESSION_TOKEN = "sessionToken0";
  private static final String UC_VENDED_TOKEN_PROVIDER =
      "io.unitycatalog.spark.auth.storage.AwsVendedTokenProvider";

  // Same key as org.apache.hadoop.fs.s3a.Constants#AWS_CREDENTIALS_PROVIDER. Keep the test helper
  // independent of hadoop-aws and the AWS SDK so it can live in the regular integration-test jar.
  private static final String S3A_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";

  private Object provider;

  @Override
  protected void checkPath(Path path) {
    // Accept the synthetic s3 scheme even though RawLocalFileSystem normally accepts file paths.
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return super.create(
        toLocalPath(path), overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (!path.toString().startsWith(SCHEME)) {
      return super.getFileStatus(path);
    }
    Path localPath = toLocalPath(path);
    try {
      return restoreS3Path(path, super.getFileStatus(localPath));
    } catch (FileNotFoundException e) {
      // Delta's LogStore checks that _delta_log exists before listing it. S3 has no real
      // directories, so expose the not-yet-materialized log prefix as an empty directory.
      if ("_delta_log".equals(path.getName())) {
        return new FileStatus(0, true, 1, getDefaultBlockSize(path), 0, path);
      }
      throw e;
    }
  }

  @Override
  public FSDataInputStream open(Path path) throws IOException {
    return super.open(toLocalPath(path));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FileStatus[] statuses;
    try {
      statuses = super.listStatus(toLocalPath(path));
    } catch (FileNotFoundException e) {
      // Object stores return an empty listing for a missing prefix.
      return new FileStatus[0];
    }
    FileStatus[] restored = new FileStatus[statuses.length];
    for (int index = 0; index < statuses.length; index++) {
      restored[index] = restoreS3Path(path, statuses[index]);
    }
    return restored;
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    return super.mkdirs(toLocalPath(path), permission);
  }

  @Override
  public boolean rename(Path source, Path destination) throws IOException {
    return super.rename(toLocalPath(source), toLocalPath(destination));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    return super.delete(toLocalPath(path), recursive);
  }

  private Path toLocalPath(Path path) {
    checkCredentials(path);
    return new Path(path.toString().replaceAll(SCHEME + "//.*?/", "file:///"));
  }

  private FileStatus restoreS3Path(Path originalPath, FileStatus status) {
    String s3Prefix = SCHEME + "//" + originalPath.toUri().getHost();
    String restoredPath = status.getPath().toString().replace("file:", s3Prefix);
    return new FileStatus(
        status.getLen(),
        status.isDirectory(),
        status.getReplication(),
        status.getBlockSize(),
        status.getModificationTime(),
        new Path(restoredPath));
  }

  private void checkCredentials(Path path) {
    assertEquals(EXPECTED_BUCKET, path.toUri().getHost(), "S3 bucket");
    Configuration conf = getConf();
    Object credentialsProvider = resolveProvider(conf);
    if (credentialsProvider == null) {
      assertEquals(EXPECTED_ACCESS_KEY, conf.get("fs.s3a.access.key"), "access key");
      assertEquals(EXPECTED_SECRET_KEY, conf.get("fs.s3a.secret.key"), "secret key");
      assertEquals(EXPECTED_SESSION_TOKEN, conf.get("fs.s3a.session.token"), "session token");
      return;
    }

    try {
      Object credentials = credentialsProvider.getClass()
          .getMethod("resolveCredentials")
          .invoke(credentialsProvider);
      assertCredential(credentials, "accessKeyId", EXPECTED_ACCESS_KEY);
      assertCredential(credentials, "secretAccessKey", EXPECTED_SECRET_KEY);
      assertCredential(credentials, "sessionToken", EXPECTED_SESSION_TOKEN);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to validate Unity Catalog credentials", e);
    }
  }

  private void assertCredential(Object credentials, String methodName, String expected)
      throws ReflectiveOperationException {
    Method method = credentials.getClass().getMethod(methodName);
    assertEquals(expected, method.invoke(credentials), methodName);
  }

  private synchronized Object resolveProvider(Configuration conf) {
    if (provider != null) {
      return provider;
    }
    String className = conf.get(S3A_CREDENTIALS_PROVIDER);
    if (!UC_VENDED_TOKEN_PROVIDER.equals(className)) {
      return null;
    }
    try {
      provider = Class.forName(className).getConstructor(Configuration.class).newInstance(conf);
      return provider;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to instantiate credential provider " + className, e);
    }
  }

  private void assertEquals(String expected, Object actual, String fieldName) {
    if (!expected.equals(actual)) {
      throw new AssertionError(
          "Unexpected " + fieldName + ": expected " + expected + ", found " + actual);
    }
  }
}
