/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.kudu.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import javax.security.auth.Subject;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.ColumnRangePredicate;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;

/**
 * Utility class to setup MR jobs that use Kudu as an input and/or output.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduTableMapReduceUtil {
  // Mostly lifted from HBase's TableMapReduceUtil

  private static final Log LOG = LogFactory.getLog(KuduTableMapReduceUtil.class);

  /**
   * "Secret key alias" used in Job Credentials to store the Kudu authentication
   * credentials. This acts as a key in a Hadoop Credentials object.
   */
  private static final Text AUTHN_CREDENTIALS_ALIAS = new Text("kudu.authn.credentials");

  private static final Text KUDU_TOKEN_KIND = new Text("kudu-authn-data");

  /**
   * Doesn't need instantiation
   */
  private KuduTableMapReduceUtil() { }


  /**
   * Base class for MR I/O formats, contains the common configurations.
   */
  private abstract static class AbstractMapReduceConfigurator<S> {
    protected final Job job;
    protected final String table;

    protected boolean addDependencies = true;

    /**
     * Constructor for the required fields to configure.
     * @param job a job to configure
     * @param table a string that contains the name of the table to read from
     */
    private AbstractMapReduceConfigurator(Job job, String table) {
      this.job = job;
      this.table = table;
    }

    /**
     * Sets whether this job should add Kudu's dependencies to the distributed cache. Turned on
     * by default.
     * @param addDependencies a boolean that says if we should add the dependencies
     * @return this instance
     */
    @SuppressWarnings("unchecked")
    public S addDependencies(boolean addDependencies) {
      this.addDependencies = addDependencies;
      return (S) this;
    }

    /**
     * Add credentials to the job so that tasks run as the user that submitted
     * the job.
     */
    protected void addCredentialsToJob(String masterAddresses, long operationTimeoutMs)
        throws KuduException {
      try (KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses)
          .defaultOperationTimeoutMs(operationTimeoutMs)
          .build()) {
        KuduTableMapReduceUtil.addCredentialsToJob(client, job);
      }
    }

    /**
     * Configures the job using the passed parameters.
     * @throws IOException If addDependencies is enabled and a problem is encountered reading
     * files on the filesystem
     */
    public abstract void configure() throws IOException;
  }

  /**
   * Builder-like class that sets up the required configurations and classes to write to Kudu.
   * <p>
   * Use either child classes when configuring the table output format.
   */
  private abstract static class AbstractTableOutputFormatConfigurator
      <S extends AbstractTableOutputFormatConfigurator<? super S>>
      extends AbstractMapReduceConfigurator<S> {

    protected String masterAddresses;
    protected long operationTimeoutMs = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;

    /**
     * {@inheritDoc}
     */
    private AbstractTableOutputFormatConfigurator(Job job, String table) {
      super(job, table);
    }

    /**
     * {@inheritDoc}
     */
    public void configure() throws IOException {
      job.setOutputFormatClass(KuduTableOutputFormat.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Operation.class);

      Configuration conf = job.getConfiguration();
      conf.set(KuduMapReduceConstants.MASTER_ADDRESSES_KEY, masterAddresses);
      conf.set(KuduMapReduceConstants.OUTPUT_TABLE_KEY, table);
      conf.setLong(KuduMapReduceConstants.OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs);
      if (addDependencies) {
        addDependencyJars(job);
      }
      addCredentialsToJob(masterAddresses, operationTimeoutMs);
    }
  }

  /**
   * Builder-like class that sets up the required configurations and classes to read from Kudu.
   * By default, block caching is disabled.
   * <p>
   * Use either child classes when configuring the table input format.
   */
  private abstract static class AbstractTableInputFormatConfigurator
      <S extends AbstractTableInputFormatConfigurator<? super S>>
      extends AbstractMapReduceConfigurator<S> {

    protected String masterAddresses;
    protected long operationTimeoutMs = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;
    protected final String columnProjection;
    protected boolean cacheBlocks;
    protected boolean isFaultTolerant;
    protected List<KuduPredicate> predicates = new ArrayList<>();

    /**
     * Constructor for the required fields to configure.
     * @param job a job to configure
     * @param table a string that contains the name of the table to read from
     * @param columnProjection a string containing a comma-separated list of columns to read.
     *                         It can be null in which case we read empty rows
     */
    private AbstractTableInputFormatConfigurator(Job job, String table, String columnProjection) {
      super(job, table);
      this.columnProjection = columnProjection;
    }

    /**
     * Sets the block caching configuration for the scanners. Turned off by default.
     * @param cacheBlocks whether the job should use scanners that cache blocks
     * @return this instance
     */
    public S cacheBlocks(boolean cacheBlocks) {
      this.cacheBlocks = cacheBlocks;
      return (S) this;
    }

    /**
     * Sets the fault tolerance configuration for the scanners. Turned off by default.
     * @param isFaultTolerant whether the job should use fault tolerant scanners
     * @return this instance
     */
    public S isFaultTolerant(boolean isFaultTolerant) {
      this.isFaultTolerant = isFaultTolerant;
      return (S) this;
    }

    /**
     * Configures the job with all the passed parameters.
     * @throws IOException If addDependencies is enabled and a problem is encountered reading
     * files on the filesystem
     */
    public void configure() throws IOException {
      job.setInputFormatClass(KuduTableInputFormat.class);

      Configuration conf = job.getConfiguration();

      conf.set(KuduMapReduceConstants.MASTER_ADDRESSES_KEY, masterAddresses);
      conf.set(KuduMapReduceConstants.INPUT_TABLE_KEY, table);
      conf.setLong(KuduMapReduceConstants.OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs);
      conf.setBoolean(KuduMapReduceConstants.SCAN_CACHE_BLOCKS, cacheBlocks);
      conf.setBoolean(KuduMapReduceConstants.FAULT_TOLERANT_SCAN, isFaultTolerant);

      if (columnProjection != null) {
        conf.set(KuduMapReduceConstants.COLUMN_PROJECTION_KEY, columnProjection);
      }

      conf.set(KuduMapReduceConstants.ENCODED_PREDICATES_KEY, base64EncodePredicates(predicates));

      if (addDependencies) {
        addDependencyJars(job);
      }

      addCredentialsToJob(masterAddresses, operationTimeoutMs);
    }
  }

  /**
   * Returns the provided predicates as a Base64 encoded string.
   * @param predicates the predicates to encode
   * @return the encoded predicates
   */
  static String base64EncodePredicates(List<KuduPredicate> predicates) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (KuduPredicate predicate : predicates) {
      predicate.toPB().writeDelimitedTo(baos);
    }
    return Base64.encodeBase64String(baos.toByteArray());
  }


  /**
   * Table output format configurator to use to specify the parameters directly.
   */
  public static class TableOutputFormatConfigurator
      extends AbstractTableOutputFormatConfigurator<TableOutputFormatConfigurator> {

    /**
     * Constructor for the required fields to configure.
     * @param job a job to configure
     * @param table a string that contains the name of the table to read from
     * @param masterAddresses a comma-separated list of masters' hosts and ports
     */
    public TableOutputFormatConfigurator(Job job, String table, String masterAddresses) {
      super(job, table);
      this.masterAddresses = masterAddresses;
    }

    /**
     * Sets the timeout for all the operations. The default is 10 seconds.
     * @param operationTimeoutMs a long that represents the timeout for operations to complete,
     *                           must be a positive value or 0
     * @return this instance
     * @throws IllegalArgumentException if the operation timeout is lower than 0
     */
    public TableOutputFormatConfigurator operationTimeoutMs(long operationTimeoutMs) {
      if (operationTimeoutMs < 0) {
        throw new IllegalArgumentException("The operation timeout must be => 0, " +
            "passed value is: " + operationTimeoutMs);
      }
      this.operationTimeoutMs = operationTimeoutMs;
      return this;
    }
  }

  /**
   * Table output format that uses a {@link CommandLineParser} in order to set the
   * master config and the operation timeout.
   */
  public static class TableOutputFormatConfiguratorWithCommandLineParser extends
      AbstractTableOutputFormatConfigurator<TableOutputFormatConfiguratorWithCommandLineParser> {

    /**
     * {@inheritDoc}
     */
    public TableOutputFormatConfiguratorWithCommandLineParser(Job job, String table) {
      super(job, table);
      CommandLineParser parser = new CommandLineParser(job.getConfiguration());
      this.masterAddresses = parser.getMasterAddresses();
      this.operationTimeoutMs = parser.getOperationTimeoutMs();
    }
  }

  /**
   * Table input format configurator to use to specify the parameters directly.
   */
  public static class TableInputFormatConfigurator
      extends AbstractTableInputFormatConfigurator<TableInputFormatConfigurator> {

    /**
     * Constructor for the required fields to configure.
     * @param job a job to configure
     * @param table a string that contains the name of the table to read from
     * @param columnProjection a string containing a comma-separated list of columns to read.
     *                         It can be null in which case we read empty rows
     * @param masterAddresses a comma-separated list of masters' hosts and ports
     */
    public TableInputFormatConfigurator(Job job, String table, String columnProjection,
                                        String masterAddresses) {
      super(job, table, columnProjection);
      this.masterAddresses = masterAddresses;
    }

    /**
     * Sets the timeout for all the operations. The default is 10 seconds.
     * @param operationTimeoutMs a long that represents the timeout for operations to complete,
     *                           must be a positive value or 0
     * @return this instance
     * @throws IllegalArgumentException if the operation timeout is lower than 0
     */
    public TableInputFormatConfigurator operationTimeoutMs(long operationTimeoutMs) {
      if (operationTimeoutMs < 0) {
        throw new IllegalArgumentException("The operation timeout must be => 0, " +
            "passed value is: " + operationTimeoutMs);
      }
      this.operationTimeoutMs = operationTimeoutMs;
      return this;
    }

    /**
     * Adds a new predicate that will be pushed down to all the tablets.
     * @param predicate a predicate to add
     * @return this instance
     * @deprecated use {@link #addPredicate}
     */
    @Deprecated
    public TableInputFormatConfigurator addColumnRangePredicate(ColumnRangePredicate predicate) {
      return addPredicate(predicate.toKuduPredicate());
    }

    /**
     * Adds a new predicate that will be pushed down to all the tablets.
     * @param predicate a predicate to add
     * @return this instance
     */
    public TableInputFormatConfigurator addPredicate(KuduPredicate predicate) {
      this.predicates.add(predicate);
      return this;
    }
  }

  /**
   * Table input format that uses a {@link CommandLineParser} in order to set the
   * master config and the operation timeout.
   * This version cannot set column range predicates.
   */
  public static class TableInputFormatConfiguratorWithCommandLineParser extends
      AbstractTableInputFormatConfigurator<TableInputFormatConfiguratorWithCommandLineParser> {

    /**
     * {@inheritDoc}
     */
    public TableInputFormatConfiguratorWithCommandLineParser(Job job,
                                                             String table,
                                                             String columnProjection) {
      super(job, table, columnProjection);
      CommandLineParser parser = new CommandLineParser(job.getConfiguration());
      this.masterAddresses = parser.getMasterAddresses();
      this.operationTimeoutMs = parser.getOperationTimeoutMs();
    }
  }

  /**
   * Use this method when setting up a task to get access to the KuduTable in order to create
   * Inserts, Updates, and Deletes.
   * @param context Map context
   * @return The kudu table object as setup by the output format
   */
  @SuppressWarnings("rawtypes")
  public static KuduTable getTableFromContext(TaskInputOutputContext context) {
    String multitonKey = context.getConfiguration().get(KuduMapReduceConstants.MULTITON_KEY);
    return KuduTableOutputFormat.getKuduTable(multitonKey);
  }

  /**
   * Export the credentials from a {@link KuduClient} and store them in the given MapReduce
   * {@link Job} so that {@link KuduClient}s created from within tasks of that job can
   * authenticate to Kudu.
   *
   * This must be used before submitting a job when running against a Kudu cluster
   * configured to require authentication. If using {@link TableInputFormatConfigurator},
   * {@link TableOutputFormatConfigurator} or another such utility class, this is called
   * automatically and does not need to be called.
   *
   * @param client the client whose credentials to export
   * @param job the job to configure
   * @throws KuduException if credentials cannot be exported
   */
  public static void addCredentialsToJob(KuduClient client, Job job)
      throws KuduException {
    Preconditions.checkNotNull(client);
    Preconditions.checkNotNull(job);

    byte[] authnCreds = client.exportAuthenticationCredentials();
    Text service = new Text(client.getMasterAddressesAsString());
    job.getCredentials().addToken(AUTHN_CREDENTIALS_ALIAS,
        new Token<TokenIdentifier>(null, authnCreds, KUDU_TOKEN_KIND, service));
  }

  /**
   * Import credentials from the current thread's JAAS {@link Subject} into the provided
   * {@link KuduClient}.
   *
   * This must be called for any clients created within a MapReduce job in order to
   * adopt the credentials added by {@link #addCredentialsToJob(KuduClient, Job)}.
   * When using {@link KuduTableInputFormat} or {@link KuduTableOutputFormat}, the
   * implementation automatically handles creating the client and importing necessary
   * credentials. As such, this is only necessary in jobs that explicitly create a
   * {@link KuduClient}.
   *
   * If no appropriate credentials are found, does nothing.
   */
  public static void importCredentialsFromCurrentSubject(KuduClient client) {
    Subject subj = Subject.getSubject(AccessController.getContext());
    if (subj == null) {
      return;
    }
    Text service = new Text(client.getMasterAddressesAsString());
    // Find the Hadoop credentials stored within the JAAS subject.
    Set<Credentials> credSet = subj.getPrivateCredentials(Credentials.class);
    if (credSet == null) {
      return;
    }
    for (Credentials creds : credSet) {
      for (Token<?> tok : creds.getAllTokens()) {
        if (!tok.getKind().equals(KUDU_TOKEN_KIND)) {
          continue;
        }
        // Only import credentials relevant to the service corresponding to
        // 'client'. This is necessary if we want to support a job which
        // reads from one cluster and writes to another.
        if (!tok.getService().equals(service)) {
          LOG.debug("Not importing credentials for service " + service +
              "(expecting service " + service + ")");
          continue;
        }
        LOG.debug("Importing credentials for service " + service);
        client.importAuthenticationCredentials(tok.getPassword());
        return;
      }
    }
  }

  /**
   * Add the Kudu dependency jars as well as jars for any of the configured
   * job classes to the job configuration, so that JobClient will ship them
   * to the cluster and add them to the DistributedCache.
   */
  public static void addDependencyJars(Job job) throws IOException {
    addKuduDependencyJars(job.getConfiguration());
    try {
      addDependencyJars(job.getConfiguration(),
          // when making changes here, consider also mapred.TableMapReduceUtil
          // pull job classes
          job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(),
          job.getInputFormatClass(),
          job.getOutputKeyClass(),
          job.getOutputValueClass(),
          job.getOutputFormatClass(),
          job.getPartitionerClass(),
          job.getCombinerClass());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Add the jars containing the given classes to the job's configuration
   * such that JobClient will ship them to the cluster and add them to
   * the DistributedCache.
   */
  public static void addDependencyJars(Configuration conf,
                                       Class<?>... classes) throws IOException {

    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // add jars as we find them to a map of contents jar name so that we can avoid
    // creating new jars for classes that have already been packaged.
    Map<String, String> packagedClasses = new HashMap<String, String>();

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) {
        continue;
      }

      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
      if (path == null) {
        LOG.warn("Could not find jar for class " + clazz +
            " in order to ship it to the cluster.");
        continue;
      }
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path + " for class " + clazz);
        continue;
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) {
      return;
    }

    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  /**
   * Add Kudu and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to external tools that
   * need to build a MapReduce job that interacts with Kudu but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   * @param conf The Configuration object to extend with dependencies.
   * @see KuduTableMapReduceUtil
   * @see <a href="https://issues.apache.org/jira/browse/PIG-3285">PIG-3285</a>
   */
  public static void addKuduDependencyJars(Configuration conf) throws IOException {
    addDependencyJars(conf,
        // explicitly pull a class from each module
        Operation.class,                      // kudu-client
        KuduTableMapReduceUtil.class,   // kudu-mapreduce
        // pull necessary dependencies
        com.stumbleupon.async.Deferred.class);
  }

  /**
   * Finds the Jar for a class or creates it if it doesn't exist. If the class
   * is in a directory in the classpath, it creates a Jar on the fly with the
   * contents of the directory and returns the path to that Jar. If a Jar is
   * created, it is created in the system temporary directory. Otherwise,
   * returns an existing jar that contains a class of the same name. Maintains
   * a mapping from jar contents to the tmp jar created.
   * @param myClass the class to find.
   * @param fs the FileSystem with which to qualify the returned path.
   * @param packagedClasses a map of class name to path.
   * @return a jar file that contains the class.
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  private static Path findOrCreateJar(Class<?> myClass, FileSystem fs,
                                      Map<String, String> packagedClasses)
      throws IOException {
    // attempt to locate an existing jar for the class.
    String jar = findContainingJar(myClass, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = JarFinder.getJar(myClass);
      updateMap(jar, packagedClasses);
    }

    if (null == jar || jar.isEmpty()) {
      return null;
    }

    LOG.debug(String.format("For class %s, using jar %s", myClass.getName(), jar));
    return new Path(jar).makeQualified(fs);
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return
   * a jar file, even if that is not the first thing on the class path that
   * has a class with the same name. Looks first on the classpath and then in
   * the <code>packagedClasses</code> map.
   * @param myClass the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> myClass, Map<String, String> packagedClasses)
      throws IOException {
    ClassLoader loader = myClass.getClassLoader();
    String classFile = myClass.getName().replaceAll("\\.", "/") + ".class";

    // first search the classpath
    for (Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }

    // now look in any jars we've packaged using JarFinder. Returns null when
    // no jar is found.
    return packagedClasses.get(classFile);
  }

  /**
   * Add entries to <code>packagedClasses</code> corresponding to class files
   * contained in <code>jar</code>.
   * @param jar The jar who's content to list.
   * @param packagedClasses map[class -> jar]
   */
  private static void updateMap(String jar, Map<String, String> packagedClasses)
      throws IOException {
    if (null == jar || jar.isEmpty()) {
      return;
    }
    ZipFile zip = null;
    try {
      zip = new ZipFile(jar);
      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
        ZipEntry entry = iter.nextElement();
        if (entry.getName().endsWith("class")) {
          packagedClasses.put(entry.getName(), jar);
        }
      }
    } finally {
      if (null != zip) {
        zip.close();
      }
    }
  }
}
