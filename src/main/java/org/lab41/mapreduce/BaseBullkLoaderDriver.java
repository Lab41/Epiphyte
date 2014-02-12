package org.lab41.mapreduce;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;



/**
 * Created by kramachandran on 12/6/13.
 */
public abstract class BaseBullkLoaderDriver extends Configured implements Tool {



    protected static final String USAGE_STRING = "Arguments:  [{file|hdfs}//:path to properties ]";
    protected static final int NUM_ARGS = 1;
    protected String propsPath = null;
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BlueprintsGraphDriver(), args);

        System.exit(exitCode);
    }

    /**
     * Since this class is meant to only deal with bluk loading scenarios. We need to ensure that the following
     * properties are set :
     *
     * <ul>
     *     <li></li>
     * </ul>
     *
     * @param configuration
     */
    protected void ensureRequiedProperties(Configuration configuration)
    {

    }
    protected Properties getProperties(String filename, Configuration conf) throws IOException {
        Properties props = null;
        InputStream is = null;
        logger.info("Getting Properties file: " + filename);
        String stripped_filename = filename.substring(6);
        logger.info("Stripped file name: " + stripped_filename);
        if (filename.startsWith("hdfs://")) {
            FileSystem fs = FileSystem.get(conf);
            is = fs.open(new Path(stripped_filename));
        } else if (filename.startsWith("file://")) {
            File file = new File(stripped_filename);
            is = new FileInputStream(file);

        }

        if (is != null) {
            logger.info("Input Stream is available : " + is.available());
        } else {
            logger.warn("Properties input stream is null ");
        }

        props = new Properties();
        props.load(is);

        return props;
    }

    protected void getAdditionalProperties(Configuration conf, String propertiesFile) throws IOException {
        Properties additionalProperties = getProperties(propertiesFile, conf);
        for (Map.Entry<Object, Object> entry : additionalProperties.entrySet()) {
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }
    }

    protected boolean parseArgs(String[] args) {
        if (args.length > NUM_ARGS) {
            return false;
        } else if (args.length == NUM_ARGS) {
            logger.info("PATH" + args[0]);
            propsPath = args[0];

        }

        return true;
    }


    public int run(String[] args) throws Exception {
        if (parseArgs(args)) {
            Configuration conf = new Configuration();
            return configureAndRunJobs(conf);
        } else {
            System.out.println(USAGE_STRING);
            return 1;
        }
    }

    protected abstract int configureAndRunJobs(Configuration conf)
            throws IOException, ClassNotFoundException, InterruptedException, StorageException;

}
