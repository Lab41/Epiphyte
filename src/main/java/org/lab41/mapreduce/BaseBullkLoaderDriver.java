package org.lab41.mapreduce;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.Settings;
import org.lab41.hbase.HbaseConfigurator;
import org.lab41.hbase.TitanHbaseSplitter;
import org.lab41.hbase.TitanHbaseThreePartSplitter;
import org.lab41.schema.GraphSchemaWriter;
import org.lab41.schema.KroneckerGraphSchemaWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Properties;



/**
 * Created by kramachandran on 12/6/13.
 */
public abstract class BaseBullkLoaderDriver extends Configured implements Tool {
    protected long GB = 1073741824; //number of bytes in a GB
    protected long MB = 1048576;

    protected static final String USAGE_STRING = "Arguments:  [{file|hdfs}//:path to properties ], [{file|hdfs|://path to system properties], [{file|hdfs}://path to hbase-site.xml}]";
    protected static final int NUM_ARGS = 3;
    protected String propsPath = null;
    protected String sysPath = null;
    protected String hbaseSiteXml = null;
    Logger logger = LoggerFactory.getLogger(BaseBullkLoaderDriver.class);



    protected Properties getProperties(String filename, Configuration conf) throws IOException {
        Properties props = null;
        InputStream is = getInputStreamForPath(filename, conf);

        if (is != null) {
            logger.info("Input Stream is available for " + filename +": " + is.available());
        } else {
            logger.warn("Properties input stream is null ");
        }

        props = new Properties();
        props.load(is);

        return props;
    }

    protected InputStream getInputStreamForPath(String filename, Configuration conf) throws IOException {

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
        return is;
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
            sysPath = args[1];
            hbaseSiteXml = args[2];
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
            throws IOException, ClassNotFoundException, InterruptedException, StorageException,
            InstantiationException, ClassNotFoundException, IllegalAccessException;

    protected void configureHbase(Configuration baseConfiguration, InputStream hbaseConf)
            throws IOException, StorageException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        logger.info("Hbase Conf available : " + hbaseConf.available());
        baseConfiguration.addResource(hbaseConf);

        String strSplitterClazz = baseConfiguration.get(Settings.SPLITTER_CLASS_KEY, Settings.SPLITTER_CLASS_DEFUALT);
        Class  splitterClazz = Class.forName(strSplitterClazz);
        TitanHbaseSplitter splitter = (TitanHbaseSplitter)splitterClazz.newInstance();

        logger.info("Made splitter + " + splitter.getClass().getCanonicalName());
        HbaseConfigurator hBaseConfigurator = new HbaseConfigurator(splitter);
        hBaseConfigurator.createHbaseTable(baseConfiguration);

        //TODO: Use reflection to set schemaWrite as a configuration option
        GraphSchemaWriter graphSchemaWriter = new KroneckerGraphSchemaWriter();
        graphSchemaWriter.writeSchema(baseConfiguration);
    }
}
