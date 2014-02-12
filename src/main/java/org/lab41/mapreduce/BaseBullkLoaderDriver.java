package org.lab41.mapreduce;

import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static com.thinkaurelius.faunus.FaunusGraph.FAUNUS_GRAPH_OUTPUT_FORMAT;
import static org.lab41.mapreduce.AdditionalConfiguration.*;

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
        String stripped_filename = filename.substring(7);
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

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    /**
     * Will create a tables in HBase.
     *
     * Requires that HBASE_CONF_DIR be set. Does not pull zookeeper qurom from the properties file.
     * @param configuration
     * @throws StorageException
     * @throws IOException
     */
    public void createHbaseTable(Configuration configuration) throws StorageException, IOException {
        //TODO: Figure out how to take full advantage of the  hbase configureation in the props file
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        String tableName = configuration.get("faunus.graph.output.titan.storage.tablename", "titan");

        Boolean presplit = configuration.getBoolean(HBASE_PRESPLIT_KEY,
                HBASE_PRESPLIT_DEFALUT);

        int numsplts = configuration.getInt(HBASE_NUM_SPLITS_KEY,
                HBASE_NUM_SPLITS_DEFAULT);

        if(hBaseAdmin.tableExists(tableName))
        {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            logger.info("deleting Table!");
        }

        if(presplit)
        {
            logger.info("Splitting! "  + numsplts);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

            // two sections of rows:
            // 1.  [00, 00, 00, 00, 00, 00, 00, 00] to [03, 00, 00, 00, 00, 00, 00, 00, 00, 00] (1/2 of regions)
            // 2.  [01, 00, 00, 00, 00, 00, 00, 00] to [FF, FF, FF, FF, FF<, FF, FF, FF, FF, FF]

            byte[] lowStart  = ArrayUtils.EMPTY_BYTE_ARRAY;
            byte[] lowEnd = new byte[]{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
            byte[][] lowsplits = Bytes.split(lowStart, lowEnd, numsplts / 2);
            //remove endpointsj
            lowsplits = Arrays.copyOfRange(lowsplits, 1, lowsplits.length-1);

            byte[]  highStart =new byte[]{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
            byte[]  highEnd = new byte[]{(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff};
            byte[][] highsplits = Bytes.split(highStart, highEnd, numsplts/2);
            highsplits = Arrays.copyOfRange(highsplits, 1, highsplits.length-1);

            byte[][] splits = new byte[numsplts][8];

            int i;
            for (i = 0; i < lowsplits.length; i ++)
            {
               splits[i] = lowsplits[i];
            }

            for (i = 0 ;i < highsplits.length; i++)
            {
                splits[i+lowsplits.length] = highsplits[i];
            }

            //debug loop
            logger.info("Splits : " + splits.length);
            for (int j =0 ; j < splits.length; j++)
            {
                logger.debug("split" + splits[j]);
            }
            hBaseAdmin.createTable(hTableDescriptor,  splits);
        }
    }

    /**
     * Will create a Titan graph provided the faunus configuration file is set correctly.
     * <p/>
     * This function will only accept the HBASE and Cassandra output formats
     *
     * @param configuration
     */
    public Graph createDB(Configuration configuration)
   {
        TitanGraph graph = null;


        logger.info(configuration.get(FAUNUS_GRAPH_OUTPUT_FORMAT));
        logger.info("Creating Graph");
        graph = (TitanGraph) GraphFactory.generateGraph(configuration, TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN);
        graph.makeKey("uuid").dataType(String.class).indexed(Vertex.class).make();
        graph.makeKey("name").dataType(String.class).make();
        graph.makeKey("randLong0").dataType(Double.class).make();
        graph.makeKey("randLong1").dataType(Double.class).make();
        graph.makeKey("randString0").dataType(String.class).make();
        graph.makeKey("randString1").dataType(String.class).make();
        graph.makeKey("randString2").dataType(String.class).make();
        graph.makeLabel("erandLong0").make();
       graph.makeLabel("erandLong1").make();
        graph.makeLabel("erandString0").make();
       graph.makeLabel("erandString1").make();
        graph.makeLabel("erandString2").make();
        graph.commit();
        logger.info("Graph Create done!");

        return graph;
    }

    public int run(String[] args) throws Exception {
        if (parseArgs(args)) {
            Configuration conf = new Configuration();

            return configureGeneratorJob(conf);

        } else {
            System.out.println(USAGE_STRING);
            return 1;
        }
    }

    protected abstract int configureGeneratorJob(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, StorageException;
}
