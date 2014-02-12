package org.lab41.mapreduce;

import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thinkaurelius.faunus.FaunusGraph.FAUNUS_GRAPH_OUTPUT_FORMAT;

/**
 * Created by kramachandran on 12/6/13.
 */
public abstract class BaseBullkLoaderDriver extends Configurable {
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);



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

    /**
     * Will create a Titan graph provided the faunus configuration file is set correctly.
     * <p/>
     * This function will only accept the HBASE and Cassandra output formats
     *
     * @param configuration
     */
    public Graph createDB(Configuration configuration) {
        TitanGraph graph = null;


        logger.info(configuration.get(FAUNUS_GRAPH_OUTPUT_FORMAT));
        logger.info("Creating Graph");
        graph = (TitanGraph) GraphFactory.generateGraph(configuration, TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN);
        graph.makeKey(BlueprintsGraphOutputMapReduce.BLUEPRINTS_ID).dataType(Long.class).indexed(Vertex.class).unique().make();
        graph.makeKey("uuid").dataType(String.class).indexed(Vertex.class).make();
        graph.makeKey("name").dataType(String.class).make();
        graph.makeKey("randLong0").dataType(Long.class).make();
        graph.makeKey("randLong1").dataType(Long.class).make();
        graph.makeKey("randString0").dataType(String.class).make();
        graph.makeKey("randString1").dataType(String.class).make();
        graph.makeKey("randString2").dataType(String.class).make();
        graph.makeLabel("randLong0").make();
        graph.makeLabel("randLong1").make();
        graph.makeLabel("randString0").make();
        graph.makeLabel("randString1").make();
        graph.makeLabel("randString2").make();
        logger.info("Graph Create done!");
        return graph;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (parseArgs(args)) {
            Configuration conf = new Configuration();

            return configureGeneratorJob(conf);

        } else {
            System.out.println(USAGE_STRING);
            return 1;
        }
    }
}
