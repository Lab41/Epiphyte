package org.lab41.hbase;

import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.lab41.mapreduce.BlueprintsGraphDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.lab41.mapreduce.AdditionalConfiguration.*;
import static com.thinkaurelius.faunus.FaunusGraph.FAUNUS_GRAPH_OUTPUT_FORMAT;
/**
 * This class sets up HBase for the test by :
 * 1. Creating the appropriate tables
 * 2. Creating splitters
 * Created by kramachandran (karkumar)
 */
public class HbaseConfigurator {
    TitanHbasePresplitter titanHbasePresplitter;
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);

    public void HbaseConfigurator(TitanHbasePresplitter titanHbasePresplitter)
    {
       this.titanHbasePresplitter = titanHbasePresplitter;
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
}
