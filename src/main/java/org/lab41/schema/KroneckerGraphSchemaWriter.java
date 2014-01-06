package org.lab41.schema;

import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.lab41.mapreduce.BlueprintsGraphDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thinkaurelius.faunus.FaunusGraph.FAUNUS_GRAPH_OUTPUT_FORMAT;

/**
 * Created by kramachandran (karkumar)
 */
public class KroneckerGraphSchemaWriter implements GraphSchemaWriter{
    Logger logger = LoggerFactory.getLogger(KroneckerGraphSchemaWriter.class);
    /**
     * Will create a Titan graph provided the faunus configuration file is set correctly.
     * <p/>
     * This function will only accept the HBASE and Cassandra output formats
     *
     * @param configuration
     */
    public Graph writeSchema(Configuration configuration)
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
