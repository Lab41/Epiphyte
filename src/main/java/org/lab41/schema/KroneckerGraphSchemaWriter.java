/*
 * Copyright 2014 In-Q-Tel Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
        graph.makeKey("vDbl0").dataType(Double.class).make();
        graph.makeKey("vDbl1").dataType(Double.class).make();
        graph.makeKey("vStr0").dataType(String.class).make();
        graph.makeKey("vStr1").dataType(String.class).make();
        graph.makeKey("vStr2").dataType(String.class).make();
        graph.makeKey("eDbl0").dataType(String.class).make();
        graph.makeKey("eDbl1").dataType(String.class).make();
        graph.makeKey("eStr0").dataType(String.class).make();
        graph.makeKey("eStr1").dataType(String.class).make();
        graph.makeKey("eStr2").dataType(String.class).make();
        graph.makeLabel("REL").make();
        graph.commit();
        logger.info("Graph Create done!");

        return graph;
    }
}
