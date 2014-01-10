package org.lab41.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce.Counters;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.util.TitanId;
import com.tinkerpop.blueprints.*;
import org.apache.cassandra.cli.CliParser;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;


/**
 *  This set of MapReduce jobs executes a bulk load reusing the ids of the nodes provides in the sequence file.
 *  This pipeline consists of two Mappers :
 *  1. VertexMapper - Input SequenceFiles of FaunusVertices, writes directly to Titan, outputs a secondary output with some stats.
 *  2. EdgeMapper - Input Sequencefile of FaunusVertices, writes directly to Titan, outputs a secondary output with some stats
 *
 *  Key Assumptions:
 *  1. Each node has an unqiue id.
 *  2. The ID is a long less the 2^62 (last two bytes are reserved for Titan's use)
 *  3. Neither Titan's partitioning options, nor the Titan's Local consitancy options have been turned on.
 *  4. Graph has been created.
 */
public class IdUsingBulkLoaderMapReduce {

    public static abstract class BaseMapper extends Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>
    {
        protected Graph graph;
        protected MultipleOutputs<NullWritable, Text> multipleOutputs;
        protected Logger logger = LoggerFactory.getLogger(VertexMapper.class);



        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).commit();
                    context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
            }
            this.graph.shutdown();
        }
    }
    public static class VertexMapper extends BaseMapper{


        public Vertex createVertex(FaunusVertex faunusVertex, Context context)
        {
            Vertex blueprintsVertex = this.graph.addVertex(TitanId.toVertexId(faunusVertex.getIdAsLong()));
            context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
            for (final String property : faunusVertex.getPropertyKeys()) {
                blueprintsVertex.setProperty(property, faunusVertex.getProperty(property));
                context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
            }
            return blueprintsVertex;
        }


        @Override
        protected void map(NullWritable key, FaunusVertex value, Context context) throws IOException, InterruptedException {
            try{
                createVertex(value, context);
            }
            catch (Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }
        }
    }
    public static class EdgeMapper extends BaseMapper
    {

        @Override
        protected void map(NullWritable key, FaunusVertex value, Context context) throws IOException, InterruptedException {
            try{
                //Only have to process OUTS because every OUT is someone elses IN!
                Set<String> outLabels = value.getEdgeLabels(Direction.OUT);

                long sourceID= TitanId.toVertexId(value.getIdAsLong());
                Vertex sourceVertex = graph.getVertex(sourceID);

                if(sourceVertex != null)
                {
                    /** Do we need to iterate through both sides? */
                    for (String label : outLabels)
                     {
                        Iterable<Edge> edges =  value.getEdges(Direction.OUT, label);
                        for(Edge faunusEdge : edges)
                        {
                            Long destID =  TitanId.toVertexId(((FaunusVertex)faunusEdge.getVertex(Direction.IN)).getIdAsLong());
                            Vertex destVertex = graph.getVertex(destID);


                            //Going to follow the pattern in BlueprintsGraphOuptuMapReduce
                            //and Check to see both vertices exists prior to adding the edge
                            if(destVertex != null)
                            {

                                Edge blueprintsEdge = graph.addEdge(null, sourceVertex, destVertex, faunusEdge.getLabel());
                               context.getCounter(Counters.EDGES_WRITTEN).increment(1l);

                                for (final String property : faunusEdge.getPropertyKeys()) {
                                    blueprintsEdge.setProperty(property,faunusEdge.getProperty(property));
                                    context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1l);
                                }

                           }
                            else
                            {
                                logger.warn("No target vertex: faunusVertex[" + faunusEdge.getVertex(Direction.IN).getId()
                                        + "] ");
                                context.getCounter(Counters.NULL_VERTEX_EDGES_IGNORED).increment(1l);
                            }
                       }
                    }
                }
                else
                {
                    logger.warn("No source vertex: faunusVertex[" + sourceID + "]");
                    context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
                }
                context.write(NullWritable.get(), NullWritable.get());
            }
            catch(Exception e)
            {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }
        }

    }
}
