package org.lab41.mapreduce;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce;
import com.thinkaurelius.faunus.formats.BlueprintsGraphOutputMapReduce.Counters;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;

import java.io.IOException;
import java.io.InputStreamReader;


/**
 *  This set of MapReduce jobs executes a bulk load reusing the ids of the nodes provides in the sequence file.
 *
 *  Key Assumptions:
 *  1. Each node has an unqiue id.
 *  2. The ID is a long less the 2^62 (last two bytes are reserved for Titan's use)
 *  3. Neither Titan's partitioning options, nor the Titan's Local consitancy options have been turned on.
 *  4. Graph has been created.
 */
public class IdUsingBulkLoaderMapReduce {

    public static class VertexMapper extends Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>
    {
        private Graph graph;
        private long timer ;  //Used to keep running total of the amount of time spent in writing to the DB
        private long rollingCounter ; // Used to keep track of the number of vertices written to the DB

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        public createVertex(FaunusVertex faunusVertex, Context context)
        {
            Vertex blueprintsVertex = this.graph.addVertex(faunusVertex.getIdAsLong());
            context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
            for (final String property : faunusVertex.getPropertyKeys()) {
                blueprintsVertex.setProperty(property, faunusVertex.getProperty(property));
                context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
            }
        }
        @Override
        protected void map(NullWritable key, FaunusVertex value, Context context) throws IOException, InterruptedException {
            try{


            }
            catch (Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);

            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class EdgeMaper extends Mapper<NullWritable, FaunusVertex, NullWrtiable, NullWrtiable>
    {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(NullWritable key, FaunusVertex value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
