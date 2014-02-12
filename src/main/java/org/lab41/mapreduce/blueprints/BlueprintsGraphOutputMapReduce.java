package org.lab41.mapreduce.blueprints;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.faunus.Tokens;
import com.thinkaurelius.faunus.formats.titan.GraphFactory;
import com.thinkaurelius.faunus.formats.titan.TitanOutputFormat;
import com.thinkaurelius.faunus.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.instrument.Instrumentation;
import java.util.HashMap;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * BlueprintsGraphOutputMapReduce will write a [NullWritable, FaunusVertex] stream to a Blueprints-enabled graph.
 * This is useful for bulk loading a Faunus graph into a Blueprints graph.
 * Graph writing happens in three distinction phase.
 * During the first Map phase, all the vertices of the graph are written.
 * During the first Reduce phase, an id-to-id distributed map of the adjacency pairs is serialized.
 * During the second Map phase, all the edges of the graph are written.
 * Each write stage is embarrassingly parallel with reduce communication only used to communicate generated vertex ids.
 * The output of the final Map phase is a degenerate graph and is not considered viable for consumption.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class BlueprintsGraphOutputMapReduce {

    public enum Counters {
        VERTICES_RETRIEVED,
        VERTICES_WRITTEN,
        VERTEX_PROPERTIES_WRITTEN,
        EDGES_WRITTEN,
        EDGE_PROPERTIES_WRITTEN,
        NULL_VERTEX_EDGES_IGNORED,
        NULL_VERTICES_IGNORED,
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS
    }

    private static final String GET_OR_CREATE_VERTEX = "getOrCreateVertex(faunusVertex,graph,mapContext)";
    private static final String FAUNUS_VERTEX = "faunusVertex";
    private static final String GRAPH = "graph";
    private static final String MAP_CONTEXT = "mapContext";

    public static final String FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE = "faunus.graph.output.blueprints.script-file";

    public static final Logger LOGGER = Logger.getLogger(BlueprintsGraphOutputMapReduce.class);
    // some random properties that will 'never' be used by anyone
    public static final String BLUEPRINTS_ID = "_bId0192834";
    public static final String ID_MAP_KEY = "_iDMaPKeY";

    public static Graph generateGraph(final Configuration config) {
        final Class<? extends OutputFormat> format = config.getClass(FaunusGraph.FAUNUS_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class);
        if (TitanOutputFormat.class.isAssignableFrom(format)) {
            return GraphFactory.generateGraph(config, TitanOutputFormat.FAUNUS_GRAPH_OUTPUT_TITAN);
        } else {
            // TODO: this is where Rexster can come into play here
            throw new RuntimeException("The provide graph output format is not supported: " + format.getName());
        }
    }


    ////////////// MAP/REDUCE WORK FROM HERE ON OUT

    // WRITE ALL THE VERTICES AND THEIR PROPERTIES
    public static class VertexMap extends Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>> {
        Logger logger =  Logger.getLogger(EdgeMap.class);
        static GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        static boolean firstRead = true;
        Graph graph;
        boolean loadingFromScratch;

        private final Holder<FaunusVertex> vertexHolder = new Holder<FaunusVertex>();
        private final LongWritable longWritable = new LongWritable();
        private final FaunusVertex shellVertex = new FaunusVertex();


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
            final String file = context.getConfiguration().get(FAUNUS_GRAPH_OUTPUT_BLUEPRINTS_SCRIPT_FILE, null);
            if (null == file)
                this.loadingFromScratch = true;
            else {
                this.loadingFromScratch = false;
                if (firstRead) {
                    final FileSystem fs = FileSystem.get(context.getConfiguration());
                    try {
                        engine.eval(new InputStreamReader(fs.open(new Path(file))));
                    } catch (Exception e) {
                        throw new IOException(e.getMessage());
                    }
                    firstRead = false;
                }
            }
            LOGGER.setLevel(Level.INFO);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            try {
                long startTime = System.currentTimeMillis();
                // Read (and/or Write) FaunusVertex (and respective properties) to Blueprints Graph
                // Attempt to use the ID provided by Faunus
                final Vertex blueprintsVertex = this.getOrCreateVertex(value, context);
                long endCreateVertexTime = System.currentTimeMillis();
                logger.info("vc(ms): " + (endCreateVertexTime - startTime));


                // Propagate shell vertices with Blueprints ids
                this.shellVertex.reuse(value.getIdAsLong());
                this.shellVertex.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId());
                // TODO: Might need to be OUT for the sake of unidirectional edges in Titan
                for (final Edge faunusEdge : value.getEdges(IN)) {
                    this.longWritable.set((Long) faunusEdge.getVertex(OUT).getId());
                    context.write(this.longWritable, this.vertexHolder.set('s', this.shellVertex));
                }

                this.longWritable.set(value.getIdAsLong());
                value.getProperties().clear();  // no longer needed in reduce phase
                value.setProperty(BLUEPRINTS_ID, blueprintsVertex.getId()); // need this for id resolution in reduce phase
                value.removeEdges(Tokens.Action.DROP, IN); // no longer needed in second map phase
                context.write(this.longWritable, this.vertexHolder.set('v', value));
                long endTime = System.currentTimeMillis();

                logger.info("tc(ms)" + (endTime - startTime));
            } catch (final Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }

        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph) {
                try {
                    ((TransactionalGraph) this.graph).commit();
                    context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
                } catch (Exception e) {
                    LOGGER.error("Could not commit transaction during VertexMap.cleanup():", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }

        public Vertex getOrCreateVertex(final FaunusVertex faunusVertex, final Mapper<NullWritable, FaunusVertex, LongWritable, Holder<FaunusVertex>>.Context context) throws InterruptedException {
            final Vertex blueprintsVertex;
            if (this.loadingFromScratch) {
                blueprintsVertex = this.graph.addVertex(faunusVertex.getIdAsLong());
                context.getCounter(Counters.VERTICES_WRITTEN).increment(1l);
                logger.debug("number of properties: " + blueprintsVertex.getPropertyKeys().size());
                for (final String property : faunusVertex.getPropertyKeys()) {
                    blueprintsVertex.setProperty(property, faunusVertex.getProperty(property));
                    context.getCounter(Counters.VERTEX_PROPERTIES_WRITTEN).increment(1l);
                }
            } else {
                try {
                    final Bindings bindings = engine.createBindings();
                    bindings.put(FAUNUS_VERTEX, faunusVertex);
                    bindings.put(GRAPH, this.graph);
                    bindings.put(MAP_CONTEXT, context);
                    blueprintsVertex = (Vertex) engine.eval(GET_OR_CREATE_VERTEX, bindings);
                } catch (Exception e) {
                    throw new InterruptedException(e.getMessage());
                }
            }
            return blueprintsVertex;
        }
    }

    public static class Reduce extends Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex> {

        @Override
        public void reduce(final LongWritable key, final Iterable<Holder<FaunusVertex>> values, final Reducer<LongWritable, Holder<FaunusVertex>, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {

            FaunusVertex faunusVertex = null;
            // generate a map of the faunus id with the blueprints id for all shell vertices (vertices incoming adjacent)
            final java.util.Map<Long, Object> faunusBlueprintsIdMap = new HashMap<Long, Object>();
            for (final Holder<FaunusVertex> holder : values) {
                if (holder.getTag() == 's') {
                    faunusBlueprintsIdMap.put(holder.get().getIdAsLong(), holder.get().getProperty(BLUEPRINTS_ID));
                } else {
                    final FaunusVertex toClone = holder.get();
                    faunusVertex = new FaunusVertex(toClone.getIdAsLong());
                    faunusVertex.setProperty(BLUEPRINTS_ID, toClone.getProperty(BLUEPRINTS_ID));
                    faunusVertex.addEdges(OUT, toClone);
                }
            }
            if (null != faunusVertex) {
                faunusVertex.setProperty(ID_MAP_KEY, faunusBlueprintsIdMap);
                context.write(NullWritable.get(), faunusVertex);
            } else {
                LOGGER.warn("No source vertex: faunusVertex[" + key.get() + "]");
                context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
            }
        }
    }

    // WRITE ALL THE EDGES CONNECTING THE VERTICES
    public static class EdgeMap extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {
        Logger logger =  Logger.getLogger(EdgeMap.class);

        Graph graph;
        private static final FaunusVertex DEAD_FAUNUS_VERTEX = new FaunusVertex();

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            this.graph = BlueprintsGraphOutputMapReduce.generateGraph(context.getConfiguration());
        }

        private long counter = 0 ;
        private long time = 0;

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            try {
                final java.util.Map<Long, Object> faunusBlueprintsIdMap = value.getProperty(ID_MAP_KEY);
                final Object blueprintsId = value.getProperty(BLUEPRINTS_ID);
                Vertex blueprintsVertex = null;
                if (null != blueprintsId)
                    blueprintsVertex = this.graph.getVertex(blueprintsId);
                // this means that an adjacent vertex to this vertex wasn't created
                if (null != blueprintsVertex) {
                    int edgeCounter = 0;
                    for (final Edge faunusEdge : value.getEdges(OUT)) {
                        long startTime = System.currentTimeMillis();
                        final Object otherId = faunusBlueprintsIdMap.get(faunusEdge.getVertex(IN).getId());
                        Vertex otherVertex = null;
                        if (null != otherId)
                            otherVertex = this.graph.getVertex(otherId);
                        if (null != otherVertex) {
                            final Edge blueprintsEdge = this.graph.addEdge(null, blueprintsVertex, otherVertex, faunusEdge.getLabel());
                            context.getCounter(Counters.EDGES_WRITTEN).increment(1l);

                            for (final String property : faunusEdge.getPropertyKeys()) {
                                blueprintsEdge.setProperty(property, faunusEdge.getProperty(property));
                                context.getCounter(Counters.EDGE_PROPERTIES_WRITTEN).increment(1l);
                            }

                        } else {
                            LOGGER.warn("No target vertex: faunusVertex[" + faunusEdge.getVertex(IN).getId() + "] blueprintsVertex[" + otherId + "]");
                            context.getCounter(Counters.NULL_VERTEX_EDGES_IGNORED).increment(1l);
                        }
                        long endTime = System.currentTimeMillis();


                    }
                } else {
                    LOGGER.warn("No source vertex: faunusVertex[" + key.get() + "] blueprintsVertex[" + blueprintsId + "]");
                    context.getCounter(Counters.NULL_VERTICES_IGNORED).increment(1l);
                }
                // the emitted vertex is not complete -- assuming this is the end of the stage and vertex is dead
                context.write(NullWritable.get(), DEAD_FAUNUS_VERTEX);
            } catch (final Exception e) {
                if (this.graph instanceof TransactionalGraph) {
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                }
                throw new IOException(e.getMessage(), e);
            }



        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.graph instanceof TransactionalGraph) {
                try {
                    ((TransactionalGraph) this.graph).commit();
                    context.getCounter(Counters.SUCCESSFUL_TRANSACTIONS).increment(1l);
                } catch (Exception e) {
                    LOGGER.error("Could not commit transaction during EdgeMap.cleanup():", e);
                    ((TransactionalGraph) this.graph).rollback();
                    context.getCounter(Counters.FAILED_TRANSACTIONS).increment(1l);
                    throw new IOException(e.getMessage(), e);
                }
            }
            this.graph.shutdown();
        }
    }

}
