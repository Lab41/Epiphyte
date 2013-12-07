package org.lab41.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.Graph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.lab41.mapreduce.blueprints.BlueprintsGraphOutputMapReduce;

import java.io.IOException;

/**
 * This Driver class does the following :
 * 1. Creates a graph in titian using a hard coded Schema. To chage subclass and overload createSchema() TODO: Change to accept a json file
 * 2. Calls the standard Faunus 0.4.1 Blueprints pipeline.
 * 3. Acceptions additional properties through a properties file. For now that file is stored within the jar, in the future
 * the file can be read off of HDFS TODO: Change to allow file to be read off of HDFS with default file in the jar.
 */
public class BlueprintsGraphDriver extends BaseBullkLoaderDriver implements Tool {




    public int configureGeneratorJob(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, StorageException {

        Configuration baseConfiguration = getConf();
        getAdditionalProperties(baseConfiguration, propsPath);


        Configuration job1Config= new Configuration(baseConfiguration);
        Configuration job2Config= new Configuration(baseConfiguration);

        createHbaseTable(baseConfiguration);

        Graph titangraph = createDB(baseConfiguration);
        FaunusGraph faunusGraph = new FaunusGraph(baseConfiguration);

        Job job1 = new Job(job1Config);
        job1.setJobName("BluePrintsGraphDriver Job1");
        job1.setJarByClass(BlueprintsGraphDriver.class);
        job1.setMapperClass(BlueprintsGraphOutputMapReduce.VertexMap.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(FaunusVertex.class);

        job1.setInputFormatClass(faunusGraph.getGraphInputFormat());
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileSystem fs = FileSystem.get(job1.getConfiguration());
        String job1Outputpath = faunusGraph.getOutputLocation().toString();
        Path intermediatePath =new Path(job1Outputpath + "/job1") ;
        if(fs.isDirectory(intermediatePath))
        {
            logger.info("Exists" +intermediatePath+" -- deleting!");
            fs.delete(intermediatePath, true);
        }

        FileOutputFormat.setOutputPath(job1, intermediatePath);
        Path inputPath = faunusGraph.getInputLocation();
        FileInputFormat.setInputPaths(job1, inputPath);


        /** Add and run First Mapper / Reducer **/
        Job job2 = new Job(job2Config);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(faunusGraph.getGraphOutputFormat());
        job2.setJobName("BluePrintsGraphDriver Job2");
        job2.setJarByClass(BlueprintsGraphDriver.class);
        job2.setMapperClass(BlueprintsGraphOutputMapReduce.EdgeMap.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(FaunusVertex.class);
        FileInputFormat.setInputPaths(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, faunusGraph.getOutputLocation());

        //no longer need the faunus graph.
        faunusGraph.shutdown();


        if(job1.waitForCompletion(true))
        {
            logger.info("SUCCESS 1");
            if(job2.waitForCompletion(true))
            {
                logger.info("SUCCESS 2");
            }

        }
        /* Add second Mapper */

        return 1;
    }

}
