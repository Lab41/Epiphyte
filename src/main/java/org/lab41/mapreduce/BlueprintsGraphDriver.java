package org.lab41.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.titan.diskstorage.StorageException;
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
import org.lab41.HdfsUtil;
import org.lab41.hbase.HbaseConfigurator;
import org.lab41.hbase.TitanHbaseThreePartSplitter;
import org.lab41.mapreduce.blueprints.BlueprintsGraphOutputMapReduce;
import org.lab41.schema.GraphSchemaWriter;
import org.lab41.schema.KroneckerGraphSchemaWriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * This Driver class does the following :
 * 1. Creates a graph in titian using a hard coded Schema. To chage subclass and overload createSchema() TODO: Change to accept a json file
 * 2. Calls the standard Faunus 0.4.1 Blueprints pipeline.
 * 3. Acceptions additional properties through a properties file. For now that file is stored within the jar, in the future
 * the file can be read off of HDFS TODO: Change to allow file to be read off of HDFS with default file in the jar.
 */
public class BlueprintsGraphDriver extends BaseBullkLoaderDriver implements Tool {

    protected long GB = 1073741824; //number of bytes in a GB
    protected long MB = 1048576;

    public int configureAndRunJobs(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, StorageException {

        Configuration baseConfiguration = getConf();
        getAdditionalProperties(baseConfiguration, propsPath);

        //For some reason oozie jobs don't see to pick up the hbase-site.xml
        //getting around it by packing in the jar
        InputStream hbaseConf= this.getClass().getClassLoader().getResourceAsStream("hbase-site.xml");

        logger.info("Hbase Conf available : " + hbaseConf.available());
        baseConfiguration.addResource(hbaseConf);

        BufferedWriter bufferedWriter = new BufferedWriter(new StringWriter());
        baseConfiguration.writeXml(bufferedWriter);
        logger.info("Base Conf: "  + bufferedWriter.toString());

        //TODO: Use reflection to set the splitter as a configuration option
        HbaseConfigurator hBaseConfigurator = new HbaseConfigurator(new TitanHbaseThreePartSplitter());
        hBaseConfigurator.createHbaseTable(baseConfiguration);

        //TODO: Use reflection to set schemaWrite as a configuration option
        GraphSchemaWriter graphSchemaWriter = new KroneckerGraphSchemaWriter();
        graphSchemaWriter.writeSchema(baseConfiguration);

        //Configure the First adn Second jobs
        FaunusGraph faunusGraph = new FaunusGraph(baseConfiguration);
        Configuration job1Config= new Configuration(baseConfiguration);
        Configuration job2Config= new Configuration(baseConfiguration);


        /** Job 1 Configuration **/
        Job job1 = new Job(job1Config);
        job1.setJobName("BluePrintsGraphDriver Job1");
        job1.setJarByClass(BlueprintsGraphDriver.class);
        job1.setMapperClass(BlueprintsGraphOutputMapReduce.VertexMap.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);
        job1.setReducerClass(BlueprintsGraphOutputMapReduce.Reduce.class);
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
            /***** Figure out how many reducer ********/

        Path[] paths  = SequenceFileInputFormat.getInputPaths(job1);
        long splits = HdfsUtil.getNumOfSplitsForInputs(paths, conf, MB*64);

         // The job is configure with 4 gb of memory;
        job1.setNumReduceTasks((int)Math.ceil(splits/48));


        /** Job  2 Configuration **/
        Job job2 = new Job(job2Config);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(faunusGraph.getGraphOutputFormat());
        job2.setJobName("BluePrintsGraphDriver Job2");
        job2.setJarByClass(BlueprintsGraphDriver.class);
        job2.setMapperClass(BlueprintsGraphOutputMapReduce.EdgeMap.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(FaunusVertex.class);
        FileInputFormat.setInputPaths(job2, intermediatePath);
        job2.setNumReduceTasks(0);

        String strJob2OutputPath = faunusGraph.getOutputLocation().toString();
        Path job2Path = new Path(strJob2OutputPath + "/job2");

        if(fs.isDirectory(job2Path)){
            logger.info("Exists" + strJob2OutputPath + " --deleteing");
            fs.delete(intermediatePath, true);
        }

        FileOutputFormat.setOutputPath(job2, job2Path);

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
        return 1;
    }

}
