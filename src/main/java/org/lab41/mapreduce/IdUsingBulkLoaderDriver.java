package org.lab41.mapreduce;

import com.thinkaurelius.faunus.FaunusGraph;
import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Holder;
import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.lab41.HdfsUtil;
import org.lab41.hbase.HbaseConfigurator;
import org.lab41.hbase.TitanHbaseThreePartSplitter;
import org.lab41.mapreduce.blueprints.BlueprintsGraphOutputMapReduce;
import org.lab41.schema.GraphSchemaWriter;
import org.lab41.schema.KroneckerGraphSchemaWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.lab41.Settings.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class IdUsingBulkLoaderDriver extends BaseBullkLoaderDriver
{
    Logger logger = LoggerFactory.getLogger(IdUsingBulkLoaderDriver.class);

    public int configureAndRunJobs(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, StorageException, InstantiationException, IllegalAccessException {

        logger.info("IdUsingBulkLoaderDriver");

        Configuration baseConfiguration = getConf();
        getAdditionalProperties(baseConfiguration, propsPath);
        getAdditionalProperties(baseConfiguration, sysPath);

        String hbaseSiteXmlPath = hbaseSiteXml;
        InputStream hbaseSiteXmlIS = getInputStreamForPath(hbaseSiteXmlPath, baseConfiguration);

        configureHbase(baseConfiguration, hbaseSiteXmlIS);

        //Configure the First adn Second jobs
        FaunusGraph faunusGraph = new FaunusGraph(baseConfiguration);

        String job1Outputpath = faunusGraph.getOutputLocation().toString();
        Path intermediatePath =new Path(job1Outputpath + "/job1") ;

        FileSystem fs = FileSystem.get(baseConfiguration);
        Job job1 = configureJob1(faunusGraph, intermediatePath, baseConfiguration, fs);
        Job job2 = configureJob2(baseConfiguration, faunusGraph,  fs);

        //no longer need the faunus graph.
        faunusGraph.shutdown();
        if(job1.waitForCompletion(true))
        {
            logger.info("SUCCESS 1: Cleaning up HBASE ");
            HBaseAdmin hBaseAdmin = new HBaseAdmin(baseConfiguration);
            hBaseAdmin.majorCompact(baseConfiguration.get("faunus.graph.output.titan.storage.tablename"));

            boolean betweenSplit = conf.getBoolean(BETWEEN_SPLIT_KEY, BETWEEN_SPLIT_DEFUALT);
            if(betweenSplit)
            {
                hBaseAdmin.split(baseConfiguration.get("faunus.graph.output.titan.storage.tablename"));
            }
            hBaseAdmin.balancer();


            logger.info("HBASE Clean up complete- starting next job");

            if(job2.waitForCompletion(true))
            {
                logger.info("SUCCESS 2");
            }

        }
        return 1;
    }

    private Job configureJob2(Configuration baseConfiguration, FaunusGraph faunusGraph,  FileSystem fs) throws IOException {
        Configuration job2Config= new Configuration(baseConfiguration);
        /** Job  2 Configuration **/
        Job job2 = new Job(job2Config);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(faunusGraph.getGraphOutputFormat());
        job2.setJobName("IdUsingBulkLoader Job2: " + faunusGraph.getInputLocation());
        job2.setJarByClass(IdUsingBulkLoaderDriver.class);
        job2.setMapperClass(IdUsingBulkLoaderMapReduce.EdgeMapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(FaunusVertex.class);

        FileInputFormat.setInputPaths(job2, faunusGraph.getInputLocation());
        job2.setNumReduceTasks(0);

        String strJob2OutputPath = faunusGraph.getOutputLocation().toString();
        Path job2Path = new Path(strJob2OutputPath + "/job2");

        if(fs.isDirectory(job2Path)){
            logger.info("Exists" + strJob2OutputPath + " --deleteing");
            fs.delete(job2Path, true);
        }

        FileOutputFormat.setOutputPath(job2, job2Path);

        return job2;
    }

    private Job configureJob1( FaunusGraph faunusGraph,
                              Path intermediatePath, Configuration baseConfiguration, FileSystem fs) throws IOException {

        Configuration job1Config = new Configuration(baseConfiguration);
        /** Job 1 Configuration **/

        Job job1 = new Job(job1Config);
        job1.setJobName("IdUsingBulkLoader Job1" + faunusGraph.getInputLocation());
        job1.setJarByClass(IdUsingBulkLoaderDriver.class);
        job1.setMapperClass(IdUsingBulkLoaderMapReduce.VertexMapper.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Holder.class);

        job1.setNumReduceTasks(0);

        job1.setInputFormatClass(faunusGraph.getGraphInputFormat());
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);


        if(fs.isDirectory(intermediatePath))
        {
            logger.info("Exists" +intermediatePath+" -- deleting!");
            fs.delete(intermediatePath, true);
        }

        FileOutputFormat.setOutputPath(job1, intermediatePath);
        Path inputPath = faunusGraph.getInputLocation();
        FileInputFormat.setInputPaths(job1, inputPath);
        return job1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new IdUsingBulkLoaderDriver(), args);

        System.exit(exitCode);
    }


}
