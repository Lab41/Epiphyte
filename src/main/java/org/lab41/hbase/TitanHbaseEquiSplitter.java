package org.lab41.hbase;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.lab41.mapreduce.BlueprintsGraphDriver;
import org.lab41.mapreduce.SeperateEdgeAndVertexListDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import static org.lab41.Settings.*;

/**
 * Created by kramachandran (karkumar)
 */
public class TitanHbaseEquiSplitter implements TitanHbaseSplitter {
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);

    @Override
    public HTableDescriptor createAndSplitTable(String tableName, HBaseAdmin hBaseAdmin, Configuration configuration)
            throws IOException {

            int numSplits = configuration.getInt(NUM_SPLITS_KEY, NUM_SPLITS_DEFAULT);
            logger.info("Splitting! " + numSplits);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);


            RegionSplitter.UniformSplit us = new RegionSplitter.UniformSplit();
            //defaults to 0x00 to 0xffff... as start and end points
            byte[][] splits = us.split(numSplits);

            //debug loop
            logger.info("Splits : " + splits.length);
            for (int j = 0; j < splits.length; j++) {
                logger.info("createAndSplitTable" + Hex.encodeHexString(splits[j]) + " Bytes.toBytesString : " + Bytes.toStringBinary(splits[j]));
            }

            hBaseAdmin.createTable(hTableDescriptor, splits);
            return hTableDescriptor;
   }
}
