package org.lab41.hbase;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.lab41.mapreduce.BlueprintsGraphDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by kramachandran (karkumar)
 */
public class TitanHbaseEquiSplitter implements TitanHbaseTableCreator {
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);

    @Override
    public void createAndSplitTable(String tableName, HBaseAdmin hBaseAdmin, int numSplits) throws IOException {
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

    }
}
