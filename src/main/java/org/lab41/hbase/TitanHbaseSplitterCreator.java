package org.lab41.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by kramachandran (karkumar)
 */
public interface TitanHbaseSplitterCreator {

    public HTableDescriptor createAndSplitTable(String tablename, HBaseAdmin hbaseAdmin, int numberofSplits) throws IOException;
}
