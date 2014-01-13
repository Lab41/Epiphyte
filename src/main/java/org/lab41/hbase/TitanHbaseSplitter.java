package org.lab41.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by kramachandran (karkumar)
 */
public interface TitanHbaseSplitter {

    public HTableDescriptor createAndSplitTable(String tablename, HBaseAdmin hbaseAdmin, Configuration configuration) throws IOException;
}
