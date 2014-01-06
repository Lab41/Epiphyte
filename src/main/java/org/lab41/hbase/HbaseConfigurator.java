package org.lab41.hbase;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.lab41.mapreduce.BlueprintsGraphDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.lab41.mapreduce.AdditionalConfiguration.*;

/**
 * This class sets up HBase for the test by :
 * 1. Creating the appropriate tables
 * 2. Creating splitters
 * Created by kramachandran (karkumar)
 */
public class HbaseConfigurator {
    TitanHbaseTableCreator titanHbaseTableCreator;
    Logger logger = LoggerFactory.getLogger(BlueprintsGraphDriver.class);

    public HbaseConfigurator(TitanHbaseTableCreator titanHbaseTableCreator) {
        this.titanHbaseTableCreator = titanHbaseTableCreator;
    }

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    /**
     * Will create a tables in HBase.
     *
     * Requires that HBASE_CONF_DIR be set. Does not pull zookeeper qurom from the properties file.
     * @param configuration
     * @throws StorageException
     * @throws IOException
     */
    public void createHbaseTable(Configuration configuration) throws StorageException, IOException {
        //TODO: Figure out how to take full advantage of the  hbase configureation in the props file
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        String tableName = configuration.get("faunus.graph.output.titan.storage.tablename", "titan");

        Boolean presplit = configuration.getBoolean(HBASE_PRESPLIT_KEY,
                HBASE_PRESPLIT_DEFALUT);

        int numsplts = configuration.getInt(HBASE_NUM_SPLITS_KEY,
                HBASE_NUM_SPLITS_DEFAULT);

        if(hBaseAdmin.tableExists(tableName))
        {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            logger.info("deleting Table!");
        }

        if(presplit)
        {

        }
    }


}