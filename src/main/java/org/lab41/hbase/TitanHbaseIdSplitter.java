package org.lab41.hbase;

import com.thinkaurelius.titan.core.util.TitanId;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.lab41.Settings.*;

/**
 * Created by kramachandran (karkumar)
 */
public class TitanHbaseIdSplitter implements TitanHbaseSplitter {
    Logger logger = Logger.getLogger(TitanHbaseIdSplitter.class);

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    public HTableDescriptor createAndSplitTable(String tableName, HBaseAdmin hbaseAdmin, Configuration configuration)
            throws IOException   {
        Long maxId = configuration.getLong(MAXID_KEY, MAXID_DEFAULT);
        Long regionSize = configuration.getLong(REGION_SIZE_KEY, REGION_SIZE_DEFAULT);
        ArrayList<byte[]> arrayList = new ArrayList<byte[]>();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        int numSplits = configuration.getInt(NUM_SPLITS_KEY, NUM_SPLITS_DEFAULT);

        for (long i = 1 ; i < maxId ; i += regionSize)
        {
            byte[] splitPoint = longToBytes(TitanId.toVertexId(i));
            arrayList.add(splitPoint);
        }

        byte[] midStart = new byte[]{0x01, (byte) 0x00, (int) 0x00, (byte) 0x00, 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        byte[] midEnd = new byte[]{(byte) 0x01, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        byte[][] midsplits = Bytes.split(midStart, midEnd, (int)Math.ceil(numSplits*0.75));;
        midsplits = Arrays.copyOfRange(midsplits, 0, midsplits.length - 1);

        for(int i = 0 ; i < midsplits.length; i++)
        {
            arrayList.add(midsplits[i]);
        }

        byte[] highStart = new byte[]{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
        byte[] highEnd = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        byte[][] highSplits = Bytes.split(highStart, highEnd, (int)Math.ceil(numSplits * 0.25));
        highSplits = Arrays.copyOfRange(highSplits, 0, highSplits.length-1);

        for(int i = 0 ; i < highSplits.length; i++)
        {
            arrayList.add(highSplits[i]);
        }

        byte[][] splits = new byte[arrayList.size()][8];
        arrayList.toArray(splits);
        //debug loop
        logger.info("Splits : " + splits.length);
        for (int j = 0; j < splits.length; j++) {
            logger.info("createAndSplitTable" + Hex.encodeHexString(splits[j]) +
                    " Bytes.toBytesString : " + Bytes.toStringBinary(splits[j]));
        }

        hbaseAdmin.createTable(hTableDescriptor, splits);

        return hTableDescriptor;
    }

}
