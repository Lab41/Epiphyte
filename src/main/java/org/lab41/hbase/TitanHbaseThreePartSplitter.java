package org.lab41.hbase;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.lab41.Settings.*;
/**
 * This splitter splits the key range into three parts:
 * Created by kramachandran (karkumar)
 */
public class TitanHbaseThreePartSplitter implements TitanHbaseSplitter {
    Logger logger = LoggerFactory.getLogger(TitanHbaseThreePartSplitter.class);
    public HTableDescriptor createAndSplitTable(String tableName, HBaseAdmin hBaseAdmin, Configuration conf) throws IOException {

      int numSplits = conf.getInt(NUM_SPLITS_KEY, NUM_SPLITS_DEFAULT);
        logger.info("Splitting! " + numSplits);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);


        byte[] lowStart = ArrayUtils.EMPTY_BYTE_ARRAY;
        byte[] lowEnd = new byte[]{0x01, (byte) 0x00, (int) 0x00, (byte) 0x00, 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        byte[][] lowsplits = Bytes.split(lowStart, lowEnd, (int)Math.ceil(numSplits * 0.05));
        //remove endpointsj
        lowsplits = Arrays.copyOfRange(lowsplits, 1, lowsplits.length - 1);

        byte[] midStart = new byte[]{0x01, (byte) 0x00, (int) 0x00, (byte) 0x00, 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        byte[] midEnd = new byte[]{(byte) 0x01, (byte) 0x01, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00};
        byte[][] midsplits = Bytes.split(midStart, midEnd, (int)Math.ceil(numSplits*0.25));
        midsplits = Arrays.copyOfRange(midsplits, 0, midsplits.length - 1);

        byte[] highStart = new byte[]{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};
        byte[] highEnd = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        byte[][] highsplits = Bytes.split(highStart, highEnd, (int)Math.ceil(numSplits * 0.60));
        highsplits = Arrays.copyOfRange(highsplits, 0, highsplits.length - 1);

        byte[][] splits = new byte[lowsplits.length + midsplits.length + highsplits.length][8];

        int i;
        for (i = 0; i < lowsplits.length; i++) {
            splits[i] = lowsplits[i];
        }

        for (i = 0; i < midsplits.length; i++) {
            splits[i + lowsplits.length] = midsplits[i];
        }

        for (i = 0; i < highsplits.length; i++) {
            splits[i + lowsplits.length + midsplits.length] = highsplits[i];
        }

        //debug loop
        logger.info("Splits : " + splits.length);
        for (int j = 0; j < splits.length; j++) {
            logger.info("createAndSplitTable" + Hex.encodeHexString(splits[j]) +
                    " Bytes.toBytesString : " + Bytes.toStringBinary(splits[j]));
        }

        hBaseAdmin.createTable(hTableDescriptor, splits);
        return hTableDescriptor;

    }
}
