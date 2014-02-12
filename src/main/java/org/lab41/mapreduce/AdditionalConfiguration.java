package org.lab41.mapreduce;

/**
 * Created by kramachandran on 12/6/13.
 */
public class AdditionalConfiguration {

    /**
     * If lab41.hbase.presplit is set to true then the you
     * must all set lab41.hbase.numberOfSplits.
     */
    public static final String HBASE_PRESPLIT_KEY = "lab41.hbase.presplit";
    public static final boolean HBASE_PRESPLIT_DEFALUT = false;

    /**
     *
     */
    public static final String HBASE_NUM_SPLITS_KEY= "lab41.hbase.numberOfSplits";
    public static final int HBASE_NUM_SPLITS_DEFAULT = 0;
}
