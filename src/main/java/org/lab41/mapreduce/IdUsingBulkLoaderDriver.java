package org.lab41.mapreduce;

import com.thinkaurelius.titan.diskstorage.StorageException;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by kramachandran on 12/6/13.
 */
public class IdUsingBulkLoaderDriver extends BaseBullkLoaderDriver
{

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    @Override
    protected int configureAndRunJobs(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException, StorageException {
        return 0;
    }


    public static void main(String[] args) throws Exception{

    }
}
