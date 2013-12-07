package org.lab41.mapreduce;

import org.apache.hadoop.util.Tool;

/**
 * This driver is meant to drive a pipeline consisting of only two stages :
 * 1. A Map step that inputs all the vertices
 * 2. A Reduce step that inputs all the edges
 *
 * This driver requires that Titan is configured to use the provided IDs
 *
 * Each Mapper will output a secondary Text file a series of metrics
 *
 *
 */
public class SeperateEdgeAndVertexListDriver extends IdUsingBulkLoaderDriver implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        return super.run(args);
    }
}
