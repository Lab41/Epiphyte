/*
 * Copyright 2014 In-Q-Tel Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
