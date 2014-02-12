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

package org.lab41;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by kramachandran (karkumar)
 */
public class HdfsUtil {
    /**
     * Estimates the number of splits by taking the size of the paths and dividing by the splitSize.
     *
     * @param paths
     * @param configuration
     * @param splitSize
     * @return
     * @throws IOException
     */
    public static long getNumOfSplitsForInputs(Path[] paths, Configuration configuration, long splitSize) throws IOException
    {
        long size = getSizeOfPaths(paths, configuration);
        long splits = (int) Math.ceil( size / (splitSize)) ;
        return splits;
    }

    public static long getSizeOfPaths(Path[] paths, Configuration configuration) throws IOException
    {
        long totalSize = 0L;

        for(Path path: paths)
        {
           totalSize += getSizeOfDirectory(path, configuration);
        }
        return totalSize;
    }
    public static long getSizeOfDirectory(Path path, Configuration configuration) throws IOException {
        //Get the file size of the unannotated Edges
        FileSystem fileSystem = FileSystem.get(configuration);
        long size  = fileSystem.getContentSummary(path).getLength();
        return size;
    }
}
