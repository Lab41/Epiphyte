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

package org.lab41.schema;

import com.tinkerpop.blueprints.Graph;
import org.apache.hadoop.conf.Configuration;

/**
 *
 *
 * Created by kramachandran (karkumar)
 */
public interface GraphSchemaWriter {
    /**
     * This function, given a titan graph configuration will write the schema of that graph to the graph.
     * @param configuration
     */
    public Graph writeSchema(Configuration configuration);
}
