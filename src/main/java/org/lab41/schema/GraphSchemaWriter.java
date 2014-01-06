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
