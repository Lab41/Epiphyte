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

import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MRShutDownHook extends Thread {//This shutdown hook ensure that any Hadoop processes started by this class

    //are terminated when this class is terminated
    protected class ShutDownHook extends Thread {
        private final Job job;

        private final Logger logger = LoggerFactory.getLogger(MRShutDownHook.class);

        public ShutDownHook(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                //Will throw a runtime exception of this is called on a job that is not yet running?
                if (!job.isComplete()) {

                    job.killJob();

                }
            } catch (IllegalStateException e) {
                logger.warn("Shutdown hook exception", e);
            } catch (IOException e) {
                logger.warn("Shutdown hook exception", e);
            }

        }
    }
}
