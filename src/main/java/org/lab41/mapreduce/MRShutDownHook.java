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
