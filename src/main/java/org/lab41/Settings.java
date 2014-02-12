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

/**
 * Created by kramachandran (karkumar)
 */
public class Settings {

    /**
     * Used by TitanHbaseEquiSplitter & TitantHbaseThreePartSplitter to determine the number of splits to make
     *
     */
    public static String NUM_SPLITS_KEY = "org.lab41.hbase.numberOfSplits";
    public static int NUM_SPLITS_DEFAULT = 128;

    /**
     *
     */
     /**
     * If lab41.hbase.presplit is set to true then the you
     * must all set lab41.hbase.numberOfSplits.
     */
    public static final String HBASE_PRESPLIT_KEY = "org.lab41.hbase.presplit";
    public static final boolean HBASE_PRESPLIT_DEFALUT = false;

    /**
     * Used to determine which pre-splitter to use
     */
    public static String SPLITTER_CLASS_KEY = "org.lab41.hbase.preSplitterClass";
    public static String SPLITTER_CLASS_DEFUALT = "org.lab41.hbase.TitanHbaseEquiSpliter";

    /**
     * Used by the TitanHbaseIdSplitter to determine the number of regions:
     *
     * # of Regions = maxID/regionSize
     */
    public static String MAXID_KEY = "org.lab41.hbase.maxId";
    public static long MAXID_DEFAULT = 1000000000;

    /**
     * Used by the TitanHbaseIdSplitter
     *
     * # of Regions = maxID/regionSize
     */
    public static String REGION_SIZE_KEY = "org.lab41.hbase.regionSize";
    public static long REGION_SIZE_DEFAULT = 10000000;

    /**
     * If set to true the the dirver will call split on the table between jobs.
     *
     * Defaulting to false because this can take a lot of time
     */
    public static String BETWEEN_SPLIT_KEY = "org.lab41.hbase.betweenSplit";
    public static boolean BETWEEN_SPLIT_DEFUALT = false;
}
