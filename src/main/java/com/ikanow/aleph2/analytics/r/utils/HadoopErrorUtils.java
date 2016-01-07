/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ikanow.aleph2.analytics.r.utils;

/** Hadoop related errors
 * @author Alex
 */
public class HadoopErrorUtils {

	public static final String NO_MAPPERS = "Found no mappers bucket={0} job={1} config={2}";
	public static final String MISSING_REQUIRED_FIELD = "The field {0} in bucket {1} is missing but required";
	
	final public static String BATCH_TOPOLOGIES_NOT_YET_SUPPORTED = "Batch topologies not yet supported";
	final public static String TECHNOLOGY_NOT_MODULE = "Can only be called from technology, not module";

	final public static String STREAMING_HADOOP_JOB = "Hadoop job was specified as streaming, not possible (bucket:job = {0}:{1})";
	
	final public static String JOB_STOP_ERROR = "Failed to stop job {0} bucket {1}: could not found or other error";

	final public static String TECHNOLOGY_OVERRIDE_INVALID = "Currently only the following are valid technology overrides settings: {3} not {4} (bucket:job:config = {0}:{1}:{2})";
	
	// General errors:
	
	public final static String EXCEPTION_CAUGHT = "Caught Exception: {0}";
	final public static String VALIDATION_ERROR = "Validation Error: {0}";
	final public static String NOT_YET_IMPLEMENTED = "This operation is not currently supported: {0}";

	// Temp issues which will get addressed
	
	final public static String ERROR_IN_ANALYTIC_JOB_CONFIGURATION = "Currently analytic job must encapsulate a batch enrichment (with the key being the name); name must be formed of alphanumeric/_ characters, batch job = {0} (bucket:job = {1}:{2})";
	final public static String CURR_DEPENDENCY_RESTRICTIONS = "Currently the internal Hadoop dependencies (in the analytic job _config_, not the higher level external analytic job dependencies) must either be empty, \"$previous\", or the reducer step (only if after it in the config list), specified dependency = {0}, batch config(s) = {1} (bucket:job = {2}:{3})";
	final public static String CURR_INPUT_RESTRICTIONS = "Currently {0} is not a supported data service (bucket:job = {1}:{2})";
	final public static String TEMP_TRANSIENT_OUTPUTS_MUST_BE_BATCH = "Currently (will be fixed soon), transient outputs must be batch (bucket={0}, job={1}, output type={2})";
	final public static String CURRENTLY_ONLY_ONE_REDUCE_SUPPORTED = "Currently it is only possible to specify one non-empty \"grouping_fields\" across the pipeline (\"reduce\") stage when using the Hadoop analytic engine (and it can't be at the start): {2} (bucket={0}, job={1})";	
}
