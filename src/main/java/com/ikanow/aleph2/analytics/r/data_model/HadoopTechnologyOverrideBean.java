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
package com.ikanow.aleph2.analytics.r.data_model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Config bean for available technology overrides (in the enrichment metadata control bean)
 * @author Alex
 */
public class HadoopTechnologyOverrideBean implements Serializable {
	private static final long serialVersionUID = -6990533978104476468L;

	/** Jackson c'tor
	 */
	protected HadoopTechnologyOverrideBean() {}
	
	/** The desired number of reducers to use in grouping operations (no default since might want to calculate based on cluster settings)
	 * @return
	 */
	public Integer num_reducers() { return num_reducers; }

	/** For jobs with mapping operations, determines whether to use the combiner or not (optional, defaults to true)
	 * @return
	 */
	public  Boolean use_combiner() { return Optional.ofNullable(use_combiner).orElse(true); }
	
	/** Requests an optimal batch size for this job - NOTE not guaranteed to be granted
	 * @return
	 */
	public Integer requested_batch_size() { return requested_batch_size; }		
	
	/** Hadoop parameters (eg "mapred.task.timeout") to override where allowed (by user and system permissions)
	 *  Note the "."s are replaced by ":"s to workaround certain DBs' key issues, eg would actually use "mapred:task:timeout" in the above example)
	 * @return
	 */
	public Map<String, String> config() { return Optional.ofNullable(config).orElse(Collections.emptyMap()); }
	
	private Integer num_reducers;
	private Boolean use_combiner;
	private Integer requested_batch_size;
	private Map<String, String> config;
}
