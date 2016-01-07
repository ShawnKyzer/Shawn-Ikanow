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

import java.util.Optional;

import org.apache.hadoop.mapreduce.Job;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;

import fj.data.Validation;


/** Interface for launching batch enrichment jobs
 * @author Alex
 */
public interface IBeJobService {

	/** Launches a batch enrichment job
	 * @param bucket - the enrichment bucket to launch
	 * @param config_element - the name of the config element
	 * @return
	 */
	Validation<String, Job> runEnhancementJob(DataBucketBean bucket, final Optional<ProcessingTestSpecBean> testSpec);
}
