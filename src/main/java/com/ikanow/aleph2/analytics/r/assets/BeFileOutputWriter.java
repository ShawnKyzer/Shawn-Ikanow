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
package com.ikanow.aleph2.analytics.r.assets;

import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

/** Output Writer specific to batch enrichment
 *  (Doesn't currently do anything, all the outputting occurs via the context)
 * @author jfreydank
 */
public class BeFileOutputWriter extends RecordWriter<String, Tuple2<Long, IBatchRecord>>{
	static final Logger _logger = LogManager.getLogger(BeFileOutputWriter.class); 
	
	/** User c'tor
	 */
	public BeFileOutputWriter() {
		super();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(String key, Tuple2<Long, IBatchRecord> value) throws IOException, InterruptedException {
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	}

}
