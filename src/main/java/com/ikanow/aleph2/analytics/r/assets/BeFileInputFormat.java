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
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;

import java.util.Arrays;

/** The file input format specific to batch enrichment modules
 * @author jfreydank
 */
public class BeFileInputFormat extends UpdatedCombineFileInputFormat<String, Tuple2<Long, IBatchRecord>> {
	private static final Logger logger = LogManager.getLogger(BeFileInputFormat.class);

	/** User c'tor
	 */
	public BeFileInputFormat(){
		super();
		logger.debug("BeFileInputFormat.constructor");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat#isSplitable(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		logger.debug("BeFileInputFormat.isSplitable");
		return false;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader<String, Tuple2<Long, IBatchRecord>> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
		logger.debug("BeFileInputFormat.createRecordReader");
		BeFileInputReader reader = new BeFileInputReader();
		// (don't initialize - that gets called seperately by the interface)
		return reader;
	} // createRecordReader

	//(should make this configurable)
	public static long MAX_SPLIT_SIZE = 64L*1024L*1024L;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		logger.debug("BeFileInputFormat.getSplits");
		
		super.setMaxSplitSize(MAX_SPLIT_SIZE);		
		
		try {			
			final List<InputSplit> splits = Lambdas.get(Lambdas.wrap_u(() -> {
				final List<InputSplit> tmp = super.getSplits(context);
				
				String debug_max_str = context.getConfiguration().get(BatchEnrichmentJob.BE_DEBUG_MAX_SIZE);
				if (null != debug_max_str)
				{
					final int requested_records = Integer.parseInt(debug_max_str);
					
					// dump 5* the request number of splits into one mega split
					// to strike a balance between limiting the data and making sure for 
					// tests that enough records are generated
					
					final CombineFileSplit combined = 
							new CombineFileSplit(
								tmp.stream()
									.map(split -> (CombineFileSplit)split)
									.flatMap(split -> Arrays.stream(split.getPaths()))
									.limit(5L*requested_records)
									.<Path>toArray(size -> new Path[size])
								,
								ArrayUtils.toPrimitive(tmp.stream()
									.map(split -> (CombineFileSplit)split)
									.flatMap(split -> Arrays.stream(split.getStartOffsets()).boxed())
									.limit(5L*requested_records)
									.<Long>toArray(size -> new Long[size]), 0L)
								,
								ArrayUtils.toPrimitive(tmp.stream()
									.map(split -> (CombineFileSplit)split)
									.flatMap(split -> Arrays.stream(split.getLengths()).boxed())
									.limit(5L*requested_records)
									.<Long>toArray(size -> new Long[size]), 0L)
								,
								tmp.stream()
									.map(split -> (CombineFileSplit)split)
									.flatMap(Lambdas.wrap_u(split -> Arrays.stream(split.getLocations())))
									.limit(5L*requested_records)
									.<String>toArray(size -> new String[size])
							)
							;
					return Arrays.<InputSplit>asList(combined);
				}
				else return tmp;
			}));
			
			logger.debug("BeFileInputFormat.getSplits: " +((splits!=null)? splits.size():"null"));
			return splits;
			
		} catch (Throwable t) {
			logger.error(ErrorUtils.getLongForm("Error getting splits, error = {0}", t));
			
			return Collections.emptyList();
		}
	}
}
