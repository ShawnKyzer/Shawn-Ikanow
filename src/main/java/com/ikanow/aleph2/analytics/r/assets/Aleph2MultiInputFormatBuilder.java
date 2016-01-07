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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** A more generic multiple input format utility than the one provided by Hadoop
 * @author Alex
 */
public class Aleph2MultiInputFormatBuilder {

	public static final String ALEPH2_MULTI_INPUT_FORMAT_PREFIX = "aleph2.input.multi."; // (then the overwrite values
	public static final String ALEPH2_MULTI_INPUT_FORMAT_JOBS = "aleph2.input.multi.list"; // (then the overwrite values
	public static final String ALEPH2_MULTI_INPUT_FORMAT_CLAZZ = "aleph2.input.multi.clazz"; // (then the overwrite values
	
	final protected HashMap<String, Job> _inputs = new HashMap<>();
	
	/** User c'tor, just for setting up the job via addInput
	 */
	public Aleph2MultiInputFormatBuilder() 	{}
	
	/** Add a another path to this input format
	 * @param unique_name
	 * @param from_job - a job that just contains a fairly empty configuration
	 */
	public Aleph2MultiInputFormatBuilder addInput(final String unique_name, final Job from_job)
	{
		_inputs.put(unique_name, from_job);
		return this;
	}

	/** Sets the output configurations in the job 
	 * @param job
	 */
	public Job build(final Job job) {
		
		job.getConfiguration().set(ALEPH2_MULTI_INPUT_FORMAT_JOBS, _inputs.keySet().stream().collect(Collectors.joining(",")));
		_inputs.entrySet().stream().forEach(Lambdas.wrap_consumer_u(kv -> {
			try (final Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration>(job.getConfiguration(), Configuration.class)) {
				final Configuration new_config = new Configuration(kv.getValue().getConfiguration());
				new_config.set(ALEPH2_MULTI_INPUT_FORMAT_CLAZZ, kv.getValue().getInputFormatClass().getName());
				job.getConfiguration().set(ALEPH2_MULTI_INPUT_FORMAT_PREFIX + kv.getKey(), stringifier.toString(new_config));
			}
		}));			
		job.setInputFormatClass(Aleph2MultiInputFormat.class);
		return job;
	}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// INPUT FORMAT
	
	@SuppressWarnings("rawtypes")
	public static class Aleph2MultiInputFormat extends InputFormat {
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public RecordReader createRecordReader(InputSplit split, TaskAttemptContext task_context) throws IOException, InterruptedException {			
			return new Aleph2MultiRecordReader(split, task_context);			
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public List<InputSplit> getSplits(JobContext job_context) throws IOException, InterruptedException {
			
			// (NOTE: This is called from the originating client)
			
			return Arrays.stream(job_context.getConfiguration().get(ALEPH2_MULTI_INPUT_FORMAT_JOBS).split(","))
					.map(id -> Tuples._2T(id, job_context.getConfiguration().get(ALEPH2_MULTI_INPUT_FORMAT_PREFIX + id)))
					.filter(id_cfg -> null != id_cfg._2())
					.<InputSplit>flatMap(Lambdas.wrap_u(id_cfg -> {
						final Configuration config = Lambdas.get(Lambdas.wrap_u(() -> {
							try (final Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration>(job_context.getConfiguration(), Configuration.class)) {
								return stringifier.fromString(id_cfg._2());
							}							
						}));
						config.addResource(job_context.getConfiguration()); //(ie add everything else)
						InputFormat format = (InputFormat) ReflectionUtils.newInstance(Class.forName(config.get(ALEPH2_MULTI_INPUT_FORMAT_CLAZZ)), config);
						
						return ((List<InputSplit>)format.getSplits(createJobContext(job_context, config)))
								.stream()
								.map(Lambdas.wrap_u(split -> new Aleph2MultiInputSplit(id_cfg._1(), split, config, format.getClass(), job_context.getMapperClass())));
					}))
					.collect(Collectors.toList())
					;
		}
	}	

	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// RECORD READER
	
	@SuppressWarnings("rawtypes")
	public static class Aleph2MultiRecordReader extends RecordReader {

		RecordReader _delegate;
		
		public Aleph2MultiRecordReader(InputSplit split, TaskAttemptContext task_context) throws IOException, InterruptedException {
			// (NOTE: these are called from each actual mapper)
			
			final Aleph2MultiInputSplit input_split = (Aleph2MultiInputSplit) split;
			
			final Configuration config = Lambdas.get(Lambdas.wrap_u(() -> {
				try (final Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration>(task_context.getConfiguration(), Configuration.class)) {
					return stringifier.fromString(task_context.getConfiguration().get(ALEPH2_MULTI_INPUT_FORMAT_PREFIX + input_split.getName()));
				}							
			}));
			config.addResource(task_context.getConfiguration()); //(ie add everything else)
			
			
			InputFormat input_format = (InputFormat) ReflectionUtils.newInstance(input_split.getInputFormatClass(), task_context.getConfiguration());

			_delegate = input_format.createRecordReader(input_split.getInputSplit(), createTaskContext(task_context, config));						
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext task_context) throws IOException, InterruptedException {
			//(must be an Aleph2MultiInputSplit by construction, add this bit of code for robustness)
			final InputSplit delegate = Patterns.match(split).<InputSplit>andReturn()
											.when(Aleph2MultiInputSplit.class, s -> s.getInputSplit())
											.otherwise(s -> s);													
					
			_delegate.initialize(delegate, task_context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return _delegate.nextKeyValue();
		}

		@Override
		public Object getCurrentKey() throws IOException, InterruptedException {
			return _delegate.getCurrentKey();
		}

		@Override
		public Object getCurrentValue() throws IOException,
				InterruptedException {
			return _delegate.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return _delegate.getProgress();
		}

		@Override
		public void close() throws IOException {
			_delegate.close();
		}
		
	}
	
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	/** Proxy class for JobContext so that can override the configuration params
	 * @param delegate
	 * @param config_override
	 * @return
	 */
	private static JobContext createJobContext(final JobContext delegate, final Configuration config_override) {
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				if (method.getName().equals("getConfiguration")) { //override
					return config_override;
				}
				else { // everything else gets passed in
					final Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
					final Object o = m.invoke(delegate, args);
					return o;
				}
			}
		};
		return (JobContext) Proxy.newProxyInstance(JobContext.class.getClassLoader(),
																new Class[] { JobContext.class }, handler);
	}
	
	/** Proxy class for JobContext so that can override the configuration params
	 * @param delegate
	 * @param config_override
	 * @return
	 */
	private static TaskAttemptContext createTaskContext(final TaskAttemptContext delegate, final Configuration config_override) {
		InvocationHandler handler = new InvocationHandler() {
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				if (method.getName().equals("getConfiguration")) { //override
					return config_override;
				}
				else { // everything else gets passed in
					final Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
					final Object o = m.invoke(delegate, args);
					return o;
				}
			}
		};
		return (TaskAttemptContext) Proxy.newProxyInstance(TaskAttemptContext.class.getClassLoader(),
																new Class[] { TaskAttemptContext.class }, handler);
	}
	
}
