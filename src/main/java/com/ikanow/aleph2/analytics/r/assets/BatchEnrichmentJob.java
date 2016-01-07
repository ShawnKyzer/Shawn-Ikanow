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
/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/package com.ikanow.aleph2.analytics.r.assets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.analytics.r.data_model.IBeJobConfigurable;
import com.ikanow.aleph2.analytics.r.services.BatchEnrichmentContext;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

import fj.Unit;
import fj.data.Either;
import fj.data.Validation;

/** Encapsulates a Hadoop job intended for batch enrichment or analytics
 * @author jfreydank
 */
public class BatchEnrichmentJob{

	public static String BATCH_SIZE_PARAM = "aleph2.batch.batchSize";
	public static String BE_CONTEXT_SIGNATURE = "aleph2.batch.beContextSignature";
	public static String BE_DEBUG_MAX_SIZE = "aleph2.batch.debugMaxSize";

	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		logger.debug("BatchEnrichmentJob constructor");
	}
	
	/** Temp class because i'm running into v1 vs v2 log4j classpath issues that I don't have time to resolve but I'd quite like for
	 *  logging to work
	 * @author Alex
	 */
	protected static class Loggable {
		protected Optional<org.apache.log4j.Logger> _v1_logger = Optional.empty(); // (don't make final in case Loggable c'tor isn't called)
		public Loggable() {
			_v1_logger = Lambdas.get(() -> {
				try {
					return Optional.<org.apache.log4j.Logger>of(org.apache.log4j.LogManager.getLogger(BatchEnrichmentJob.class));
				}
				catch (Throwable t) {
					logger.error(ErrorUtils.getLongForm("Error creating v1 logger: {0}", t));
					return Optional.<org.apache.log4j.Logger>empty();
				}
			});			
		}
	}
	
	/** Common functions between mapper and reducer
	 * @author Alex
	 *
	 */
	protected static abstract class BatchEnrichmentBase extends Loggable implements IBeJobConfigurable  {

		//////////////////////////////////////////////////////////////////////////////////////////////////
		
		// COMMON
		
		protected Optional<EnrichmentControlMetadataBean> _grouping_element = Optional.empty();
		
		protected DataBucketBean _data_bucket = null;
		protected BatchEnrichmentContext _enrichment_context = null;

		protected int _batch_size = 100;
		protected List<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> _ec_metadata = null;
		
		protected List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> _batch = new ArrayList<>();		
		
		/** Returns the starting stage for this element type
		 * @return
		 */
		protected abstract ProcessingStage getStartingStage();
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setDataBucket(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public void setDataBucket(DataBucketBean dataBucketBean) {
			this._data_bucket = dataBucketBean;			
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentContext(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext)
		 */
		@Override
		public void setEnrichmentContext(BatchEnrichmentContext enrichmentContext) {
			this._enrichment_context = enrichmentContext;
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
		 */
		@Override
		public void setBatchSize(int bs) {
			this._batch_size=bs;			
		}

		//////////////////////////////////////////////////////////////////////////////////////////////
		
		/** Performs generic set up operations
		 * @param config
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void setup(final Configuration config) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			
			try {
				BatchEnrichmentJob.extractBeJobParameters(this, config);
			} catch (Exception e) {
				throw new IOException(e);
			}			
			_v1_logger.ifPresent(logger -> logger.info("Setup BatchEnrichmentJob for " + this._enrichment_context.getBucket().map(b -> b.full_name()).orElse("unknown") + " Stage: " + this.getClass().getSimpleName() + ", Grouping = " + this._grouping_element));
			logger.info("Setup BatchEnrichmentJob for " + this._enrichment_context.getBucket().map(b -> b.full_name()).orElse("unknown") + " Stage: " + this.getClass().getSimpleName() + ", Grouping = " + this._grouping_element);
			
			final Iterator<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> it = _ec_metadata.iterator();
			ProcessingStage mutable_prev_stage = this.getStartingStage();
			while (it.hasNext()) {
				final Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean> t3 = it.next();
				
				_v1_logger.ifPresent(logger -> logger.info("Set up enrichment module " + t3._2().getClass().getSimpleName() + " name " + Optional.ofNullable(t3._3().name()).orElse("(no name)")));
				logger.info("Set up enrichment module " + t3._2().getClass().getSimpleName() + " name " + Optional.ofNullable(t3._3().name()).orElse("(no name)"));
				
				t3._1().onStageInitialize(t3._2(), _data_bucket, t3._3(), 
						Tuples._2T(
							mutable_prev_stage
							, 
							Patterns.match(this).<ProcessingStage>andReturn()
								.when(__ -> it.hasNext(), __ -> ProcessingStage.batch)
								.when(BatchEnrichmentBaseMapper.class, __ -> _grouping_element.isPresent(), __ -> ProcessingStage.grouping)
								.when(BatchEnrichmentBaseMapper.class, __ -> !_grouping_element.isPresent(), __ -> ProcessingStage.output)
								.when(BatchEnrichmentBaseCombiner.class, __ -> ProcessingStage.grouping)
								.when(BatchEnrichmentBaseReducer.class, __ -> ProcessingStage.output)
								.otherwise(__ -> ProcessingStage.unknown)
						),
						_grouping_element.map(cfg -> cfg.grouping_fields().stream()
													.filter(x -> !x.equals(EnrichmentControlMetadataBean.UNKNOWN_GROUPING_FIELDS))
													.collect(Collectors.toList()))
						);	
				
				mutable_prev_stage = ProcessingStage.batch;
			}
			
		} // setup
		
		/** Checks if we should send a batch of objects to the next stage in the pipeline
		 * @param flush
		 */
		protected void checkBatch(boolean flush, final TaskInputOutputContext<?,?,ObjectNodeWritableComparable,ObjectNodeWritableComparable> hadoop_context){
			if (flush) {
				_v1_logger.ifPresent(logger -> logger.info("Completing job"));
				logger.info("Completing job");
			}
			
			if((_batch.size() >= _batch_size) || (!_batch.isEmpty() && flush)) {
				hadoop_context.progress(); // (for little performance may in some cases prevent timeouts)
				
				final Iterator<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> it = _ec_metadata.iterator();
				List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> mutable_start = _batch;
				// Note currently we're sending the output of one pipeline into the input of the other
				// eventually we want to build the dependency graph and then perform the different stages in parallel
				while (it.hasNext()) {
					final Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean> t3 = it.next();
					
					// Skip over the grouping element, we've already processed it
					if (this._grouping_element.filter(g -> g == t3._3()).isPresent()) continue;
					
					t3._2().clearOutputRecords();
					t3._1().onObjectBatch(mutable_start.stream().map(t2 -> t2._1()), Optional.of(mutable_start.size()), Optional.empty());
					mutable_start = t3._2().getOutputRecords();
	
					if (flush) {
						if (_v1_logger.isPresent()) // (have to do it this way because of mutable var) 
							_v1_logger.get().info("Stage " + Optional.ofNullable(t3._3().name()).orElse("(no name)") + " output records=" + mutable_start.size() + " final_stage=" + !it.hasNext());
						logger.info("Stage " + Optional.ofNullable(t3._3().name()).orElse("(no name)") + " output records=" + mutable_start.size() + " final_stage=" + !it.hasNext());
					}
					
					if (!it.hasNext()) { // final stage output anything we have here
						completeBatchFinalStage(mutable_start, hadoop_context);
					}
				}				
				_batch.clear();
			}		
		}
		
		/** cleanup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void cleanup(
				TaskInputOutputContext<?, ?, ObjectNodeWritableComparable, ObjectNodeWritableComparable> context)
				throws IOException, InterruptedException {
			checkBatch(true, context);
			
			//DEBUG
			//System.out.println("Flushing output....." + new java.util.Date());
			
			_ec_metadata.stream().forEach(ecm -> ecm._1().onStageComplete(true));
			if (null != _enrichment_context) {
				_enrichment_context.flushBatchOutput(Optional.empty()).join();
			}
						
			//DEBUG
			//System.out.println("Completed Flushing output....." + new java.util.Date());			
		}
		
		/** Completes the final stage of a mapper/reducer/combiner
		 */
		public abstract void completeBatchFinalStage(final List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> output_objects, final TaskInputOutputContext<?,?,ObjectNodeWritableComparable,ObjectNodeWritableComparable> hadoop_context);
		
		/** Enrichment config initialization delegate - gets given a stream generator that modifies the incoming stream of jobs and only 
		 *  passes those that apply to the given mapper/reducer/combiner
		 * @param ecMetadata 
		 * @param streamGenerator
		 */
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata, UnaryOperator<Stream<EnrichmentControlMetadataBean>> streamGenerator)
		{
			_grouping_element = 
					ecMetadata.stream()
						.filter(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true))
						.filter(cfg -> !Optionals.ofNullable(cfg.grouping_fields()).isEmpty())
						.findFirst()
						;			
			
			final Map<String, SharedLibraryBean> library_beans = _enrichment_context.getAnalyticsContext().getLibraryConfigs();
			this._ec_metadata = streamGenerator.apply(ecMetadata.stream().filter(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true)))									
									.<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>>flatMap(ecm -> {
										final Optional<String> entryPoint = BucketUtils.getBatchEntryPoint(library_beans, ecm);
										
										_v1_logger.ifPresent(logger -> logger.info("Trying to launch stage " + Optional.ofNullable(ecm.name()).orElse("(no name)") + " with entry point = " + entryPoint));
										logger.info("Trying to launch stage " + Optional.ofNullable(ecm.name()).orElse("(no name)") + " with entry point = " + entryPoint);
										
										return entryPoint.map(Stream::of).orElseGet(() -> Stream.of(BePassthroughModule.class.getName()))
												.flatMap(Lambdas.flatWrap_i(ep -> {
													try {
														return (IEnrichmentBatchModule)Class.forName(ep).newInstance();
													}
													catch (Throwable t) {
														_v1_logger.ifPresent(logger -> logger.info(ErrorUtils.getLongForm("Error intializing {1}:{2}: {0}", t,
																Optional.ofNullable(ecm.name()).orElse("(no name)"), entryPoint
																)));
														throw t; // (will be ignored)
													}
												}))
												.map(mod -> {			
													_v1_logger.ifPresent(logger -> logger.info("Completed initialization of stage " + Optional.ofNullable(ecm.name()).orElse("(no name)")));
													logger.info("Completed initialization of stage " + Optional.ofNullable(ecm.name()).orElse("(no name)"));
						
													final BatchEnrichmentContext cloned_context = new BatchEnrichmentContext(_enrichment_context, _batch_size);
													Optional.ofNullable(library_beans.get(ecm.module_name_or_id())).ifPresent(lib -> cloned_context.setModule(lib));													
													return Tuples._3T(mod, cloned_context, ecm);
												});
									})
									.collect(Collectors.toList());			
		}
	
	}
	
	/** Pulls out batch enrichment parameters from the Hadoop configuration file
	 *  (Common to mapper and reducer)
	 * @param beJobConfigurable
	 * @param configuration
	 * @throws Exception
	 */
	public static void extractBeJobParameters(IBeJobConfigurable beJobConfigurable, Configuration configuration) throws Exception{
		
		final String contextSignature = configuration.get(BE_CONTEXT_SIGNATURE);  
		final BatchEnrichmentContext enrichmentContext = (BatchEnrichmentContext) ContextUtils.getEnrichmentContext(contextSignature);
		
		beJobConfigurable.setEnrichmentContext(enrichmentContext);
		final DataBucketBean dataBucket = enrichmentContext.getBucket().get();
		beJobConfigurable.setDataBucket(dataBucket);
		final List<EnrichmentControlMetadataBean> config = Optional.ofNullable(dataBucket.batch_enrichment_configs()).orElse(Collections.emptyList());
		beJobConfigurable.setEcMetadata(config.isEmpty()
											? Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())
											: config
				);
		beJobConfigurable.setBatchSize(configuration.getInt(BATCH_SIZE_PARAM,100));	
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// VALIDATOR
		
	/** Just used as an interface to validate enrichment jobs
	 * @author Alex
	 */
	public static class BatchEnrichmentBaseValidator extends BatchEnrichmentBase {
		protected ProcessingStage getStartingStage() { return ProcessingStage.unknown; }		
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(java.util.List)
		 */
		@Override
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
			this.setEcMetadata(ecMetadata, s -> s);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob.BatchEnrichmentBase#completeBatchFinalStage(java.util.List, org.apache.hadoop.mapreduce.TaskInputOutputContext)
		 */
		@Override
		public void completeBatchFinalStage(
				List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> output_objects,
				TaskInputOutputContext<?, ?, ObjectNodeWritableComparable, ObjectNodeWritableComparable> hadoop_context) {
			//(nothing to do here)
		}
		
		/** Validate the modules in this job
		 * @return
		 */
		public List<BasicMessageBean> validate() {
			return _ec_metadata.stream().<BasicMessageBean>flatMap(t3 -> t3._1().validateModule((IEnrichmentModuleContext) t3._2(), _data_bucket, t3._3()).stream()).collect(Collectors.toList());
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// MAPPER
		
	/** All the mapper functionality, just not in mapper form because of java8 multiple inheritance rules
	 * @author Alex
	 */
	protected static class BatchEnrichmentBaseMapper extends BatchEnrichmentBase {
		protected ProcessingStage getStartingStage() { return ProcessingStage.input; }		
 
		/** Setup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void setup(Mapper<String, Tuple2<Long, IBatchRecord>, ObjectNodeWritableComparable, ObjectNodeWritableComparable>.Context context) throws IOException, InterruptedException {
			this.setup(context.getConfiguration());
		} // setup
		
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
			// Only take the initial set of pre-reducer jobs
			this.setEcMetadata(ecMetadata, s -> StreamUtils.takeWhile(s, cfg -> Optionals.ofNullable(cfg.grouping_fields()).isEmpty()));
		}
		
		/** Mapper delegate
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, ObjectNodeWritableComparable, ObjectNodeWritableComparable>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			
			_batch.add(Tuples._2T(value, Optional.empty()));
			checkBatch(false, context);
		} // map

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob.BatchEnrichmentBase#completeBatchFinalStage(java.util.List)
		 */
		@Override
		public void completeBatchFinalStage(
				List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> output_objects, final TaskInputOutputContext<?,?,ObjectNodeWritableComparable,ObjectNodeWritableComparable> hadoop_context) {
			
			this._grouping_element.map(config -> {
				
				output_objects.forEach(Lambdas.wrap_consumer_i(record -> {
					final JsonNode out_object = record._1()._2().getJson();
					hadoop_context.write(
							new ObjectNodeWritableComparable((ObjectNode) record._2().orElseGet(() -> {
								final ObjectNode key = 
										config.grouping_fields().stream() // (non empty by construction)	
												.reduce(ObjectNodeWritableComparable._mapper.createObjectNode(),
														(acc, v) -> {
															JsonUtils.getProperty(v, out_object).ifPresent(val -> acc.put(v, val));
															return acc;
														},
														(acc1, acc2) -> acc1 // (can't ever happen)
														);
								
								return key;
								
							})) // (need to build a json object out of the grouping fields)
							, 
							new ObjectNodeWritableComparable((ObjectNode) out_object));
				}));
				
				return Unit.unit();
			})
			.orElseGet(() -> {
				final IAnalyticsContext analytics_context = _enrichment_context.getAnalyticsContext();
				output_objects.forEach(record ->
					analytics_context.emitObject(Optional.empty(), _enrichment_context.getJob(), Either.left(record._1()._2().getJson()), Optional.empty()));				
				
				return Unit.unit();
			});			
		}
		
	}
	
	/** Mapper implementation - just delegate everything to BatchEnrichmentBaseMapper
	 * @author Alex
	 */
	public static class BatchEnrichmentMapper extends Mapper<String, Tuple2<Long, IBatchRecord>, ObjectNodeWritableComparable, ObjectNodeWritableComparable> {
		protected final BatchEnrichmentBaseMapper _delegate = new BatchEnrichmentBaseMapper();
		
		/** User c'tor
		 */
		public BatchEnrichmentMapper(){
			super();
			logger.debug("BatchErichmentMapper constructor");
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			
			_delegate.setup(context);
			
		} // setup

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			
			_delegate.map(key, value, context);
		} // map


		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			_delegate.cleanup(context);
		}


	} //BatchErichmentMapper

	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// REDUCER (combiner below)
			
	protected static class BatchEnrichmentBaseReducer extends BatchEnrichmentBase {
		protected ProcessingStage getStartingStage() { return ProcessingStage.grouping; }		
		protected boolean is_combiner = false;
		
		protected boolean _single_element;
		protected Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean> _first_element;
		protected IAnalyticsContext _analytics_context;
		
		/** Setup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void setup(
				Reducer<ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable>.Context context)
				throws IOException, InterruptedException {
			this.setup(context.getConfiguration());
			
			_single_element = (1 == _ec_metadata.size());	
			_first_element = _ec_metadata.get(0); //(exists by construction)
			
			_analytics_context = _enrichment_context.getAnalyticsContext();			
		}
		
		/** Reduce delegate
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void reduce(
				ObjectNodeWritableComparable key,
				Iterable<ObjectNodeWritableComparable> values,
				Reducer<ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable>.Context context)
				throws IOException, InterruptedException
		{
			// OK first up, do the reduce bit, no batching needed for this bit
			
			// Set up a semi-streaming response so if there's a large number of records with a single key then we don't try to do everything at once
			
			final Function<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>, Validation<BasicMessageBean, JsonNode>> handleReduceOutput = Lambdas.wrap_u(record -> {
				if (_single_element) { // yay no need to batch
					if (is_combiner) {
						context.write(key, new ObjectNodeWritableComparable((ObjectNode) record._1()._2().getJson()));
					}
					else {
						_analytics_context.emitObject(Optional.empty(), _enrichment_context.getJob(), Either.left(record._1()._2().getJson()), Optional.empty());										
					}				
				}
				else { //(else add to the batch - the non map elements get handled by the batch processing)
					_batch.add(record);
					checkBatch(false, context);
				}
				return null;
			});
			final BatchEnrichmentContext local_enrich_context = _first_element._2();
			local_enrich_context.overrideOutput(handleReduceOutput);
			
			_first_element._1().cloneForNewGrouping().onObjectBatch(
					StreamUtils.zipWithIndex(Optionals.streamOf(values, false))
						.map(ix -> {
							if (0 == (ix.getIndex() % _batch_size)) context.progress(); // (every 200 records report progress to try to keep the system alive)
							return Tuples._2T(0L, new BeFileInputReader.BatchRecord(ix.getValue().get(), null));
						})
						,
					Optional.empty(), Optional.of(key.get()));

			//(shouldn't be necessary, but can't do any harm either)
			if (!_first_element._2().getOutputRecords().isEmpty()) _first_element._2().clearOutputRecords();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
			// Only take the reducer + (reducer-not-combiner) post reducer jobs 
			this.setEcMetadata(ecMetadata, is_combiner
					? s -> s.filter(cfg -> !Optionals.ofNullable(cfg.grouping_fields()).isEmpty())
					: s -> StreamUtils.skipWhile(s, cfg -> Optionals.ofNullable(cfg.grouping_fields()).isEmpty()));
		}

		@Override
		public void completeBatchFinalStage(
				List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> output_objects, final TaskInputOutputContext<?,?,ObjectNodeWritableComparable,ObjectNodeWritableComparable> hadoop_context)
		{
			output_objects.forEach(record ->
				_analytics_context.emitObject(Optional.empty(), _enrichment_context.getJob(), Either.left(record._1()._2().getJson()), Optional.empty()));				
		}

		/** cleanup delegate - differs from base because of the arg/order of onStageComplete
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void cleanup(
				TaskInputOutputContext<?, ?, ObjectNodeWritableComparable, ObjectNodeWritableComparable> context)
				throws IOException, InterruptedException {
			checkBatch(true, context);
			
			//DEBUG
			//System.out.println("Flushing output....." + new java.util.Date());
			
			_ec_metadata.stream().skip(1).forEach(ecm -> ecm._1().onStageComplete(false));
			// do this one first
			_first_element._1().onStageComplete(true);
			if (null != _enrichment_context) {
				_enrichment_context.flushBatchOutput(Optional.empty()).join();
			}
						
			//DEBUG
			//System.out.println("Completed Flushing output....." + new java.util.Date());			
		}
		
	}
		
	/** The reducer version
	 * @author jfreydank
	 */
	public static class BatchEnrichmentReducer extends Reducer<ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable, ObjectNodeWritableComparable> {
		protected BatchEnrichmentBaseReducer _delegate;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {			
			_delegate = new BatchEnrichmentBaseReducer();
			_delegate.setup(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(
				ObjectNodeWritableComparable key,
				Iterable<ObjectNodeWritableComparable> values,
				Context context)
				throws IOException, InterruptedException
		{
			_delegate.reduce(key, values, context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException
		{
			_delegate.cleanup(context);
		}

	} // reducer

	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// COMBINER
			
	protected static class BatchEnrichmentBaseCombiner extends BatchEnrichmentBaseReducer {
		public BatchEnrichmentBaseCombiner() {
			is_combiner = true;
		}
	}
	
	/** The combiner version
	 * @author jfreydank
	 */
	public static class BatchEnrichmentCombiner extends  BatchEnrichmentReducer {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(
				Context context)
				throws IOException, InterruptedException {
			_delegate = new BatchEnrichmentBaseCombiner();
			_delegate.setup(context);
		}				
	}
	
}
