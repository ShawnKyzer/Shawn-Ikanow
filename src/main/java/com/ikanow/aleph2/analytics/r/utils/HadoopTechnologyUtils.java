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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.codepoetics.protonpack.StreamUtils;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** A collection of utilities for Hadoop related validation etc
 * @author Alex
 */
public class HadoopTechnologyUtils {

    /** Validate a single job for this analytic technology in the context of the bucket/other jobs
     * @param analytic_bucket - the bucket (just for context)
     * @param jobs - the entire list of jobs
     * @return the validated bean (check for success:false)
     */
    public static BasicMessageBean validateJobs(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs) {

        // Global validation:

        // Here we'll check:
        // (currently no global validation)

        // (Else graduate to per job validation)

        // Per-job validation:

        final List<BasicMessageBean> res =
                jobs.stream()
                        .map(job -> validateJob(analytic_bucket, jobs, job))
                        .collect(Collectors.toList());

        final boolean success = res.stream().allMatch(msg -> msg.success());

        final String message = res.stream().map(msg -> msg.message()).collect(Collectors.joining("\n"));

        return ErrorUtils.buildMessage(success, HadoopTechnologyUtils.class, "validateJobs", message);
    }

    final static Pattern CONFIG_NAME_VALIDATION = Pattern.compile("[0-9a-zA-Z_]+");

    /** Validate a single job for this analytic technology in the context of the bucket/other jobs
     * @param analytic_bucket - the bucket (just for context)
     * @param jobs - the entire list of jobs (not normally required)
     * @param job - the actual job
     * @return the validated bean (check for success:false)
     */
    public static BasicMessageBean validateJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job) {

        final LinkedList<String> errors = new LinkedList<>();

        // Type

        if (MasterEnrichmentType.batch != Optional.ofNullable(job.analytic_type()).orElse(MasterEnrichmentType.none)) {
            errors.add(ErrorUtils.get(HadoopErrorUtils.STREAMING_HADOOP_JOB, analytic_bucket.full_name(), job.name()));
        }

        // Inputs

        Optionals.ofNullable(job.inputs()).stream().forEach(input -> {
            if ("streaming".equals(input.data_service())) { // anything else is now somewhat supported, will at least call getServiceInput on it and try to resolve
                errors.add(ErrorUtils.get(HadoopErrorUtils.CURR_INPUT_RESTRICTIONS, input.data_service(), analytic_bucket.full_name(), job.name()));
            }
        });

        // Outputs

        if (null != job.output()) {
            if (Optional.ofNullable(job.output().is_transient()).orElse(false)) {
                final MasterEnrichmentType output_type = Optional.ofNullable(job.output().transient_type()).orElse(MasterEnrichmentType.none);
                if (MasterEnrichmentType.batch != output_type) {
                    errors.add(ErrorUtils.get(HadoopErrorUtils.TEMP_TRANSIENT_OUTPUTS_MUST_BE_BATCH, analytic_bucket.full_name(), job.name(), output_type));
                }
            }
        }

        // Convert config to enrichment control beans:

        try {
            if (null != job.config()) {
                final List<EnrichmentControlMetadataBean> configs = convertAnalyticJob(job.name(), job.config());

                // Is there a reducer step?
                final Set<String> reducer_steps =
                        configs.stream()
                                .filter(cfg -> !Optionals.ofNullable(cfg.grouping_fields()).isEmpty())
                                .map(cfg -> cfg.name())
                                .collect(Collectors.toSet())
                        ;

                if (reducer_steps.size() > 1) {
                    errors.add(ErrorUtils.get(HadoopErrorUtils.CURRENTLY_ONLY_ONE_REDUCE_SUPPORTED, analytic_bucket.full_name(), job.name(),
                            reducer_steps.stream().collect(Collectors.joining(";"))));
                }
                if (!reducer_steps.isEmpty()) {
                    // Reducer can't be at the start
                    if (reducer_steps.contains(configs.get(0).name())) { // non-empty by construction
                        errors.add(ErrorUtils.get(HadoopErrorUtils.CURRENTLY_ONLY_ONE_REDUCE_SUPPORTED, analytic_bucket.full_name(), job.name(),
                                configs.get(0).name()));
                    }
                }

                configs.stream()
                        .filter(config -> Optional.ofNullable(config.enabled()).orElse(true))
                        .forEach(config -> {
                            // Check names are valid

                            if (!CONFIG_NAME_VALIDATION.matcher(Optional.ofNullable(config.name()).orElse("")).matches()) {
                                errors.add(ErrorUtils.get(HadoopErrorUtils.ERROR_IN_ANALYTIC_JOB_CONFIGURATION, config.name(), analytic_bucket.full_name(), job.name()));

                            }

                            // Check that dependencies are either empty or "" or "$previous"

                            Optional.ofNullable(config.dependencies()).orElse(Collections.emptyList())
                                    .stream()
                                    .forEach(dependency -> {
                                        final String normalized_dep = Optional.ofNullable(dependency).orElse("");
                                        if (!"$previous".equals(normalized_dep) && !"".equals(normalized_dep) && !reducer_steps.contains(normalized_dep)) {
                                            errors.add(ErrorUtils.get(HadoopErrorUtils.CURR_DEPENDENCY_RESTRICTIONS, normalized_dep, config.name(), analytic_bucket.full_name(), job.name()));
                                        }
                                    });
                        })
                ;

                // Check technology overrides:
                configs.stream()
                        .filter(config -> Optional.ofNullable(config.enabled()).orElse(true))
                        .filter(config -> null != config.technology_override())
                        .forEach(config -> {
                            try {
//                                @SuppressWarnings("unused")
//                                final HadoopTechnologyOverrideBean tech_bean =
//                                        BeanTemplateUtils.from(config.technology_override(), HadoopTechnologyOverrideBean.class).get(); // (non null by construction)
                            }
                            catch (Exception e) {
                                //DEBUG
                                //e.printStackTrace();

                                errors.add(ErrorUtils.get(HadoopErrorUtils.TECHNOLOGY_OVERRIDE_INVALID, analytic_bucket.full_name(), job.name(), config.name(),
                                        Arrays.asList("num_reducers", "use_combiner", "requested_batch_size").stream().collect(Collectors.joining(";")), "(unknown)"));
                            }
                            //(will error if invalid, else do nothing
                        });

                // Check the names are different:
                // NOTE: can't test this because the names are taken from the keys which have to be unique by construction
                // when converting from batch_enrichment_config you'll get an exception earlier on if that isn't the case
                final List<String> dup_job_names =
                        configs.stream()
                                .collect(Collectors.groupingBy(cfg -> cfg.name()))
                                .entrySet()
                                .stream()
                                .filter(kv -> kv.getValue().size() > 1)
                                .map(kv -> kv.getKey())
                                .collect(Collectors.toList())
                        ;
                if (!dup_job_names.isEmpty()) {
                    errors.add(ErrorUtils.get(HadoopErrorUtils.ERROR_IN_ANALYTIC_JOB_CONFIGURATION,
                            dup_job_names.stream().collect(Collectors.joining(";")),
                            analytic_bucket.full_name(), job.name()));
                }

                // If there is a reducer, check any dependencies of that stage are after it
                final List<String> early_reducer_deps = StreamUtils.takeUntil(configs.stream(), cfg -> reducer_steps.contains(cfg.name()))
                        .filter(cfg -> null != cfg.dependencies())
                        .filter(cfg -> Optionals.ofNullable(cfg.dependencies()).stream().anyMatch(d -> reducer_steps.contains(d)))
                        .map(cfg -> cfg.name())
                        .collect(Collectors.toList())
                        ;
                if (!early_reducer_deps.isEmpty()) {
                    errors.add(ErrorUtils.get(HadoopErrorUtils.CURR_DEPENDENCY_RESTRICTIONS, reducer_steps.iterator().next(),//(exists by construction)
                            early_reducer_deps.stream().collect(Collectors.joining(";")),
                            analytic_bucket.full_name(), job.name()));
                }

            }
        }
        catch (Exception e) {
            //DEBUG
            //e.printStackTrace();

            errors.add(ErrorUtils.get(HadoopErrorUtils.ERROR_IN_ANALYTIC_JOB_CONFIGURATION, "(unknown)", analytic_bucket.full_name(), job.name()));
        }

        final boolean success = errors.isEmpty();

        return ErrorUtils.buildMessage(success, HadoopTechnologyUtils.class, "validateJobs", errors.stream().collect(Collectors.joining(";")));
    }

    /** Converts the analytic job configuration into a set of enrichment control metadata beans
     * @param job_name
     * @param analytic_config
     * @return
     */
    @SuppressWarnings("unchecked")
    public static List<EnrichmentControlMetadataBean> convertAnalyticJob(String job_name, final Map<String, Object> analytic_config) {
        final Object enrich_pipeline = analytic_config.get(EnrichmentControlMetadataBean.ENRICHMENT_PIPELINE);

        if ((null != enrich_pipeline) && List.class.isAssignableFrom(enrich_pipeline.getClass())) { // standard pipeline format
            return ((List<Object>)enrich_pipeline).stream()
                    .map(o -> BeanTemplateUtils.from((Map<String, Object>)o, EnrichmentControlMetadataBean.class).get())
                    .collect(Collectors.toList());
        }
        else { // old school format - ordering is a problem unless dependencies are specified

            //TODO: use core_shared.DependencyUtils to order this list

            return Optional.ofNullable(analytic_config).orElse(Collections.emptyMap()).entrySet()
                    .stream()
                    .filter(kv -> kv.getValue() instanceof Map)
                    .map(kv -> BeanTemplateUtils
                            .clone(
                                    BeanTemplateUtils
                                            .from((Map<String, Object>)kv.getValue(), EnrichmentControlMetadataBean.class)
                                            .get()
                            )
                            .with(EnrichmentControlMetadataBean::name, kv.getKey())
                            .done()
                    )
                    .collect(Collectors.toList());
        }
    }

    /** Generates a Hadoop configuration object
     * @param globals - the global configuration bean (containig the location of the files)
     * @return
     */
    public static Configuration getHadoopConfig(final GlobalPropertiesBean globals) {
        for (int i = 0; i < 60; ++i) {
            try {
                return getHadoopConfig(i, globals);
            }
            catch (java.util.ConcurrentModificationException e) {
                final long to_sleep = Patterns.match(i).<Long>andReturn()
                        .when(ii -> ii < 15, __ -> 100L)
                        .when(ii -> ii < 30, __ -> 250L)
                        .when(ii -> ii < 45, __ -> 500L)
                        .otherwise(__ -> 1000L)
                        + (new Date().getTime() % 100L) // (add random component)
                        ;
                try { Thread.sleep(to_sleep); } catch (Exception ee) {}
                if (59 == i) throw e;
            }
        }
        return null; //(doesn't ever get reached)
    }

    /** Generates a Hadoop configuration object
     * @param attempt - handles retries necessary because of the concurrent issues
     * @param globals - the global configuration bean (containig the location of the files)
     * @return
     */
    public static Configuration getHadoopConfig(long attempt, final GlobalPropertiesBean globals) {
        synchronized (Configuration.class) { // (this is not thread safe)
            final Configuration configuration = new Configuration(false);

            if (new File(globals.local_yarn_config_dir()).exists()) {
                configuration.addResource(new Path(globals.local_yarn_config_dir() +"/core-site.xml"));
                configuration.addResource(new Path(globals.local_yarn_config_dir() +"/yarn-site.xml"));
                configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hdfs-site.xml"));
                configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hadoop-site.xml"));
                configuration.addResource(new Path(globals.local_yarn_config_dir() +"/mapred-site.xml"));
            }
            if (attempt > 10) { // (try sleeping here)
                final long to_sleep = 500L + (new Date().getTime() % 100L); // (add random component)
                try { Thread.sleep(to_sleep); } catch (Exception e) {}
            }
            // These are not added by Hortonworks, so add them manually
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
            configuration.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs");
            // Some other config defaults:
            // (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
            configuration.set("mapred.reduce.tasks.speculative.execution", "false");

            return configuration;
        }
    }
}