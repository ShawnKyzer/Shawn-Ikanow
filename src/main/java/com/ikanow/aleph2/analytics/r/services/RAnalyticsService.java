package com.ikanow.aleph2.analytics.r.services;

import com.ikanow.aleph2.analytics.r.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.analytics.r.utils.RScriptUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import fj.data.Validation;
import org.apache.hadoop.mapreduce.Job;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Created by shawnkyzer on 12/7/15.
 */
public class RAnalyticsService implements IAnalyticsTechnologyService {


    @Override
    public void onInit(IAnalyticsContext iAnalyticsContext) {

    }

    @Override
    public boolean canRunOnThisNode(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext) {
        return false;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onNewThread(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext, boolean b) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onUpdatedThread(DataBucketBean dataBucketBean, DataBucketBean dataBucketBean1, Collection<AnalyticThreadJobBean> collection, boolean b, Optional<BucketDiffBean> optional, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onDeleteThread(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public FutureUtils.ManagementFuture<Boolean> checkCustomTrigger(DataBucketBean dataBucketBean, AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean analyticThreadComplexTriggerBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onThreadExecute(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, Collection<AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean> collection1, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onThreadComplete(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onPurge(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onPeriodicPoll(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> onTestThread(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, ProcessingTestSpecBean processingTestSpecBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> startAnalyticJob(
            DataBucketBean analytic_bucket,
            Collection<AnalyticThreadJobBean> jobs,
            AnalyticThreadJobBean job_to_start, IAnalyticsContext context)
    {
        return CompletableFuture.completedFuture(
                startAnalyticJobOrTest(analytic_bucket, jobs, job_to_start, context, Optional.empty()).validation(
                        fail -> ErrorUtils.buildErrorMessage
                                (this.getClass().getName(), "startAnalyticJob", fail)
                        ,
                        success -> ErrorUtils.buildSuccessMessage
                                (this.getClass().getName(), "startAnalyticJob", success.getJobID().toString())
                ));
    }

    /**
     * @param analytic_bucket
     * @param jobs
     * @param job_to_start
     * @param context
     * @param test_spec
     * @return
     */
    public Validation<String, Job> startAnalyticJobOrTest(
            DataBucketBean analytic_bucket,
            Collection<AnalyticThreadJobBean> jobs,
            AnalyticThreadJobBean job_to_start, IAnalyticsContext context,
            Optional<ProcessingTestSpecBean> test_spec
    )
    {

        //TODO (ALEPH-12): check if it's actually a batch enrichment first

        final BatchEnrichmentContext wrapped_context = new BatchEnrichmentContext(context);
        wrapped_context.setJob(job_to_start);

        // Create a pretend bucket that has this job as the (sole) enrichment topology...
        final DataBucketBean converted_bucket =
                (null != analytic_bucket.batch_enrichment_configs())
                        ? analytic_bucket
                        : BeanTemplateUtils.clone(analytic_bucket)
                        .with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
                        .with(DataBucketBean::batch_enrichment_configs, HadoopTechnologyUtils.convertAnalyticJob(job_to_start.name(), job_to_start.config()))
                        .done();

        wrapped_context.setBucket(converted_bucket);

        final BeJobLauncher beJobService = new BeJobLauncher(wrapped_context.getServiceContext().getGlobalProperties(), wrapped_context);
        final Validation<String, Job> result = beJobService.runEnhancementJob(converted_bucket, test_spec);

        // Once we have the results then we add to the list
        RScriptUtils.initializeSparkR();
        RScriptUtils.createDataFrame();
        return result;
    }
    @Override
    public CompletableFuture<BasicMessageBean> stopAnalyticJob(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, AnalyticThreadJobBean analyticThreadJobBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> resumeAnalyticJob(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, AnalyticThreadJobBean analyticThreadJobBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> suspendAnalyticJob(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, AnalyticThreadJobBean analyticThreadJobBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public CompletableFuture<BasicMessageBean> startAnalyticJobTest(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, AnalyticThreadJobBean analyticThreadJobBean, ProcessingTestSpecBean processingTestSpecBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public FutureUtils.ManagementFuture<Boolean> checkAnalyticJobProgress(DataBucketBean dataBucketBean, Collection<AnalyticThreadJobBean> collection, AnalyticThreadJobBean analyticThreadJobBean, IAnalyticsContext iAnalyticsContext) {
        return null;
    }

    @Override
    public Collection<Object> getUnderlyingArtefacts() {
        return null;
    }

    @Override
    public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> aClass, Optional<String> optional) {
        return null;
    }
}
