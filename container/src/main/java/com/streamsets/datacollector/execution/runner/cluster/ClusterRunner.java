/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.execution.runner.cluster;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.streamsets.datacollector.alerts.AlertEventListener;
import com.streamsets.datacollector.callback.CallbackInfo;
import com.streamsets.datacollector.cluster.ApplicationState;
import com.streamsets.datacollector.cluster.ClusterModeConstants;
import com.streamsets.datacollector.cluster.ClusterPipelineStatus;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.creation.PipelineBeanCreator;
import com.streamsets.datacollector.creation.PipelineConfigBean;
import com.streamsets.datacollector.execution.AbstractRunner;
import com.streamsets.datacollector.execution.PipelineState;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.PipelineStatus;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.StateListener;
import com.streamsets.datacollector.execution.alerts.AlertInfo;
import com.streamsets.datacollector.execution.cluster.ClusterHelper;
import com.streamsets.datacollector.execution.metrics.MetricsEventRunnable;
import com.streamsets.datacollector.execution.runner.common.PipelineRunnerException;
import com.streamsets.datacollector.execution.runner.common.ProductionPipeline;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineBuilder;
import com.streamsets.datacollector.execution.runner.common.ProductionPipelineRunner;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.Pipeline;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.stagelibrary.StageLibraryTask;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.updatechecker.UpdateChecker;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.datacollector.validation.ValidationError;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.ClusterSource;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;

import dagger.ObjectGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Control class to interact with slave pipelines running on cluster. It provides support for starting, stopping and
 * checking status of pipeline. It also registers information about the pipelines running on slaves.
 */
public class ClusterRunner extends AbstractRunner {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterRunner.class);
  static final String APPLICATION_STATE = "cluster.application.state";

  @Inject RuntimeInfo runtimeInfo;
  @Inject Configuration configuration;
  @Inject PipelineStateStore pipelineStateStore;
  @Inject @Named("runnerExecutor") SafeScheduledExecutorService runnerExecutor;

  private final String name;
  private final String rev;
  private final String user;
  private ObjectGraph objectGraph;
  private final ReentrantLock callbackCacheLock;
  private ClusterHelper clusterHelper;
  private final File tempDir;
  private final Cache<String, CallbackInfo> slaveCallbackList;
  private static final long SUBMIT_TIMEOUT_SECS = 120;
  private ScheduledFuture<?> managerRunnableFuture;
  private ScheduledFuture<?> metricRunnableFuture;
  private volatile boolean isClosed;
  private ScheduledFuture<?> updateCheckerFuture;
  private UpdateChecker updateChecker;
  private MetricsEventRunnable metricsEventRunnable;
  private PipelineConfiguration pipelineConf;


  private static final Map<PipelineStatus, Set<PipelineStatus>> VALID_TRANSITIONS =
     new ImmutableMap.Builder<PipelineStatus, Set<PipelineStatus>>()
    .put(PipelineStatus.EDITED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STARTING, ImmutableSet.of(PipelineStatus.START_ERROR, PipelineStatus.RUNNING, PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.START_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    // cannot transition to disconnecting from Running
    .put(PipelineStatus.RUNNING, ImmutableSet.of(PipelineStatus.CONNECT_ERROR, PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED,
      PipelineStatus.FINISHED, PipelineStatus.KILLED, PipelineStatus.RUN_ERROR))
    .put(PipelineStatus.RUN_ERROR, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPING, ImmutableSet.of(PipelineStatus.STOPPED, PipelineStatus.CONNECT_ERROR, PipelineStatus.DISCONNECTED))
    .put(PipelineStatus.FINISHED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.STOPPED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.KILLED, ImmutableSet.of(PipelineStatus.STARTING))
    .put(PipelineStatus.CONNECT_ERROR, ImmutableSet.of(PipelineStatus.RUNNING, PipelineStatus.STOPPING, PipelineStatus.DISCONNECTED,
      PipelineStatus.KILLED, PipelineStatus.FINISHED, PipelineStatus.RUN_ERROR))
    .put(PipelineStatus.DISCONNECTED, ImmutableSet.of(PipelineStatus.CONNECTING))
    .put(PipelineStatus.CONNECTING, ImmutableSet.of(PipelineStatus.STARTING, PipelineStatus.RUNNING, PipelineStatus.CONNECT_ERROR,
      PipelineStatus.FINISHED, PipelineStatus.KILLED, PipelineStatus.RUN_ERROR, PipelineStatus.DISCONNECTED))
    .build();

  @VisibleForTesting
  ClusterRunner(String name, String rev, String user, RuntimeInfo runtimeInfo, Configuration configuration,
    PipelineStoreTask pipelineStore, PipelineStateStore pipelineStateStore, StageLibraryTask stageLibrary,
    SafeScheduledExecutorService executorService, ClusterHelper clusterManager) {

    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.pipelineStateStore = pipelineStateStore;
    this.pipelineStore = pipelineStore;
    this.stageLibrary = stageLibrary;
    this.runnerExecutor = executorService;
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.callbackCacheLock = new ReentrantLock();
    this.slaveCallbackList = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build();
    this.tempDir = Files.createTempDir();
    if (clusterManager == null) {
      this.clusterHelper = new ClusterHelper(runtimeInfo, tempDir);
    } else {
      this.clusterHelper = clusterManager;
    }
  }

  public ClusterRunner(String user, String name, String rev, ObjectGraph objectGraph) {
    this.name = name;
    this.rev = rev;
    this.user = user;
    this.objectGraph = objectGraph;
    this.objectGraph.inject(this);
    this.callbackCacheLock = new ReentrantLock();
    this.slaveCallbackList = CacheBuilder.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build();
    this.tempDir = Files.createTempDir();
    this.clusterHelper = new ClusterHelper(runtimeInfo, tempDir);
    int refreshInterval = configuration.get(REFRESH_INTERVAL_PROPERTY, REFRESH_INTERVAL_PROPERTY_DEFAULT);
    if (refreshInterval > 0) {
      metricsEventRunnable = new MetricsEventRunnable(runtimeInfo, refreshInterval, this, null, eventListenerManager);
    }
  }

  @Override
  public void prepareForDataCollectorStart() throws PipelineStoreException, PipelineRunnerException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
    String msg = null;
    switch (status) {
      case STARTING:
        msg = "Pipeline was in STARTING state, forcing it to DISCONNECTED";
      case CONNECTING:
        msg = msg == null ? "Pipeline was in CONNECTING state, forcing it to DISCONNECTED" : msg;
      case RUNNING:
        msg = msg == null ? "Pipeline was in RUNNING state, forcing it to DISCONNECTED" : msg;
      case CONNECT_ERROR:
        msg = msg == null ? "Pipeline was in CONNECT_ERROR state, forcing it to DISCONNECTED" : msg;
      case STOPPING:
        msg = msg == null ? "Pipeline was in STOPPING state, forcing it to DISCONNECTED": msg;
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.DISCONNECTED, msg);
        break;
      case DISCONNECTED:
        break;
      case RUN_ERROR: // do nothing
      case EDITED:
      case FINISHED:
      case KILLED:
      case START_ERROR:
      case STOPPED:
        break;
      default:
        throw new IllegalStateException(Utils.format("Pipeline in undefined state: '{}'", status));
    }
  }

  @Override
  public void onDataCollectorStart() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException, StageException {
    PipelineStatus status = getState().getStatus();
    LOG.info("Pipeline '{}::{}' has status: '{}'", name, rev, status);
    switch (status) {
      case DISCONNECTED:
        String msg = "Pipeline was in DISCONNECTED state, changing it to CONNECTING";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.CONNECTING, msg);
        connectOrStart();
        break;
      default:
        LOG.error(Utils.format("Pipeline has unexpected status: '{}' on data collector start", status));
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getRev() {
    return rev;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public void resetOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onDataCollectorStop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    stopPipeline(true);
  }

  @Override
  public synchronized void stop() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException {
    stopPipeline(false);
  }

  private synchronized void stopPipeline(boolean isNodeShuttingDown) throws PipelineStoreException,
    PipelineRunnerException, PipelineRuntimeException {
    try {
      if (isNodeShuttingDown) {
        validateAndSetStateTransition(PipelineStatus.DISCONNECTED, "Node is shutting down, disconnecting from the "
          + "pipeline in cluster mode");
      } else {
        validateAndSetStateTransition(PipelineStatus.STOPPING, "Stopping the pipeline in cluster mode");
        ApplicationState appState = new ApplicationState((Map) getState().getAttributes().get(APPLICATION_STATE));
        if (appState.getId() == null) {
          throw new PipelineRunnerException(ContainerError.CONTAINER_0101, "for cluster application");
        }
        stop(appState, pipelineConf);
      }
    } finally {
      cancelRunnable();
    }
  }

  private Map<String, Object> getAttributes() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev).getAttributes();
  }

  private void connectOrStart() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException,
    StageException {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.putAll(getAttributes());
    ApplicationState appState = new ApplicationState((Map) attributes.get(APPLICATION_STATE));
    if (appState.getId() == null) {
      prepareForStart();
      start();
    } else {
      runtimeInfo.setSDCToken(appState.getSdcToken());
      try {
        pipelineConf = getPipelineConf(name, rev);
      } catch (PipelineRunnerException e) {
        validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, e.getMessage(), new HashMap<String, Object>());
        throw e;
      }
      connect(appState, pipelineConf);
      if (getState().getStatus().isActive()) {
        scheduleRunnable(pipelineConf);
      }
    }
  }

  @Override
  public void prepareForStart() throws PipelineStoreException, PipelineRunnerException {
    LOG.info("Preparing to start pipeline '{}::{}'", name, rev);
    validateAndSetStateTransition(PipelineStatus.STARTING, "Starting pipeline in cluster mode");
  }

  @Override
  public synchronized void start() throws PipelineStoreException, PipelineRunnerException, PipelineRuntimeException,
    StageException {
    Utils.checkState(!isClosed,
      Utils.formatL("Cannot start the pipeline '{}::{}' as the runner is already closed", name, rev));
    ExecutionMode executionMode = pipelineStateStore.getState(name, rev).getExecutionMode();
    if (executionMode != ExecutionMode.CLUSTER) {
      throw new PipelineRunnerException(ValidationError.VALIDATION_0073);
    }
    LOG.debug("State of pipeline for '{}::{}' is '{}' ", name, rev, getState());
    try {
      pipelineConf = getPipelineConf(name, rev);
    } catch (PipelineRunnerException e) {
      validateAndSetStateTransition(PipelineStatus.START_ERROR, e.getMessage(), new HashMap<String, Object>());
      throw e;
    }
    doStart(pipelineConf, getClusterSourceInfo(name, rev, pipelineConf));
  }

  @Override
  public PipelineState getState() throws PipelineStoreException {
    return pipelineStateStore.getState(name, rev);
  }

  @Override
  public String captureSnapshot(String name, int batches, int batchSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Snapshot getSnapshot(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SnapshotInfo> getSnapshotsInfo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSnapshot(String id) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<PipelineState> getHistory() throws PipelineStoreException {
    return pipelineStateStore.getHistory(name, rev, false);
  }

  @Override
  public void deleteHistory() {
    pipelineStateStore.deleteHistory(name ,rev);
  }

  @Override
  public Object getMetrics() {
    if (metricsEventRunnable != null) {
      return metricsEventRunnable.getAggregatedMetrics();
    }
    return null;
  }

  @Override
  public List<Record> getErrorRecords(String stage, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ErrorMessage> getErrorMessages(String stage, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Record> getSampledRecords(String sampleId, int max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<CallbackInfo> getSlaveCallbackList() {
    List<CallbackInfo> callbackInfoSet;
    callbackCacheLock.lock();
    try {
      callbackInfoSet = new ArrayList<>(slaveCallbackList.asMap().values());
    } finally {
      callbackCacheLock.unlock();
    }
    return callbackInfoSet;
  }

  @Override
  public void addAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();

  }

  @Override
  public void removeAlertEventListener(AlertEventListener alertEventListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteAlert(String alertId) throws PipelineRunnerException, PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<AlertInfo> getAlerts() throws PipelineStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    isClosed = true;
  }

  private synchronized void validateAndSetStateTransition(PipelineStatus toStatus, String message)
    throws PipelineStoreException, PipelineRunnerException {
    final Map<String, Object> attributes = new HashMap<>();
    attributes.putAll(getAttributes());
    validateAndSetStateTransition(toStatus, message, attributes);
  }

  private synchronized void validateAndSetStateTransition(PipelineStatus toStatus, String message, Map<String, Object> attributes)
    throws PipelineStoreException, PipelineRunnerException {
    Utils.checkState(attributes!=null, "Attributes cannot be set to null");
    final PipelineStatus status = getState().getStatus();
    if (status == toStatus) {
      LOG.debug(Utils.format("Ignoring status '{}' as this is same as current status", status));
    } else {
      checkState(VALID_TRANSITIONS.get(status).contains(toStatus), ContainerError.CONTAINER_0102, status, toStatus);
      PipelineState pipelineState = pipelineStateStore.saveState(user, name, rev, toStatus, message, attributes,
        ExecutionMode.CLUSTER);
      if(eventListenerManager != null) {
        eventListenerManager.broadcastPipelineState(pipelineState);
      }
    }
  }

  private void checkState(boolean expr, ContainerError error, Object... args) throws PipelineRunnerException {
    if (!expr) {
      throw new PipelineRunnerException(error, args);
    }
  }

  @Override
  public void updateSlaveCallbackInfo(CallbackInfo callbackInfo) {
    String sdcToken = Strings.nullToEmpty(runtimeInfo.getSDCToken());
    if (sdcToken.equals(callbackInfo.getSdcClusterToken()) &&
      !RuntimeInfo.UNDEF.equals(callbackInfo.getSdcURL())) {
      callbackCacheLock.lock();
      try {
        slaveCallbackList.put(callbackInfo.getSdcURL(), callbackInfo);
      } finally {
        callbackCacheLock.unlock();
      }
    } else {
      LOG.warn("SDC Cluster token not matched");
    }
  }

  private void clearSlaveList() {
    callbackCacheLock.lock();
    try {
      slaveCallbackList.invalidateAll();
    } finally {
      callbackCacheLock.unlock();
    }
  }

  @VisibleForTesting
  ClusterSourceInfo getClusterSourceInfo(String name, String rev, PipelineConfiguration pipelineConf)
    throws PipelineRuntimeException, StageException, PipelineStoreException {

    ProductionPipeline p = createProductionPipeline(name, rev, configuration, pipelineConf);
    Pipeline pipeline = p.getPipeline();
    try {
      pipeline.init();
    } finally {
      pipeline.destroy();
    }
    Source source = p.getPipeline().getSource();
    ClusterSource clusterSource;
    if (source instanceof ClusterSource) {
      clusterSource = (ClusterSource)source;
    } else {
      throw new RuntimeException(Utils.format("Stage '{}' does not implement '{}'", source.getClass().getName(),
        ClusterSource.class.getName()));
    }

    try {
      int parallelism = clusterSource.getParallelism();
      String clusterSourceName  = clusterSource.getName();
      if(parallelism < 1) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0112);
      }
      return new ClusterSourceInfo(clusterSourceName, parallelism, clusterSource.isInBatchMode(),
                                   clusterSource.getConfigsToShip());
    } catch (IOException ex) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0117, ex.getMessage(), ex);
    }
  }

  static class ClusterSourceInfo {
    private final int parallelism;
    private final String clusterSourceName;
    private final boolean isInBatchMode;
    private final Map<String, String> configsToShip;

    ClusterSourceInfo(String clusterSourceName, int parallelism, boolean isInBatchMode, Map<String, String> configsToShip) {
      this.parallelism = parallelism;
      this.clusterSourceName = clusterSourceName;
      this.isInBatchMode = isInBatchMode;
      this.configsToShip = configsToShip;
    }

    int getParallelism() {
      return parallelism;
    }

    String getClusterSourceName() {
      return clusterSourceName;
    }

    boolean isInBatchMode() {
      return isInBatchMode;
    }

    Map<String, String> getConfigsToShip() {
      return configsToShip;
    }

  }

  private ProductionPipeline createProductionPipeline(String name, String rev, Configuration configuration,
    PipelineConfiguration pipelineConfiguration) throws PipelineStoreException, PipelineRuntimeException,
    StageException {
    ProductionPipelineRunner runner =
      new ProductionPipelineRunner(name, rev, configuration, runtimeInfo, new MetricRegistry(),
        null, null, null);
    ProductionPipelineBuilder builder =
      new ProductionPipelineBuilder(name, rev, runtimeInfo, stageLibrary,  runner, null);
    return builder.build(pipelineConfiguration);
  }

  static class ManagerRunnable implements Runnable {
    private final ClusterRunner clusterRunner;
    private final PipelineConfiguration pipelineConf;

    public ManagerRunnable(ClusterRunner clusterRunner, PipelineConfiguration pipelineConf) {
      this.clusterRunner = clusterRunner;
      this.pipelineConf = pipelineConf;
    }

    @Override
    public void run() {
      try {
        checkStatus();
      } catch (Throwable throwable) {
        String msg = "Unexpected error: " + throwable;
        LOG.error(msg, throwable);
      }
    }

    private void checkStatus() throws PipelineStoreException, PipelineRunnerException {
      if (clusterRunner.getState().getStatus().isActive()) {
        PipelineState ps = clusterRunner.getState();
        ApplicationState appState = new ApplicationState((Map) ps.getAttributes().get(APPLICATION_STATE));
        clusterRunner.connect(appState, pipelineConf);
      }
      if (!clusterRunner.getState().getStatus().isActive()) {
        LOG.debug(Utils.format("Cancelling the task as the runner is in a non-active state '{}'",
          clusterRunner.getState()));
        clusterRunner.cancelRunnable();
      }
    }
  }

  private void connect(ApplicationState appState, PipelineConfiguration pipelineConf) throws PipelineStoreException,
    PipelineRunnerException {
    ClusterPipelineStatus clusterPipelineState = null;
    String msg = null;
    boolean connected = false;
    try {
      clusterPipelineState = clusterHelper.getStatus(appState, pipelineConf);
      connected = true;
    } catch (IOException ex) {
      msg = "IO Error while trying to check the status of pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to check the status of pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    } catch (Exception ex) {
      msg = "Error getting status of pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    }
    if (connected) {
      if (clusterPipelineState == ClusterPipelineStatus.RUNNING) {
        msg = "Connected to pipeline in cluster mode";
        validateAndSetStateTransition(PipelineStatus.RUNNING, msg);
      } else if (clusterPipelineState == ClusterPipelineStatus.FAILED) {
        msg = "Pipeline failed in cluster";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.RUN_ERROR, msg);
      } else if (clusterPipelineState == ClusterPipelineStatus.SUCCEEDED) {
        msg = "Pipeline succeeded in cluster";
        LOG.debug(msg);
        validateAndSetStateTransition(PipelineStatus.FINISHED, msg);
      }
    }
  }

  private synchronized void doStart(PipelineConfiguration pipelineConf, ClusterSourceInfo clusterSourceInfo) throws PipelineStoreException,
    PipelineRunnerException {
    Utils.checkNotNull(pipelineConf, "PipelineConfiguration cannot be null");
    Utils.checkState(clusterSourceInfo.getParallelism() != 0, "Parallelism cannot be zero");
    List<Issue> errors = new ArrayList<>();
    PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipelineConf, errors);
    if (pipelineConfigBean == null) {
      throw new PipelineRunnerException(ContainerError.CONTAINER_0116, errors);
    }
    Map<String, String> environment = new HashMap<>(pipelineConfigBean.clusterLauncherEnv);
    Map<String, String> sourceInfo = new HashMap<>();
    File bootstrapDir = new File(this.runtimeInfo.getLibexecDir(), "bootstrap-libs");
    String msg = null;
    try {
      //create pipeline and get the parallelism info from the source
      sourceInfo.put(ClusterModeConstants.NUM_EXECUTORS_KEY, String.valueOf(clusterSourceInfo.getParallelism()));
      sourceInfo.put(ClusterModeConstants.CLUSTER_SOURCE_NAME, clusterSourceInfo.getClusterSourceName());
      sourceInfo.put(ClusterModeConstants.CLUSTER_SOURCE_BATCHMODE, String.valueOf(clusterSourceInfo.isInBatchMode()));
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_NAME, name);
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_REV, rev);
      sourceInfo.put(ClusterModeConstants.CLUSTER_PIPELINE_USER, user);
      for (Map.Entry<String, String> configsToShip : clusterSourceInfo.getConfigsToShip().entrySet()) {
        LOG.info("Config to ship " + configsToShip.getKey() + ":" + configsToShip.getValue());
        sourceInfo.put(configsToShip.getKey(), configsToShip.getValue());
      }
      // This is needed for UI
      runtimeInfo.setAttribute(ClusterModeConstants.NUM_EXECUTORS_KEY, clusterSourceInfo.getParallelism());
      clearSlaveList();
      ApplicationState applicationState = clusterHelper.submit(pipelineConf, stageLibrary, new File(runtimeInfo.getConfigDir()),
          new File(runtimeInfo.getResourcesDir()), new File(runtimeInfo.getStaticWebDir()), bootstrapDir, environment,
          sourceInfo, SUBMIT_TIMEOUT_SECS);
      // set state of running before adding callback which modified attributes
      Map<String, Object> attributes = new HashMap<>();
      attributes.putAll(getAttributes());
      attributes.put(APPLICATION_STATE, applicationState.getMap());
      validateAndSetStateTransition(PipelineStatus.RUNNING, "Pipeline in cluster is running", attributes);
      scheduleRunnable(pipelineConf);
    } catch (IOException ex) {
      msg = "IO Error while trying to start the pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.START_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to start the pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.START_ERROR, msg);
    } catch (Exception ex) {
      msg = "Unexpected error starting pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.START_ERROR, msg);
    }
  }

  private void scheduleRunnable(PipelineConfiguration pipelineConf) throws PipelineStoreException,
    PipelineRunnerException {
    updateChecker = new UpdateChecker(runtimeInfo, configuration, pipelineConf, this);
    updateCheckerFuture = runnerExecutor.scheduleAtFixedRate(updateChecker, 1, 24 * 60, TimeUnit.MINUTES);
    if (metricsEventRunnable != null) {
      metricRunnableFuture =
        runnerExecutor.scheduleAtFixedRate(metricsEventRunnable, 0, metricsEventRunnable.getScheduledDelay(),
          TimeUnit.MILLISECONDS);
    }
    managerRunnableFuture =
      runnerExecutor.scheduleAtFixedRate(new ManagerRunnable(this, pipelineConf), 0, 30, TimeUnit.SECONDS);
  }

  private void cancelRunnable() {
    if (metricRunnableFuture != null) {
      metricRunnableFuture.cancel(true);
    }
    if (managerRunnableFuture != null) {
      managerRunnableFuture.cancel(false);
    }
    if (updateCheckerFuture != null) {
      updateCheckerFuture.cancel(true);
    }
  }

  private synchronized void stop(ApplicationState applicationState, PipelineConfiguration pipelineConf)
    throws PipelineStoreException, PipelineRunnerException {
    Utils.checkState(applicationState != null, "Application state cannot be null");
    boolean stopped = false;
    String msg;
    try {
      clusterHelper.kill(applicationState, pipelineConf);
      stopped = true;
    } catch (IOException ex) {
      msg = "IO Error while trying to stop the pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    } catch (TimeoutException ex) {
      msg = "Timedout while trying to stop the pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    } catch (Exception ex) {
      msg = "Unexpected error stopping pipeline" + ex;
      LOG.error(msg, ex);
      validateAndSetStateTransition(PipelineStatus.CONNECT_ERROR, msg);
    }
    Map<String, Object> attributes = new HashMap<>();
    if (stopped) {
      attributes.putAll(getAttributes());
      attributes.remove(APPLICATION_STATE);
      validateAndSetStateTransition(PipelineStatus.STOPPED, "Stopped cluster pipeline", attributes);
    }
  }

  @Override
  public Map getUpdateInfo() {
    return updateChecker.getUpdateInfo();
  }
}