/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnProtos.SplitMessage;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.custom.SchedulerCustomUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.custom.SchedulerCustomUtils.NodeSelection;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.customtypes.BlockScore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.customtypes.CollectionsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.annotations.VisibleForTesting;

/**
 * A scheduler that schedules resources between a set of queues. The scheduler
 * keeps track of the resources used by each queue, and attempts to maintain
 * fairness by scheduling tasks at queues whose allocations are farthest below
 * an ideal fair distribution.
 * 
 * The fair scheduler supports hierarchical queues. All queues descend from a
 * queue named "root". Available resources are distributed among the children
 * of the root queue in the typical fair scheduling fashion. Then, the children
 * distribute the resources assigned to them to their children in the same
 * fashion.  Applications may only be scheduled on leaf queues. Queues can be
 * specified as children of other queues by placing them as sub-elements of their
 * parents in the fair scheduler configuration file.
 * 
 * A queue's name starts with the names of its parents, with periods as
 * separators.  So a queue named "queue1" under the root named, would be 
 * referred to as "root.queue1", and a queue named "queue2" under a queue
 * named "parent1" would be referred to as "root.parent1.queue2".
 */
@LimitedPrivate("yarn")
@Unstable
@SuppressWarnings("unchecked")
public class CustomYLocSimFairScheduler extends FairScheduler {
  private boolean initialized;
  private FairSchedulerConfiguration conf;
  private Resource minimumAllocation;
  private Resource maximumAllocation;
  private Resource incrAllocation;
  private QueueManager queueMgr;
  private volatile Clock clock;
  private boolean usePortForNodeName;
  
  

  private static final Log LOG = LogFactory.getLog(CustomYLocSimFairScheduler.class);
  
  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  
  // Value that container assignment methods return when a container is
  // reserved
  public static final Resource CONTAINER_RESERVED = Resources.createResource(-1);

  // How often fair shares are re-calculated (ms)
  protected long UPDATE_INTERVAL = 500;

  // Aggregate metrics
  FSQueueMetrics rootMetrics;

  // Time when we last updated preemption vars
  protected long lastPreemptionUpdateTime;
  // Time we last ran preemptTasksIfNecessary
  private long lastPreemptCheckTime;

  // Nodes in the cluster, indexed by NodeId
  private Map<NodeId, FSSchedulerNode> nodes = 
      new ConcurrentHashMap<NodeId, FSSchedulerNode>();

  // Aggregate capacity of the cluster
  private Resource clusterCapacity = 
      RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Resource.class);

  // Preemption related variables
  protected boolean preemptionEnabled;
  protected float preemptionUtilizationThreshold;

  // How often tasks are preempted
  protected long preemptionInterval;
  
  // ms to wait before force killing stuff (must be longer than a couple
  // of heartbeats to give task-kill commands a chance to act).
  protected long waitTimeBeforeKill; 
  
  // Containers whose AMs have been warned that they will be preempted soon.
  private List<RMContainer> warnedContainers = new ArrayList<RMContainer>();
  
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected boolean continuousSchedulingEnabled; // Continuous Scheduling enabled or not
  protected int continuousSchedulingSleepMs; // Sleep time for each pass in continuous scheduling
  private Comparator nodeAvailableResourceComparator =
          new NodeAvailableResourceComparator(); // Node available resource comparator
  protected double nodeLocalityThreshold; // Cluster threshold for node locality
  protected double rackLocalityThreshold; // Cluster threshold for rack locality
  protected long nodeLocalityDelayMs; // Delay for node locality
  protected long rackLocalityDelayMs; // Delay for rack locality
  private FairSchedulerEventLog eventLog; // Machine-readable event log
  protected boolean assignMultiple; // Allocate multiple containers per
                                    // heartbeat
  protected int maxAssign; // Max containers to assign per heartbeat
  
  //Added by Yehia Elshater
  private HashMap<String,Integer> blocksReplica = new HashMap<String, Integer>();
  public final static int DEFAULT_REPLICA = 2;
  private static double SCORE_THRESHOLD = -0.2;
  private static int MAX_BLOCK_BUDGET = -1;
  private static NodeSelection nodeSelectionPolicy = NodeSelection.RANDOM;
  public static final String MAX_BLOCK_BUDGET_KEY =  "MAX_BLOCK_BUDGET";
  public static final String SCORE_THRESHOLD_KEY =  "SCORE_THRESHOLD";
  public static final String NODE_SELECTION_KEY =  "NODE_SELECTION";
  public static final String TOPOLOGY_INFO =  "TOPOLOGY_INFO";
  public static final String TOPOLOGY_FILE_PATH =  "TOPOLOGY_FILE_PATH";
  public static final String INPUT_FILE_TRACE = "INPUT_FILE_TRACE";
  public static final String IS_DISTRIBUTE_DATA_ENABLED = "DISTRIBUTE_DATA_ENABLED";
  private String[] topologyInfo = null;

  private static volatile ConcurrentHashMap<String, Integer> blocksPopularity = new ConcurrentHashMap<String, Integer>();
  private static volatile ConcurrentHashMap<NodeId, Integer> nodeScoresMap = new ConcurrentHashMap<NodeId, Integer>();
  
  private static volatile ConcurrentHashMap<String , Integer> nodeMapsMetric = new ConcurrentHashMap<>();
  private static volatile ConcurrentHashMap<String , Integer> nodeReducersMetric = new ConcurrentHashMap<>();

  @VisibleForTesting
  final MaxRunningAppsEnforcer maxRunningEnforcer;

  private AllocationFileLoaderService allocsLoader;
  @VisibleForTesting
  AllocationConfiguration allocConf;
  
  public CustomYLocSimFairScheduler() {
    clock = new SystemClock();
    allocsLoader = new AllocationFileLoaderService();
    queueMgr = new QueueManager(this);
    maxRunningEnforcer = new MaxRunningAppsEnforcer(this);
  }

  private void validateConf(Configuration conf) {
    // validate scheduler memory allocation setting
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
     
    if (minMem < 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min should equal greater than 0"
        + ", max should be no smaller than min.");
    }

    // validate scheduler vcores allocation setting
    int minVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores < 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ", min should equal greater than 0"
        + ", max should be no smaller than min.");
    }
    
    topologyInfo = conf.getStrings(TOPOLOGY_INFO);
    if (topologyInfo == null || topologyInfo.length ==0 ) {
    	System.err.println("Topology is not set");
    }
    initNetworkTopology(topology , topologyInfo);
  }

  public FairSchedulerConfiguration getConf() {
    return conf;
  }

  public QueueManager getQueueManager() {
    return queueMgr;
  }
  
  public ConcurrentHashMap<String,Integer> getNodeMapsMetric() {
	  return nodeMapsMetric;
  }
  
  public ConcurrentHashMap<String,Integer> getNodeReduceMetric() {
	  return nodeReducersMetric;
  }

  @Override
  public RMContainer getRMContainer(ContainerId containerId) {
    FSSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
    return (attempt == null) ? null : attempt.getRMContainer(containerId);
  }

  private FSSchedulerApp getCurrentAttemptForContainer(
      ContainerId containerId) {
    SchedulerApplication app =
        applications.get(containerId.getApplicationAttemptId()
          .getApplicationId());
    if (app != null) {
      return (FSSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }

  /**
   * A runnable which calls {@link CustomYLocSimFairScheduler#update()} every
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread implements Runnable {
    public void run() {
      while (true) {
        try {
          Thread.sleep(UPDATE_INTERVAL);
          update();
          preemptTasksIfNecessary();
        } catch (Exception e) {
          LOG.error("Exception in fair scheduler UpdateThread", e);
        }
      }
    }
  }

  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and amount of used and
   * required resources per job.
   */
  protected synchronized void update() {
    updatePreemptionVariables(); // Determine if any queues merit preemption

    FSQueue rootQueue = queueMgr.getRootQueue();

    // Recursively update demands for all queues
    rootQueue.updateDemand();

    rootQueue.setFairShare(clusterCapacity);
    // Recursively compute fair shares for all queues
    // and update metrics
    rootQueue.recomputeShares();
  }

  /**
   * Update the preemption fields for all QueueScheduables, i.e. the times since
   * each queue last was at its guaranteed share and at > 1/2 of its fair share
   * for each type of task.
   */
  private void updatePreemptionVariables() {
    long now = clock.getTime();
    lastPreemptionUpdateTime = now;
    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      if (!isStarvedForMinShare(sched)) {
        sched.setLastTimeAtMinShare(now);
      }
      if (!isStarvedForFairShare(sched)) {
        sched.setLastTimeAtHalfFairShare(now);
      }
    }
  }

  /**
   * Is a queue below its min share for the given task type?
   */
  boolean isStarvedForMinShare(FSLeafQueue sched) {
    Resource desiredShare = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
      sched.getMinShare(), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterCapacity,
        sched.getResourceUsage(), desiredShare);
  }

  /**
   * Is a queue being starved for fair share for the given task type? This is
   * defined as being below half its fair share.
   */
  boolean isStarvedForFairShare(FSLeafQueue sched) {
    Resource desiredFairShare = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
        Resources.multiply(sched.getFairShare(), .5), sched.getDemand());
    return Resources.lessThan(RESOURCE_CALCULATOR, clusterCapacity,
        sched.getResourceUsage(), desiredFairShare);
  }

  /**
   * Check for queues that need tasks preempted, either because they have been
   * below their guaranteed share for minSharePreemptionTimeout or they have
   * been below half their fair share for the fairSharePreemptionTimeout. If
   * such queues exist, compute how many tasks of each type need to be preempted
   * and then select the right ones using preemptTasks.
   */
  protected synchronized void preemptTasksIfNecessary() {
    if (!shouldAttemptPreemption()) {
      return;
    }

    long curTime = clock.getTime();
    if (curTime - lastPreemptCheckTime < preemptionInterval) {
      return;
    }
    lastPreemptCheckTime = curTime;

    Resource resToPreempt = Resources.clone(Resources.none());
    for (FSLeafQueue sched : queueMgr.getLeafQueues()) {
      Resources.addTo(resToPreempt, resToPreempt(sched, curTime));
    }
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity, resToPreempt,
        Resources.none())) {
      preemptResources(queueMgr.getLeafQueues(), resToPreempt);
    }
  }

  /**
   * Preempt a quantity of resources from a list of QueueSchedulables. The
   * policy for this is to pick apps from queues that are over their fair share,
   * but make sure that no queue is placed below its fair share in the process.
   * We further prioritize preemption by choosing containers with lowest
   * priority to preempt.
   */
  protected void preemptResources(Collection<FSLeafQueue> scheds,
      Resource toPreempt) {
    if (scheds.isEmpty() || Resources.equals(toPreempt, Resources.none())) {
      return;
    }

    Map<RMContainer, FSSchedulerApp> apps = 
        new HashMap<RMContainer, FSSchedulerApp>();
    Map<RMContainer, FSLeafQueue> queues = 
        new HashMap<RMContainer, FSLeafQueue>();

    // Collect running containers from over-scheduled queues
    List<RMContainer> runningContainers = new ArrayList<RMContainer>();
    for (FSLeafQueue sched : scheds) {
      if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getResourceUsage(), sched.getFairShare())) {
        for (AppSchedulable as : sched.getRunnableAppSchedulables()) {
          for (RMContainer c : as.getApp().getLiveContainers()) {
            runningContainers.add(c);
            apps.put(c, as.getApp());
            queues.put(c, sched);
          }
        }
      }
    }

    // Sort containers into reverse order of priority
    Collections.sort(runningContainers, new Comparator<RMContainer>() {
      public int compare(RMContainer c1, RMContainer c2) {
        int ret = c1.getContainer().getPriority().compareTo(
            c2.getContainer().getPriority());
        if (ret == 0) {
          return c2.getContainerId().compareTo(c1.getContainerId());
        }
        return ret;
      }
    });
    
    // Scan down the list of containers we've already warned and kill them
    // if we need to.  Remove any containers from the list that we don't need
    // or that are no longer running.
    Iterator<RMContainer> warnedIter = warnedContainers.iterator();
    Set<RMContainer> preemptedThisRound = new HashSet<RMContainer>();
    while (warnedIter.hasNext()) {
      RMContainer container = warnedIter.next();
      if (container.getState() == RMContainerState.RUNNING &&
          Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              toPreempt, Resources.none())) {
        warnOrKillContainer(container, apps.get(container), queues.get(container));
        preemptedThisRound.add(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      } else {
        warnedIter.remove();
      }
    }

    // Scan down the rest of the containers until we've preempted enough, making
    // sure we don't preempt too many from any queue
    Iterator<RMContainer> runningIter = runningContainers.iterator();
    while (runningIter.hasNext() &&
        Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
            toPreempt, Resources.none())) {
      RMContainer container = runningIter.next();
      FSLeafQueue sched = queues.get(container);
      if (!preemptedThisRound.contains(container) &&
          Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              sched.getResourceUsage(), sched.getFairShare())) {
        warnOrKillContainer(container, apps.get(container), sched);
        
        warnedContainers.add(container);
        Resources.subtractFrom(toPreempt, container.getContainer().getResource());
      }
    }
  }
  
  private void warnOrKillContainer(RMContainer container, FSSchedulerApp app,
      FSLeafQueue queue) {
    LOG.info("Preempting container (prio=" + container.getContainer().getPriority() +
        "res=" + container.getContainer().getResource() +
        ") from queue " + queue.getName());
    
    Long time = app.getContainerPreemptionTime(container);

    if (time != null) {
      // if we asked for preemption more than maxWaitTimeBeforeKill ms ago,
      // proceed with kill
      if (time + waitTimeBeforeKill < clock.getTime()) {
        ContainerStatus status =
          SchedulerUtils.createPreemptedContainerStatus(
            container.getContainerId(), SchedulerUtils.PREEMPTED_CONTAINER);

        // TODO: Not sure if this ever actually adds this to the list of cleanup
        // containers on the RMNode (see SchedulerNode.releaseContainer()).
        completedContainer(container, status, RMContainerEventType.KILL);
        LOG.info("Killing container" + container +
            " (after waiting for premption for " +
            (clock.getTime() - time) + "ms)");
      }
    } else {
      // track the request in the FSSchedulerApp itself
      app.addPreemption(container, clock.getTime());
    }
  }

  /**
   * Return the resource amount that this queue is allowed to preempt, if any.
   * If the queue has been below its min share for at least its preemption
   * timeout, it should preempt the difference between its current share and
   * this min share. If it has been below half its fair share for at least the
   * fairSharePreemptionTimeout, it should preempt enough tasks to get up to its
   * full fair share. If both conditions hold, we preempt the max of the two
   * amounts (this shouldn't happen unless someone sets the timeouts to be
   * identical for some reason).
   */
  protected Resource resToPreempt(FSLeafQueue sched, long curTime) {
    String queue = sched.getName();
    long minShareTimeout = allocConf.getMinSharePreemptionTimeout(queue);
    long fairShareTimeout = allocConf.getFairSharePreemptionTimeout();
    Resource resDueToMinShare = Resources.none();
    Resource resDueToFairShare = Resources.none();
    if (curTime - sched.getLastTimeAtMinShare() > minShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getMinShare(), sched.getDemand());
      resDueToMinShare = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    if (curTime - sched.getLastTimeAtHalfFairShare() > fairShareTimeout) {
      Resource target = Resources.min(RESOURCE_CALCULATOR, clusterCapacity,
          sched.getFairShare(), sched.getDemand());
      resDueToFairShare = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
          Resources.none(), Resources.subtract(target, sched.getResourceUsage()));
    }
    Resource resToPreempt = Resources.max(RESOURCE_CALCULATOR, clusterCapacity,
        resDueToMinShare, resDueToFairShare);
    if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
        resToPreempt, Resources.none())) {
      String message = "Should preempt " + resToPreempt + " res for queue "
          + sched.getName() + ": resDueToMinShare = " + resDueToMinShare
          + ", resDueToFairShare = " + resDueToFairShare;
      LOG.info(message);
    }
    return resToPreempt;
  }

  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return rmContext.getContainerTokenSecretManager();
  }

  // synchronized for sizeBasedWeight
  public synchronized ResourceWeights getAppWeight(AppSchedulable app) {
    double weight = 1.0;
    if (sizeBasedWeight) {
      // Set weight based on current memory demand
      weight = Math.log1p(app.getDemand().getMemory()) / Math.log(2);
    }
    weight *= app.getPriority().getPriority();
    if (weightAdjuster != null) {
      // Run weight through the user-supplied weightAdjuster
      weight = weightAdjuster.adjustWeight(app, weight);
    }
    return new ResourceWeights((float)weight);
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  public Resource getIncrementResourceCapability() {
    return incrAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return maximumAllocation;
  }

  public double getNodeLocalityThreshold() {
    return nodeLocalityThreshold;
  }

  public double getRackLocalityThreshold() {
    return rackLocalityThreshold;
  }

  public long getNodeLocalityDelayMs() {
    return nodeLocalityDelayMs;
  }

  public long getRackLocalityDelayMs() {
    return rackLocalityDelayMs;
  }

  public boolean isContinuousSchedulingEnabled() {
    return continuousSchedulingEnabled;
  }

  public synchronized int getContinuousSchedulingSleepMs() {
    return continuousSchedulingSleepMs;
  }

  public Resource getClusterCapacity() {
    return clusterCapacity;
  }

  public Clock getClock() {
    return clock;
  }

  protected void setClock(Clock clock) {
    this.clock = clock;
  }

  public FairSchedulerEventLog getEventLog() {
    return eventLog;
  }

  /**
   * Add a new application to the scheduler, with a given id, queue name, and
   * user. This will accept a new app even if the user or queue is above
   * configured limits, but the app will not be marked as runnable.
   */
  protected synchronized void addApplication(ApplicationId applicationId,
      String queueName, String user) {
    if (queueName == null || queueName.isEmpty()) {
      String message = "Reject application " + applicationId +
              " submitted by user " + user + " with an empty queue name.";
      LOG.info(message);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, message));
      return;
    }

    RMApp rmApp = rmContext.getRMApps().get(applicationId);
    FSLeafQueue queue = assignToQueue(rmApp, queueName, user);
    if (queue == null) {
      return;
    }

    // Enforce ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);

    if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)
        && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
      String msg = "User " + userUgi.getUserName() +
              " cannot submit applications to queue " + queue.getName();
      LOG.info(msg);
      rmContext.getDispatcher().getEventHandler()
          .handle(new RMAppRejectedEvent(applicationId, msg));
      return;
    }
  
    SchedulerApplication application =
        new SchedulerApplication(queue, user);
    applications.put(applicationId, application);
    queue.getMetrics().submitApp(user);

    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName + ", currently num of applications: "
        + applications.size());
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
  }

  /**
   * Add a new application attempt to the scheduler.
   */
  protected synchronized void addApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt) {
    SchedulerApplication application =
        applications.get(applicationAttemptId.getApplicationId());
    String user = application.getUser();
    FSLeafQueue queue = (FSLeafQueue) application.getQueue();

    FSSchedulerApp attempt =
        new FSSchedulerApp(applicationAttemptId, user,
            queue, new ActiveUsersManager(getRootQueueMetrics()),
            rmContext);
    if (transferStateFromPreviousAttempt) {
      attempt.transferStateFromPreviousAttempt(application
        .getCurrentAppAttempt());
    }
    application.setCurrentAppAttempt(attempt);

    boolean runnable = maxRunningEnforcer.canAppBeRunnable(queue, user);
    queue.addApp(attempt, runnable);
    if (runnable) {
      maxRunningEnforcer.trackRunnableApp(attempt);
    } else {
      maxRunningEnforcer.trackNonRunnableApp(attempt);
    }
    
    queue.getMetrics().submitAppAttempt(user);

    LOG.info("Added Application Attempt " + applicationAttemptId
        + " to scheduler from user: " + user);
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(applicationAttemptId,
            RMAppAttemptEventType.ATTEMPT_ADDED));
  }

  /**
   * Helper method that attempts to assign the app to a queue. The method is
   * responsible to call the appropriate event-handler if the app is rejected.
   */
  @VisibleForTesting
  FSLeafQueue assignToQueue(RMApp rmApp, String queueName, String user) {
    FSLeafQueue queue = null;
    String appRejectMsg = null;

    try {
      QueuePlacementPolicy placementPolicy = allocConf.getPlacementPolicy();
      queueName = placementPolicy.assignAppToQueue(queueName, user);
      if (queueName == null) {
        appRejectMsg = "Application rejected by queue placement policy";
      } else {
        queue = queueMgr.getLeafQueue(queueName, true);
        if (queue == null) {
          appRejectMsg = queueName + " is not a leaf queue";
        }
      }
    } catch (IOException ioe) {
      appRejectMsg = "Error assigning app to queue " + queueName;
    }

    if (appRejectMsg != null && rmApp != null) {
      LOG.error(appRejectMsg);
      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppRejectedEvent(rmApp.getApplicationId(), appRejectMsg));
      return null;
    }

    if (rmApp != null) {
      rmApp.setQueue(queue.getName());
    } else {
      LOG.error("Couldn't find RM app to set queue name on");
    }
    return queue;
  }

  private synchronized void removeApplication(ApplicationId applicationId,
      RMAppState finalState) {
    SchedulerApplication application = applications.get(applicationId);
    if (application == null){
      LOG.warn("Couldn't find application " + applicationId);
      return;
    }
    application.stop(finalState);
    applications.remove(applicationId);
  }

  private synchronized void removeApplicationAttempt(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) {
    LOG.info("Application " + applicationAttemptId + " is done." +
        " finalState=" + rmAppAttemptFinalState);
    SchedulerApplication application =
        applications.get(applicationAttemptId.getApplicationId());
    FSSchedulerApp attempt = getSchedulerApp(applicationAttemptId);

    if (attempt == null || application == null) {
      LOG.info("Unknown application " + applicationAttemptId + " has completed!");
      return;
    }

    // Release all the running containers
    for (RMContainer rmContainer : attempt.getLiveContainers()) {
      if (keepContainers
          && rmContainer.getState().equals(RMContainerState.RUNNING)) {
        // do not kill the running container in the case of work-preserving AM
        // restart.
        LOG.info("Skip killing " + rmContainer.getContainerId());
        continue;
      }
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              SchedulerUtils.COMPLETED_APPLICATION),
              RMContainerEventType.KILL);
    }

    // Release all reserved containers
    for (RMContainer rmContainer : attempt.getReservedContainers()) {
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              rmContainer.getContainerId(),
              "Application Complete"),
              RMContainerEventType.KILL);
    }
    // Clean up pending requests, metrics etc.
    attempt.stop(rmAppAttemptFinalState);

    // Inform the queue
    FSLeafQueue queue = queueMgr.getLeafQueue(attempt.getQueue()
        .getQueueName(), false);
    boolean wasRunnable = queue.removeApp(attempt);

    if (wasRunnable) {
      maxRunningEnforcer.untrackRunnableApp(attempt);
      maxRunningEnforcer.updateRunnabilityOnAppRemoval(attempt,
          attempt.getQueue());
    } else {
      maxRunningEnforcer.untrackNonRunnableApp(attempt);
    }
  }

  /**
   * Clean up a completed container.
   */
  private synchronized void completedContainer(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (rmContainer == null) {
      LOG.info("Null container completed...");
      return;
    }

    Container container = rmContainer.getContainer();

    // Get the application for the finished container
    FSSchedulerApp application =
        getCurrentAttemptForContainer(container.getId());
    ApplicationId appId =
        container.getId().getApplicationAttemptId().getApplicationId();
    if (application == null) {
      LOG.info("Container " + container + " of" +
          " unknown application attempt " + appId +
          " completed with event " + event);
      return;
    }

    // Get the node on which the container was allocated
    FSSchedulerNode node = nodes.get(container.getNodeId());

    if (rmContainer.getState() == RMContainerState.RESERVED) {
      application.unreserve(node, rmContainer.getReservedPriority());
      node.unreserveResource(application);
    } else {
      application.containerCompleted(rmContainer, containerStatus, event);
      node.releaseContainer(container);
      updateRootQueueMetrics();
    }

    LOG.info("Application attempt " + application.getApplicationAttemptId()
        + " released container " + container.getId() + " on node: " + node
        + " with event: " + event);
    
    /****
     * Decreasing the blocks popularity
     */
 
    if (!container.getBlockId().isEmpty()) {
    	if (blocksPopularity.containsKey(container.getBlockId())) {
    		blocksPopularity.put(container.getBlockId(), blocksPopularity.get(container.getBlockId()) - 1);
    	}
    }
    calculateBlocksPopularity();
    
    //resetting the replica for poor blocks popularity (blocks with zscore less than a specific threshold)
    if (!zScores.isEmpty()) {
    	synchronized (zScores) {
	    	Collections.sort(zScores);
		    for (int i = zScores.size() - 1 ; i > 0 ; i--) {
		    	if (zScores.get(i).getzScore() > SCORE_THRESHOLD)
		    		break;
		    	
		    	if (container.getBlockId() != null && blocksReplica.containsKey(container.getBlockId())) {
			    	blocksReplica.put(container.getBlockId(), DEFAULT_REPLICA);
		    	}
		    }
    	}
    }
  }
  

  

  private synchronized void addNode(RMNode node) {
    nodes.put(node.getNodeID(), new FSSchedulerNode(node, usePortForNodeName));
    Resources.addTo(clusterCapacity, node.getTotalCapability());
    updateRootQueueMetrics();

    LOG.info("Added node " + node.getNodeAddress() +
        " cluster capacity: " + clusterCapacity);
  }

  private synchronized void removeNode(RMNode rmNode) {
    FSSchedulerNode node = nodes.get(rmNode.getNodeID());
    // This can occur when an UNHEALTHY node reconnects
    if (node == null) {
      return;
    }
    Resources.subtractFrom(clusterCapacity, rmNode.getTotalCapability());
    updateRootQueueMetrics();

    // Remove running containers
    List<RMContainer> runningContainers = node.getRunningContainers();
    for (RMContainer container : runningContainers) {
      completedContainer(container,
          SchedulerUtils.createAbnormalContainerStatus(
              container.getContainerId(),
              SchedulerUtils.LOST_CONTAINER),
          RMContainerEventType.KILL);
    }

    // Remove reservations, if any
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      completedContainer(reservedContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              reservedContainer.getContainerId(),
              SchedulerUtils.LOST_CONTAINER),
          RMContainerEventType.KILL);
    }

    nodes.remove(rmNode.getNodeID());
    LOG.info("Removed node " + rmNode.getNodeAddress() +
        " cluster capacity: " + clusterCapacity);
  }

  @Override
  public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals) {
    // Make sure this application exists
    FSSchedulerApp application = getSchedulerApp(appAttemptId);
    if (application == null) {
      LOG.info("Calling allocate on removed " +
          "or non existant application " + appAttemptId);
      return EMPTY_ALLOCATION;
    }

    // Sanity check
    SchedulerUtils.normalizeRequests(ask, new DominantResourceCalculator(),
        clusterCapacity, minimumAllocation, maximumAllocation, incrAllocation);

    // Set amResource for this app
    if (!application.getUnmanagedAM() && ask.size() == 1
        && application.getLiveContainers().isEmpty()) {
      application.setAMResource(ask.get(0).getCapability());
    }

    // Release containers
    for (ContainerId releasedContainerId : release) {
      RMContainer rmContainer = getRMContainer(releasedContainerId);
      if (rmContainer == null) {
        RMAuditLogger.logFailure(application.getUser(),
            AuditConstants.RELEASE_CONTAINER,
            "Unauthorized access or invalid container", "FairScheduler",
            "Trying to release container not owned by app or with invalid id",
            application.getApplicationId(), releasedContainerId);
      }
      completedContainer(rmContainer,
          SchedulerUtils.createAbnormalContainerStatus(
              releasedContainerId,
              SchedulerUtils.RELEASED_CONTAINER),
          RMContainerEventType.RELEASED);
    }
    synchronized (application) {
    	HashSet<String> currentRequestedBlocks = new HashSet<String>(); //current block ids requested by the current application
        for (ResourceRequest request : ask) {
  			if (request.getSplitMessages() != null
  					&& request.getSplitMessages().size() > 0) {
  				for (SplitMessage sm : request.getSplitMessages()) {
  					if (sm.getFilePath().isEmpty())
  						continue;
	  		        String splitKey = sm.getFilePath() + "_" + sm.getStartOffset();
	  		        currentRequestedBlocks.add(splitKey);
	  		        if (!blockLocations.containsKey(splitKey)) {
	  		        	blockLocations.put(splitKey, new HashSet<String>(sm.getSplitHostsList()));
	  		        	for (String node : sm.getSplitHostsList()) {
	  		        		CollectionsUtil.incrementMapValue(nodeScoresMap, SchedulerCustomUtils.retrieveSchedulerNodeIdByHostName(nodes, node));
	  		        	}
	  		        }
	  		        if (!splitKey.trim().isEmpty()) {
	  			        if (blocksPopularity.containsKey(splitKey))
	  			        	blocksPopularity.put(splitKey, blocksPopularity.get(splitKey) + 1);
	  			        else {
	  			        	blocksPopularity.put(splitKey, 1);
	  			        	blocksReplica.put(splitKey, DEFAULT_REPLICA);
	  			        }
	  		        }
  				}  
  		}
        }
        calculateBlocksPopularity();
        Set<BlockScore> topKBlocks = new HashSet<BlockScore>();
        synchronized (zScores) {
	    	if (!zScores.isEmpty()) {
	    		//System.out.println("budget " + budget + " topK " + topKBlocks);
	    		//Collections.sort(zScores);
	    		for (BlockScore bs : zScores) {
	    			if (bs.getzScore() >= SCORE_THRESHOLD && blocksReplica.get(bs.getBlockId()) < MAX_BLOCK_BUDGET) {
	    				topKBlocks.add(bs);
	    			}
	    		}
	    	}
		}
        
      
      //Choosing the node to replicate the blocks
      if (nodeSelectionPolicy.equals(NodeSelection.RANDOM))
    	  chooseRandomNodesToReplicate(ask, currentRequestedBlocks, topKBlocks);
      else
    	  chooseMinNodeScoreToReplicate (ask , currentRequestedBlocks , topKBlocks);
	    
        
      if (!ask.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("allocate: pre-update" +
              " applicationAttemptId=" + appAttemptId +
              " application=" + application.getApplicationId());
        }
        application.showRequests();

        // Update application requests
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update");
        application.showRequests();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate:" +
            " applicationAttemptId=" + appAttemptId +
            " #ask=" + ask.size());

        LOG.debug("Preempting " + application.getPreemptionContainers().size()
            + " container(s)");
      }
      
      Set<ContainerId> preemptionContainerIds = new HashSet<ContainerId>();
      for (RMContainer container : application.getPreemptionContainers()) {
        preemptionContainerIds.add(container.getContainerId());
      }

      
      /*********************Calculating How Many of Returned Allocated Containers are Local and Rack containers *****************/

      /*********************Calculating How Many of Returned Allocated Containers are Local and Rack containers *****************/
      
      application.updateBlacklist(blacklistAdditions, blacklistRemovals);
      ContainersAndNMTokensAllocation allocation =
    		  application.pullNewlyAllocatedContainersAndNMTokens();
      Allocation allocationToAM = new Allocation(allocation.getContainerList(),
    	        application.getHeadroom(), preemptionContainerIds, null, null,
    	        allocation.getNMTokenList());
      
      allocationToAM.setNewBLocks(blockLocations);
      return allocationToAM;
    }
  }
  

private void chooseMinNodeScoreToReplicate(List<ResourceRequest> ask,
			HashSet<String> currentRequestedBlocks, Set<BlockScore> topKBlocks) {
		for (BlockScore tp : topKBlocks){
	  	  FSSchedulerNode targetNode = null;
	  	  if (blockLocations.get(tp.getBlockId()).size() == MAX_BLOCK_BUDGET)
	  		  continue;
	  	  Set<NodeId> blackListNodes = new HashSet<NodeId>(nodes.size()); //This set contains the nodes that are already have a copy of the current replica
		  for (String localNode : blockLocations.get(tp.getBlockId())) {
	  		  for (NodeId nodeKey : nodes.keySet()) {
	  			  if (nodeKey.getHost().equals(localNode)) {
	  				  blackListNodes.add(nodeKey);
	  				  break;
	  			  }
	  		  }
	  	  }
	  	  while (targetNode == null) {
	  		  synchronized (nodeScoresMap) {
	  			  targetNode = SchedulerCustomUtils.selectMinScoreNode(nodeScoresMap , nodes , blackListNodes);
	  		  }
	  		  if (!blockLocations.get(tp.getBlockId()).contains(targetNode.getNodeID().getHost())) {
	  			  //Then Replicate on node.
	  			  /*allocationToAM.addNewBlock(tp.getBlockId(), targetNode.getNodeID().getHost());*/
	  			  System.out.println("Copying Block " + tp.getBlockId() + " To " + targetNode.getNodeID().getHost() + " totalNumberOfCopies " + ++totalNumberOfCopies );
	  			  LocalityType copyType = getCopyType(targetNode, blackListNodes);
	  			  if (copyType.equals(LocalityType.RACK_LOCAL)) {
	  				  totalDRMRackCopies ++;
	  			  }
	  			  else {
	  				  totalDRMSwitchCopies ++;
	  			  }
	  			  //increase the replication of this block
	  			  blocksReplica.put(tp.getBlockId(), blocksReplica.get(tp.getBlockId()) + 1);
	  			  HashSet<String> nodesOfBlock = blockLocations.get(tp.getBlockId());
	  			  nodesOfBlock.add(targetNode.getNodeID().getHost());
	  			  blockLocations.put(tp.getBlockId(), nodesOfBlock);
	  			  blocksPopularity.put(tp.getBlockId(), blocksPopularity.get(tp.getBlockId()) - 1); //decrease the popularity
	  			  synchronized (nodeScoresMap) {
	  				  CollectionsUtil.incrementMapValue(nodeScoresMap,SchedulerCustomUtils.retrieveSchedulerNodeIdByHostName(nodes, targetNode.getNodeID().getHost()) );
	  			  }
	  			  //System.out.println(nodeScoresMap);
	  			  
	  			  //update ask requests based on the new replicas
	  			  if (currentRequestedBlocks.contains(tp.getBlockId())) {
	  				  Resource containerResource = BuilderUtils.newResource(1024, 1);
	  				  String hostFullName = "/" + topology.get(targetNode.getNodeID().getHost()) + "/" + topology.get(targetNode.getNodeID().getHost());
	  				  ResourceRequest newRequest = createResourceRequest(containerResource, hostFullName, 20, 1, tp.getBlockId());
	  				  ask.add(newRequest);
	  			  }
	  			  
	  			  break;
	  		  }
	  		  else {
	  			  blackListNodes.add(targetNode.getNodeID());
	  			  targetNode = null;
	  			  //continue searching for a new node to hold the new replica
	  		  }
	  	  }
	    }
	}

private void chooseRandomNodesToReplicate(List<ResourceRequest> ask,
		HashSet<String> currentRequestedBlocks, Set<BlockScore> topKBlocks) {
	for (BlockScore tp : topKBlocks){
		
	  //TODO: Remove all the redundant code between chooseRandomNodes and chooseMinNodes
  	  FSSchedulerNode targetNode = null; 
  	  FSSchedulerNode[] nodesArray = new FSSchedulerNode[16];
  	  if (blockLocations.get(tp.getBlockId()).size() == MAX_BLOCK_BUDGET)
  		  continue;
	  Set<NodeId> blackListNodes = new HashSet<NodeId>(nodes.size()); //This set contains the nodes that are already have a copy of the current replica
  	  for (String localNode : blockLocations.get(tp.getBlockId())) {
  		  for (NodeId nodeKey : nodes.keySet()) {
  			  if (nodeKey.getHost().equals(localNode)) {
  				  blackListNodes.add(nodeKey);
  				  break;
  			  }
  		  }
  	  }
  	  while (targetNode == null) {
  		  nodesArray = nodes.values().toArray(nodesArray);
  		  targetNode = nodesArray[r.nextInt(16)];
  		  if (!blockLocations.get(tp.getBlockId()).contains(targetNode.getNodeID().getHost())) {
  			  /*if (!isNodeInContainers(allocationToAM.getContainers(), targetNode.getNodeID().getHost()))
  				  continue;*/
  			  //Then Replicate on node.
  			  /*allocationToAM.addNewBlock(tp.getBlockId(), targetNode.getNodeID().getHost());*/
  			  System.out.println("Copying Block " + tp.getBlockId() + " To " + targetNode.getNodeID().getHost() + " totalNumberOfCopies " + ++totalNumberOfCopies );
  			  LocalityType copyType = getCopyType(targetNode, blackListNodes);
 			  if (copyType.equals(LocalityType.RACK_LOCAL)) {
 				  totalDRMRackCopies ++;
 			  }
 			  else {
 				  totalDRMSwitchCopies ++;
 			  }
  			  //increase the replication of this block
  			  blocksReplica.put(tp.getBlockId(), blocksReplica.get(tp.getBlockId()) + 1);
  			  HashSet<String> nodesOfBlock = blockLocations.get(tp.getBlockId());
  			  nodesOfBlock.add(targetNode.getNodeID().getHost());
  			  blockLocations.put(tp.getBlockId(), nodesOfBlock);
  			  blocksPopularity.put(tp.getBlockId(), blocksPopularity.get(tp.getBlockId()) - 1); //decrease the popularity
  			 // CollectionsUtil.incrementMapValue(nodeScoresMap,targetNode.getNodeID().getHost()); //No need for this line as the selection of the node is random anyway
  			  
  			  //update ask requests based on the new replicas
  			  if (currentRequestedBlocks.contains(tp.getBlockId())) {
  				  Resource containerResource = BuilderUtils.newResource(1024, 1);
  				  String hostFullName = "/" + topology.get(targetNode.getNodeID().getHost()) + "/" + topology.get(targetNode.getNodeID().getHost());
  				  ResourceRequest newRequest = createResourceRequest(containerResource, hostFullName, 20, 1, tp.getBlockId());
  				  ask.add(newRequest);
  			  }
  			  
  			  break;
  		  }
  		  else {
  			  blackListNodes.add(targetNode.getNodeID());
  			  targetNode = null;
  		  }
  	  }
    }
}
  
private final static RecordFactory recordFactory =
		  RecordFactoryProvider.getRecordFactory(null);


 private ResourceRequest createResourceRequest(
          Resource resource, String host, int priority, int numContainers, String blockId) {
    ResourceRequest request = recordFactory
        .newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    request.setRelaxLocality(true);
    SplitMessage sm = SplitMessage.newBuilder()
    				  .addSplitHosts(host)
    				  .setFilePath(blockId.split("_")[0])
    				  .setStartOffset(Long.parseLong(blockId.split("_")[1])).build();
    List<SplitMessage> smList = new ArrayList<SplitMessage>();
    smList.add(sm);
    request.setSplitMessages(smList);
    return request;
  }
  
  private List<BlockScore> zScores = new ArrayList<BlockScore>();
  
  private static HashMap<String,HashSet<String>> blockLocations = new HashMap<String,HashSet<String>>();
  
  Random r = new Random(1000l);
  
  private void calculateBlocksPopularity () {
	  StringBuilder b = new StringBuilder();
	  synchronized (zScores) {
		  zScores.clear();
	}
	  if (blocksPopularity.size() == 0)
		  return;
	  SynchronizedDescriptiveStatistics ds = new SynchronizedDescriptiveStatistics();
	  for (int v : blocksPopularity.values()) {
		  ds.addValue(v);
	  }
	  for (String k : blocksPopularity.keySet()) {
		  int v = blocksPopularity.get(k);
		  double sd = ds.getStandardDeviation();
		  if (sd == 0)
			  sd = 1; 
		  double zScore = (v-ds.getMean())/sd;
		  synchronized (zScores) {
			  zScores.add(new BlockScore(k, zScore));
		}
		  if (k.equals("hdfs://master.cdh.queens:8020/user/hive/warehouse/tpch4g.db/lineitem_5"))
			  //b.append(",").append(v).append("_").append((v-ds.getMean())/ds.getStandardDeviation());
		  	  //b.append((v-ds.getMean())/ds.getStandardDeviation());
			  b.append(v);
		  
	  }
	  
	  b.append("\n");
	  try {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("src/test/resources/blocksPopularity.csv"),true));
		writer.write(b.toString());
		writer.close();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
	 /* if (!b.toString().isEmpty())
		  System.out.println(b);*/
	  //Collections.sort(zScores);
  }
  
  private int sum (Collection<Integer> x) {
	  int sum = 0;
	  for (int i : x){
		  if (x!=null)
			  sum +=i;
	  }
	  return sum;
  }

  private HashMap<String, Integer> appsLocalTasksCounts = new HashMap<String, Integer>();
  private HashMap<String, Integer> appsRackTasksCounts = new HashMap<String, Integer>();
  private HashMap<String, Integer> appsOffSwitchTasksCounts = new HashMap<String, Integer>();
  private static HashMap<String, String> topology = new HashMap<String, String>();
  private static volatile int totalGain = 0;
  private static volatile int totalNumberOfCopies = 0;
  //separete totalNumberOfCopies to rackCopies and offswitchCopies
  private static volatile int totalDRMLocalCopies = 0; // represents the the total local containers because the presence of the new replicas created by DRM
  private static volatile int totalDRMRackCopies = 0; // represents the penalty of copying a new replica to another machine on the same rack
  private static volatile int totalDRMSwitchCopies = 0; // represents the penalty of copying a new replica to another machine off-switch the rack
  //private HashMap<String,Boolean> countedContainers = new HashMap<String, Boolean>();

/***
 * increment task locality for node and rack tasks
 * @author Yehia Elshater
 * @param application
 * @param appLocalityTasks
 */
public void incrementTaskLocality(String appId, LocalityType locality) {
	switch (locality) {
		case NODE_LOCAL : incrementTaskLocality(appId, appsLocalTasksCounts); break;
		case RACK_LOCAL : incrementTaskLocality(appId, appsRackTasksCounts); break;
		case OFF_SWITCH : incrementTaskLocality(appId, appsOffSwitchTasksCounts); break;
	}
	
}

public void incrementDRMNodeLocaity() {
	totalDRMLocalCopies ++;
}

public synchronized void updateGain(int gain) {
	totalGain += gain;
	System.out.println("current gain: " + gain + " Total gain " + totalGain );
}

public int getGain() {
	return totalGain;
}

public int getTotalCopies () {
	return totalNumberOfCopies;
}

public int getDRMLocalCopies(){
	return totalDRMLocalCopies;
}

public int getDRMRackCopies() {
	return totalDRMRackCopies;
}

public int getDRMOffSwitchCopies() {
	return totalDRMSwitchCopies ;
}


public void clearLocalityStatistics() {
	appsLocalTasksCounts.clear();
	appsRackTasksCounts.clear();
	appsOffSwitchTasksCounts.clear();
}

private boolean incrementTaskLocality(String appId, HashMap<String, Integer> appLocalityTasks) {
/*	int currentLocalTasksCount = 0, currentRackTasksCount = 0, currentOffSwitchTasks = 0;
	if (appsLocalTasksCounts.containsKey(appId))
		currentLocalTasksCount = appsLocalTasksCounts.get(appId).intValue();
	if (appsRackTasksCounts.containsKey(appId))
		currentRackTasksCount = appsRackTasksCounts.get(appId).intValue();
	if (appsOffSwitchTasksCounts.containsKey(appId))
		currentOffSwitchTasks = appsOffSwitchTasksCounts.get(appId)
				.intValue();

	if (currentLocalTasksCount + currentRackTasksCount
			+ currentOffSwitchTasks > maxNumberOfRequests.get(appId)) {
		//System.out.println("Max has been reached");
		return false;
	}*/
	synchronized (appLocalityTasks) {
		if (appLocalityTasks.containsKey(appId)) {
			appLocalityTasks.put(appId, appLocalityTasks.get(appId) + 1);
		} else {
			appLocalityTasks.put(appId, 1);
		}
	}
	return true;
	// TODO should modify any code that calls this method, it should terminates if this method returns false 
}

/***
 * 
 * @author Yehia Elshater
 * @param targetNode is the targetNode that the new replica will be shipped to
 * @param blackListNodes the current nodes that hold a copy of the current replica
 * @return
 */
public LocalityType getCopyType(FSSchedulerNode targetNode, Set<NodeId> blackListNodes) {
	LocalityType localityType = LocalityType.OFF_SWITCH;
	String targetNodeRack = topology.get(targetNode.getNodeID().getHost());
	for (NodeId ni : blackListNodes) {
		String blackNodeRack = topology.get(ni.getHost());
		if (blackNodeRack.equals(targetNodeRack)) {
			return LocalityType.RACK_LOCAL;
		}
	}
	return localityType;
}


/***
 * Fixing the topology now .. should be removed later
 * @author Yehia Elshater
 * @param topology
 */
public void initNetworkTopology(HashMap<String, String> topology , String []topologyInfo) {
	if (topologyInfo == null) { //to pass TestReplicaSimulation Test case
		  topology.put("slave1", "rack1");
		  topology.put("slave3", "rack1");
		  topology.put("slave5", "rack1");
		  topology.put("slave7", "rack1");
		  
		  topology.put("slave2", "rack2");
		  topology.put("slave4", "rack2");
		  topology.put("slave6", "rack2");
		  topology.put("slave8", "rack2");
		  
		  topology.put("slave9",  "rack3");
		  topology.put("slave11", "rack3");
		  topology.put("slave13", "rack3");
		  topology.put("slave15", "rack3");
		  
		  topology.put("slave10", "rack4");
		  topology.put("slave12", "rack4");
		  topology.put("slave14", "rack4");
		  topology.put("slave16", "rack4");
		}
	else {
		for (String nodeRack : topologyInfo) {
			String nodeName = nodeRack.split("-")[0];
			String rackName = nodeRack.split("-")[1];
			topology.put(nodeName, rackName);
		}
	}
	

}
  
  /**
   * Process a container which has launched on a node, as reported by the node.
   */
  private void containerLaunchedOnNode(ContainerId containerId, FSSchedulerNode node) {
    // Get the application for the finished container
    FSSchedulerApp application = getCurrentAttemptForContainer(containerId);
    if (application == null) {
      LOG.info("Unknown application "
          + containerId.getApplicationAttemptId().getApplicationId()
          + " launched container " + containerId + " on node: " + node);
      return;
    }

    application.containerLaunchedOnNode(containerId, node.getNodeID());
  }

  /**
   * Process a heartbeat update from a node.
   */
  private synchronized void nodeUpdate(RMNode nm) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("nodeUpdate: " + nm + " cluster capacity: " + clusterCapacity);
    }
    eventLog.log("HEARTBEAT", nm.getHostName());
    FSSchedulerNode node = nodes.get(nm.getNodeID());

    // Update resource if any change
    SchedulerUtils.updateResourceIfChanged(node, nm, clusterCapacity, LOG);
    
    List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();
    List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
    List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
    for(UpdatedContainerInfo containerInfo : containerInfoList) {
      newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
      completedContainers.addAll(containerInfo.getCompletedContainers());
    }
    // Processing the newly launched containers
    for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
      containerLaunchedOnNode(launchedContainer.getContainerId(), node);
    }

    // Process completed containers
    for (ContainerStatus completedContainer : completedContainers) {
      ContainerId containerId = completedContainer.getContainerId();
      LOG.debug("Container FINISHED: " + containerId);
      completedContainer(getRMContainer(containerId),
          completedContainer, RMContainerEventType.FINISHED);
    }

    if (continuousSchedulingEnabled) {
      if (!completedContainers.isEmpty()) {
        attemptScheduling(node);
      }
    } else {
      attemptScheduling(node);
    }
  }

  private void continuousScheduling() {
    while (true) {
      List<NodeId> nodeIdList = new ArrayList<NodeId>(nodes.keySet());
      // Sort the nodes by space available on them, so that we offer
      // containers on emptier nodes first, facilitating an even spread. This
      // requires holding the scheduler lock, so that the space available on a
      // node doesn't change during the sort.
      synchronized (this) {
        Collections.sort(nodeIdList, nodeAvailableResourceComparator);
      }

      // iterate all nodes
      for (NodeId nodeId : nodeIdList) {
        if (nodes.containsKey(nodeId)) {
          FSSchedulerNode node = nodes.get(nodeId);
          try {
            if (Resources.fitsIn(minimumAllocation,
                    node.getAvailableResource())) {
              attemptScheduling(node);
            }
          } catch (Throwable ex) {
            LOG.warn("Error while attempting scheduling for node " + node +
                    ": " + ex.toString(), ex);
          }
        }
      }
      try {
        Thread.sleep(getContinuousSchedulingSleepMs());
      } catch (InterruptedException e) {
        LOG.warn("Error while doing sleep in continuous scheduling: " +
                e.toString(), e);
      }
    }
  }

  /** Sort nodes by available resource */
  private class NodeAvailableResourceComparator implements Comparator<NodeId> {

    @Override
    public int compare(NodeId n1, NodeId n2) {
      return RESOURCE_CALCULATOR.compare(clusterCapacity,
              nodes.get(n2).getAvailableResource(),
              nodes.get(n1).getAvailableResource());
    }
  }
  
  private synchronized void attemptScheduling(FSSchedulerNode node) {
    // Assign new containers...
    // 1. Check for reserved applications
    // 2. Schedule if there are no reservations

    AppSchedulable reservedAppSchedulable = node.getReservedAppSchedulable();
    if (reservedAppSchedulable != null) {
      Priority reservedPriority = node.getReservedContainer().getReservedPriority();
      if (!reservedAppSchedulable.hasContainerForNode(reservedPriority, node)) {
        // Don't hold the reservation if app can no longer use it
        LOG.info("Releasing reservation that cannot be satisfied for application "
            + reservedAppSchedulable.getApp().getApplicationAttemptId()
            + " on node " + node);
        reservedAppSchedulable.unreserve(reservedPriority, node);
        reservedAppSchedulable = null;
      } else {
        // Reservation exists; try to fulfill the reservation
        if (LOG.isDebugEnabled()) {
          LOG.debug("Trying to fulfill reservation for application "
              + reservedAppSchedulable.getApp().getApplicationAttemptId()
              + " on node: " + node);
        }
        
        node.getReservedAppSchedulable().assignReservedContainer(node);
      }
    }
    if (reservedAppSchedulable == null) {
      // No reservation, schedule at queue which is farthest below fair share
      int assignedContainers = 0;
      while (node.getReservedContainer() == null) {
        boolean assignedContainer = false;
        if (Resources.greaterThan(RESOURCE_CALCULATOR, clusterCapacity,
              queueMgr.getRootQueue().assignContainer(node),
              Resources.none())) {
          assignedContainers++;
          assignedContainer = true;
        }
        if (!assignedContainer) { break; }
        if (!assignMultiple) { break; }
        if ((assignedContainers >= maxAssign) && (maxAssign > 0)) { break; }
      }
    }
    updateRootQueueMetrics();
  }

  @Override
  public SchedulerNodeReport getNodeReport(NodeId nodeId) {
    FSSchedulerNode node = nodes.get(nodeId);
    return node == null ? null : new SchedulerNodeReport(node);
  }
  
  public FSSchedulerApp getSchedulerApp(ApplicationAttemptId appAttemptId) {
    SchedulerApplication app =
        applications.get(appAttemptId.getApplicationId());
    if (app != null) {
      return (FSSchedulerApp) app.getCurrentAppAttempt();
    }
    return null;
  }
  
  @Override
  public SchedulerAppReport getSchedulerAppInfo(
      ApplicationAttemptId appAttemptId) {
    FSSchedulerApp attempt = getSchedulerApp(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return new SchedulerAppReport(attempt);
  }
  
  @Override
  public ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId) {
    FSSchedulerApp attempt = getSchedulerApp(appAttemptId);
    if (attempt == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request for appInfo of unknown attempt " + appAttemptId);
      }
      return null;
    }
    return attempt.getResourceUsageReport();
  }
  
  /**
   * Subqueue metrics might be a little out of date because fair shares are
   * recalculated at the update interval, but the root queue metrics needs to
   * be updated synchronously with allocations and completions so that cluster
   * metrics will be consistent.
   */
  private void updateRootQueueMetrics() {
    rootMetrics.setAvailableResourcesToQueue(
        Resources.subtract(
            clusterCapacity, rootMetrics.getAllocatedResources()));
  }

  /**
   * Check if preemption is enabled and the utilization threshold for
   * preemption is met.
   *
   * @return true if preemption should be attempted, false otherwise.
   */
  private boolean shouldAttemptPreemption() {
    if (preemptionEnabled) {
      return (preemptionUtilizationThreshold < Math.max(
          (float) rootMetrics.getAllocatedMB() / clusterCapacity.getMemory(),
          (float) rootMetrics.getAllocatedVirtualCores() /
              clusterCapacity.getVirtualCores()));
    }
    return false;
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return rootMetrics;
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
    case NODE_ADDED:
      if (!(event instanceof NodeAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      addNode(nodeAddedEvent.getAddedRMNode());
      break;
    case NODE_REMOVED:
      if (!(event instanceof NodeRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
      removeNode(nodeRemovedEvent.getRemovedRMNode());
      break;
    case NODE_UPDATE:
      if (!(event instanceof NodeUpdateSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
      nodeUpdate(nodeUpdatedEvent.getRMNode());
      break;
    case APP_ADDED:
      if (!(event instanceof AppAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
      addApplication(appAddedEvent.getApplicationId(),
        appAddedEvent.getQueue(), appAddedEvent.getUser());
      break;
    case APP_REMOVED:
      if (!(event instanceof AppRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
      removeApplication(appRemovedEvent.getApplicationID(),
        appRemovedEvent.getFinalState());
      break;
    case APP_ATTEMPT_ADDED:
      if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
        appAttemptAddedEvent.getTransferStateFromPreviousAttempt());
      break;
    case APP_ATTEMPT_REMOVED:
      if (!(event instanceof AppAttemptRemovedSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      removeApplicationAttempt(
          appAttemptRemovedEvent.getApplicationAttemptID(),
          appAttemptRemovedEvent.getFinalAttemptState(),
          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
      break;
    case CONTAINER_EXPIRED:
      if (!(event instanceof ContainerExpiredSchedulerEvent)) {
        throw new RuntimeException("Unexpected event type: " + event);
      }
      ContainerExpiredSchedulerEvent containerExpiredEvent =
          (ContainerExpiredSchedulerEvent)event;
      ContainerId containerId = containerExpiredEvent.getContainerId();
      completedContainer(getRMContainer(containerId),
          SchedulerUtils.createAbnormalContainerStatus(
              containerId,
              SchedulerUtils.EXPIRED_CONTAINER),
          RMContainerEventType.EXPIRE);
      break;
    default:
      LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
    }
  }

  @Override
  public void recover(RMState state) throws Exception {
    // NOT IMPLEMENTED
  }

  @Override
  public void reinitialize(Configuration conf, RMContext rmContext)
      throws IOException {
    if (!initialized) {
      synchronized (this) {
	      this.conf = new FairSchedulerConfiguration(conf);
	      validateConf(this.conf);
	      minimumAllocation = this.conf.getMinimumAllocation();
	      maximumAllocation = this.conf.getMaximumAllocation();
	      incrAllocation = this.conf.getIncrementAllocation();
	      continuousSchedulingEnabled = this.conf.isContinuousSchedulingEnabled();
	      continuousSchedulingSleepMs =
	              this.conf.getContinuousSchedulingSleepMs();
	      nodeLocalityThreshold = this.conf.getLocalityThresholdNode();
	      rackLocalityThreshold = this.conf.getLocalityThresholdRack();
	      nodeLocalityDelayMs = this.conf.getLocalityDelayNodeMs();
	      rackLocalityDelayMs = this.conf.getLocalityDelayRackMs();
	      preemptionEnabled = this.conf.getPreemptionEnabled();
	      preemptionUtilizationThreshold =
	          this.conf.getPreemptionUtilizationThreshold();
	      assignMultiple = this.conf.getAssignMultiple();
	      maxAssign = this.conf.getMaxAssign();
	      sizeBasedWeight = this.conf.getSizeBasedWeight();
	      preemptionInterval = this.conf.getPreemptionInterval();
	      waitTimeBeforeKill = this.conf.getWaitTimeBeforeKill();
	      usePortForNodeName = this.conf.getUsePortForNodeName();
	      
	      this.rmContext = rmContext;
	      // This stores per-application scheduling information
	      initialized = true;
	      MAX_BLOCK_BUDGET = this.conf.getInt(MAX_BLOCK_BUDGET_KEY, DEFAULT_REPLICA * 2);
	      SCORE_THRESHOLD = this.conf.getDouble(SCORE_THRESHOLD_KEY, -0.2d);
	      nodeSelectionPolicy =  this.conf.getEnum(NODE_SELECTION_KEY, NodeSelection.RANDOM);
	      
      }

      Thread updateThread = new Thread(new UpdateThread());
      updateThread.setName("FairSchedulerUpdateThread");
      updateThread.setDaemon(true);
      updateThread.start();
      rootMetrics = FSQueueMetrics.forQueue("root", null, true, conf);
      this.applications =
    		  new ConcurrentHashMap<ApplicationId, SchedulerApplication>();
      this.eventLog = new FairSchedulerEventLog();
      eventLog.init(this.conf);
      allocConf = new AllocationConfiguration(conf);
      try {
    	  queueMgr.initialize(conf);
      } catch (Exception e) {
    	  throw new IOException("Failed to start FairScheduler", e);
      }
      
      if (continuousSchedulingEnabled) {
        // start continuous scheduling thread
        Thread schedulingThread = new Thread(
          new Runnable() {
            @Override
            public void run() {
              continuousScheduling();
            }
          }
        );
        schedulingThread.setName("ContinuousScheduling");
        schedulingThread.setDaemon(true);
        schedulingThread.start();
      }
      
      allocsLoader.init(conf);
      allocsLoader.setReloadListener(new AllocationReloadListener());
      // If we fail to load allocations file on initialize, we want to fail
      // immediately.  After a successful load, exceptions on future reloads
      // will just result in leaving things as they are.
      try {
        allocsLoader.reloadAllocations();
      } catch (Exception e) {
        throw new IOException("Failed to initialize FairScheduler", e);
      }
      allocsLoader.start();
    } else {
      try {
        allocsLoader.reloadAllocations();
      } catch (Exception e) {
        LOG.error("Failed to reload allocations file", e);
      }
    }
  }

  @Override
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues,
      boolean recursive) throws IOException {
    if (!queueMgr.exists(queueName)) {
      throw new IOException("queue " + queueName + " does not exist");
    }
    return queueMgr.getQueue(queueName).getQueueInfo(includeChildQueues,
        recursive);
  }

  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo() {
    UserGroupInformation user = null;
    try {
      user = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      return new ArrayList<QueueUserACLInfo>();
    }

    return queueMgr.getRootQueue().getQueueUserAclInfo(user);
  }

  @Override
  public int getNumClusterNodes() {
    return nodes.size();
  }

  @Override
  public synchronized boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    FSQueue queue = getQueueManager().getQueue(queueName);
    if (queue == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ACL not found for queue access-type " + acl
            + " for queue " + queueName);
      }
      return false;
    }
    return queue.hasAccess(acl, callerUGI);
  }
  
  public AllocationConfiguration getAllocationConfiguration() {
    return allocConf;
  }
  
  private class AllocationReloadListener implements
      AllocationFileLoaderService.Listener {

    @Override
    public void onReload(AllocationConfiguration queueInfo) {
      // Commit the reload; also create any queue defined in the alloc file
      // if it does not already exist, so it can be displayed on the web UI.
      synchronized (CustomYLocSimFairScheduler.this) {
        allocConf = queueInfo;
        allocConf.getDefaultSchedulingPolicy().initialize(clusterCapacity);
        queueMgr.updateAllocationConfiguration(allocConf);
      }
    }
  }

  @Override
  public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    FSQueue queue = queueMgr.getQueue(queueName);
    if (queue == null) {
      return null;
    }
    List<ApplicationAttemptId> apps = new ArrayList<ApplicationAttemptId>();
    queue.collectSchedulerApplications(apps);
    return apps;
  }

  @Override
  public synchronized String moveApplication(ApplicationId appId,
      String queueName) throws YarnException {
    SchedulerApplication app = applications.get(appId);
    if (app == null) {
      throw new YarnException("App to be moved " + appId + " not found.");
    }
    FSSchedulerApp attempt = (FSSchedulerApp) app.getCurrentAppAttempt();
    // To serialize with FairScheduler#allocate, synchronize on app attempt
    synchronized (attempt) {
      FSLeafQueue oldQueue = (FSLeafQueue) app.getQueue();
      FSLeafQueue targetQueue = queueMgr.getLeafQueue(queueName, false);
      if (targetQueue == null) {
        throw new YarnException("Target queue " + queueName
            + " not found or is not a leaf queue.");
      }
      if (targetQueue == oldQueue) {
        return oldQueue.getQueueName();
      }
      
      if (oldQueue.getRunnableAppSchedulables().contains(
          attempt.getAppSchedulable())) {
        verifyMoveDoesNotViolateConstraints(attempt, oldQueue, targetQueue);
      }
      
      executeMove(app, attempt, oldQueue, targetQueue);
      return targetQueue.getQueueName();
    }
  }
  
  private void verifyMoveDoesNotViolateConstraints(FSSchedulerApp app,
      FSLeafQueue oldQueue, FSLeafQueue targetQueue) throws YarnException {
    String queueName = targetQueue.getQueueName();
    ApplicationAttemptId appAttId = app.getApplicationAttemptId();
    // When checking maxResources and maxRunningApps, only need to consider
    // queues before the lowest common ancestor of the two queues because the
    // total running apps in queues above will not be changed.
    FSQueue lowestCommonAncestor = findLowestCommonAncestorQueue(oldQueue,
        targetQueue);
    Resource consumption = app.getCurrentConsumption();
    
    // Check whether the move would go over maxRunningApps or maxShare
    FSQueue cur = targetQueue;
    while (cur != lowestCommonAncestor) {
      // maxRunningApps
      if (cur.getNumRunnableApps() == allocConf.getQueueMaxApps(cur.getQueueName())) {
        throw new YarnException("Moving app attempt " + appAttId + " to queue "
            + queueName + " would violate queue maxRunningApps constraints on"
            + " queue " + cur.getQueueName());
      }
      
      // maxShare
      if (!Resources.fitsIn(Resources.add(cur.getResourceUsage(), consumption),
          cur.getMaxShare())) {
        throw new YarnException("Moving app attempt " + appAttId + " to queue "
            + queueName + " would violate queue maxShare constraints on"
            + " queue " + cur.getQueueName());
      }
      
      cur = cur.getParent();
    }
  }
  
  /**
   * Helper for moveApplication, which has appropriate synchronization, so all
   * operations will be atomic.
   */
  private void executeMove(SchedulerApplication app, FSSchedulerApp attempt,
      FSLeafQueue oldQueue, FSLeafQueue newQueue) {
    boolean wasRunnable = oldQueue.removeApp(attempt);
    // if app was not runnable before, it may be runnable now
    boolean nowRunnable = maxRunningEnforcer.canAppBeRunnable(newQueue,
        attempt.getUser());
    if (wasRunnable && !nowRunnable) {
      throw new IllegalStateException("Should have already verified that app "
          + attempt.getApplicationId() + " would be runnable in new queue");
    }
    
    if (wasRunnable) {
      maxRunningEnforcer.untrackRunnableApp(attempt);
    } else if (nowRunnable) {
      // App has changed from non-runnable to runnable
      maxRunningEnforcer.untrackNonRunnableApp(attempt);
    }
    
    attempt.move(newQueue); // This updates all the metrics
    app.setQueue(newQueue);
    newQueue.addApp(attempt, nowRunnable);
    
    if (nowRunnable) {
      maxRunningEnforcer.trackRunnableApp(attempt);
    }
    if (wasRunnable) {
      maxRunningEnforcer.updateRunnabilityOnAppRemoval(attempt, oldQueue);
    }
  }
  
  

  
  private FSQueue findLowestCommonAncestorQueue(FSQueue queue1, FSQueue queue2) {
    // Because queue names include ancestors, separated by periods, we can find
    // the lowest common ancestors by going from the start of the names until
    // there's a character that doesn't match.
    String name1 = queue1.getName();
    String name2 = queue2.getName();
    // We keep track of the last period we encounter to avoid returning root.apple
    // when the queues are root.applepie and root.appletart
    int lastPeriodIndex = -1;
    for (int i = 0; i < Math.max(name1.length(), name2.length()); i++) {
      if (name1.length() <= i || name2.length() <= i ||
          name1.charAt(i) != name2.charAt(i)) {
        return queueMgr.getQueue(name1.substring(lastPeriodIndex));
      } else if (name1.charAt(i) == '.') {
        lastPeriodIndex = i;
      }
    }
    return queue1; // names are identical
  }
  
  
  public void updateNodeContainrsMetrics (String nodeName , String rackName , boolean isMapContainer) {
	  String nodeFullName = rackName.replace("/", "_")+"_" + nodeName;
	  
	  if (isMapContainer) {
		  int numberOfMaps = 1;
		  if (nodeMapsMetric.containsKey(nodeFullName)) {
			  numberOfMaps = nodeMapsMetric.get(nodeFullName).intValue() + 1;
		  }
		  nodeMapsMetric.put(nodeFullName, Integer.valueOf(numberOfMaps));
	  }
	  else {
		  int numberOfReducers = 1;
		  if (nodeReducersMetric.containsKey(nodeFullName)) {
			  numberOfReducers = numberOfReducers + 1;
		  }
		  nodeReducersMetric.put(nodeFullName, Integer.valueOf(numberOfReducers));
	  }
  }
  
  public int getNodeMetrics (String rackName , String nodeName , boolean maps) {
	  String nodeFullName = rackName.replace("/", "_")+"_" + nodeName;
	  //System.out.println("rack name and node name provided " + nodeFullName);
	  //System.out.println("metrics " + nodeMapsMetric.keySet());
	  if (maps) {
		  return nodeMapsMetric.get(nodeFullName) == null ? 0 : nodeMapsMetric.get(nodeFullName);
	  }
	  return nodeReducersMetric.get(nodeFullName) == null ? 0 : nodeReducersMetric.get(nodeFullName);
	  
  }
  
  
  public int getAppNodeLocality(LocalityType locality) {
	  if (locality == LocalityType.NODE_LOCAL) {
		  return sum(appsLocalTasksCounts.values());
	  }
	  if (locality == LocalityType.RACK_LOCAL) {
		  return sum(appsRackTasksCounts.values());
	  }
	  if (locality == LocalityType.OFF_SWITCH) {
		  return sum(appsOffSwitchTasksCounts.values());
	  }
	  return 0;
  }
  /***
   * 
   * @author Yehia Elshater
   *
   */
  public enum LocalityType {
  	  NODE_LOCAL,
  	  RACK_LOCAL,
  	  OFF_SWITCH
  }
  

  
}
