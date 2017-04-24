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

package org.apache.hadoop.yarn.sls.appmaster;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.CustomYLocSimFairScheduler.LocalityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.CustomYLocSimFairScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.utils.MeasuresWriter;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

@Private
@Unstable
public class MRAMSimulator extends AMSimulator {
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Maps are scheduled as soon as their requests are received. Reduces are
  scheduled when all maps have finished (not support slow-start currently).
  */
  
  public static final int PRIORITY_REDUCE = 10;
  public static final int PRIORITY_MAP = 20;
  
  // pending maps
  private LinkedList<ContainerSimulator> pendingMaps =
          new LinkedList<ContainerSimulator>();
  
  // pending failed maps
  private LinkedList<ContainerSimulator> pendingFailedMaps =
          new LinkedList<ContainerSimulator>();
  
  // scheduled maps
  private LinkedList<ContainerSimulator> scheduledMaps =
          new LinkedList<ContainerSimulator>();
  
  // assigned maps
  private Map<ContainerId, ContainerSimulator> assignedMaps =
          new HashMap<ContainerId, ContainerSimulator>();
  
  // reduces which are not yet scheduled
  private LinkedList<ContainerSimulator> pendingReduces =
          new LinkedList<ContainerSimulator>();
  
  // pending failed reduces
  private LinkedList<ContainerSimulator> pendingFailedReduces =
          new LinkedList<ContainerSimulator>();
 
  // scheduled reduces
  private LinkedList<ContainerSimulator> scheduledReduces =
          new LinkedList<ContainerSimulator>();
  
  // assigned reduces
  private Map<ContainerId, ContainerSimulator> assignedReduces =
          new HashMap<ContainerId, ContainerSimulator>();
  
  // all maps & reduces
  private LinkedList<ContainerSimulator> allMaps =
          new LinkedList<ContainerSimulator>();
  private LinkedList<ContainerSimulator> allReduces =
          new LinkedList<ContainerSimulator>();
  
  /***********Defined by @author Yehia Elshater **********************/
/*  private static HashMap<String, Integer> appsLocalTasksMap = new HashMap<String, Integer>();
  private static HashMap<String, Integer> appsRackTasksMap = new HashMap<String, Integer>();
  private static HashMap<String, Integer> appsOffTasksMap = new HashMap<String, Integer>();*/
  private HashMap<String, Integer> localResourceMap = new HashMap<String, Integer>();
  private HashMap<String, Integer> rackResourceMap = new HashMap<String, Integer>();
  private HashMap<String, Integer> offResourceMap = new HashMap<String, Integer>();
  private boolean isResourceMapInit = false;
  private HashMap<String, String> containerResourceMap = new HashMap<String, String>();
  public static Priority MAP_PRIORITY = Priority.newInstance(20);
  private static volatile int totalNodeTasksCount = 0;
  private static volatile int totalRackTasksCount = 0;
  private static volatile int totalOffTasksCount = 0;
  private static volatile int totalMapTasks = 0;
  private ResourceSchedulerWrapper schedulerWrapper;
  private static volatile ConcurrentHashMap<String, Integer> blocksPopularity = new ConcurrentHashMap<String, Integer>();
  private String MEASURES_OUTPUT_FORMAT = "%s,%s,%s,%s,%s,%s,%s %s \n";
  /***********Defined by @author Yehia Elshater **********************/
  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // waiting for AM container 
  private boolean isAMContainerRunning = false;
  private Container amContainer;
  // finished
  private boolean isFinished = false;
  // resource for AM container
  private final static int MR_AM_CONTAINER_RESOURCE_MEMORY_MB = 1024;
  private final static int MR_AM_CONTAINER_RESOURCE_VCORES = 1;

  public final Logger LOG = Logger.getLogger(MRAMSimulator.class);
 
  public void init(int id, int heartbeatInterval,
      List<ContainerSimulator> containerList, ResourceManager rm, SLSRunner se,
      long traceStartTime, long traceFinishTime, String user, String queue, 
      boolean isTracked, String oldAppId) {
    super.init(id, heartbeatInterval, containerList, rm, se, 
              traceStartTime, traceFinishTime, user, queue,
              isTracked, oldAppId);
    amtype = "mapreduce";
    
    // get map/reduce tasks
    for (ContainerSimulator cs : containerList) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        pendingMaps.add(cs);
        String splitKey = cs.getSplitId();
        if (!splitKey.trim().isEmpty()) {
	        if (blocksPopularity.containsKey(splitKey))
	        	blocksPopularity.put(splitKey, blocksPopularity.get(splitKey) + 1);
	        else
	        	blocksPopularity.put(splitKey, 1);
        }
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(cs);
      }
    }
/*    synchronized (this) {
    	totalMapTasks += pendingMaps.size();
    	//System.out.println(blocksPopularity);
    	File blocksPop = new File("src/main/resources/blocksPopularity.csv");
    	if (blocksPop.exists())
    		blocksPop.delete();
	}*/
    //System.out.println("pendingMaps for app " + this.getOldId() + " " + pendingMaps.size() + " totalPendingMaps " + totalMapTasks );
    allMaps.addAll(pendingMaps);
    allReduces.addAll(pendingReduces);
    mapTotal = pendingMaps.size();
    reduceTotal = pendingReduces.size();
    totalContainers = mapTotal + reduceTotal;
    schedulerWrapper = (ResourceSchedulerWrapper) rm.getResourceScheduler();
  }

  @Override
  public void firstStep() throws InterruptedException, YarnException, IOException {
    super.firstStep();
    
    requestAMContainer();
  }

  /**
   * send out request for AM container
   */
  protected void requestAMContainer()
          throws YarnException, IOException, InterruptedException {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    /*ResourceRequest amRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            ResourceRequest.ANY, 1, 1);*/


    
    ResourceRequest amRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            amContainerNode, 1, 1);
    amRequest.setPriority(Priority.newInstance(1));
    //System.out.println("amContainerNode " + amContainerNode);
    //amRequest.setResourceName(amContainerNode);
    amRequest.setResourceName(ResourceRequest.ANY);
    ResourceRequest amRackRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            topology.get(amContainerNode), 1, 1);
    ResourceRequest amOffRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            ResourceRequest.ANY, 1, 1);
    amRequest.setRelaxLocality(true);
    
    
    
    //ask.add(amRequest);
    //ask.add(amRackRequest);
    ask.add(amOffRequest);
    LOG.debug(MessageFormat.format("Application {0} sends out allocate " +
            "request for its AM", appId));
    final AllocateRequest request = this.createAllocateRequest(ask);

    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
            .get(appAttemptId.getApplicationId())
            .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
            new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return rm.getApplicationMasterService().allocate(request);
      }
    });
    if (response != null) {
      responseQueue.put(response);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue()
          throws InterruptedException, YarnException, IOException {
    // Check whether receive the am container
    if (!isAMContainerRunning) {
      if (!responseQueue.isEmpty()) {
        AllocateResponse response = responseQueue.take();
        if (response != null
            && !response.getAllocatedContainers().isEmpty()) {
          // Get AM container
          Container container = response.getAllocatedContainers().get(0);
          se.getNmMap().get(container.getNodeId())
              .addNewContainer(container,null, -1L);
          System.out.println("amContainer " + container.getNodeId());
          // Start AM container
          amContainer = container;
          assignedContainerSet.add(amContainer.getId().toString());
          LOG.debug(MessageFormat.format("Application {0} starts its " +
              "AM container ({1}).", appId, amContainer.getId()));
          isAMContainerRunning = true;
        }
      }
      return;
    }

    while (! responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (! response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          releasedContainers.remove(cs.getContainerId());
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one" +
                      "mapper finished ({1}).", appId, containerId));
              assignedMaps.remove(containerId);
              mapFinished ++;
              //System.out.println("mapFinished = " + mapFinished);
              finishedContainers ++;
    		 // System.out.println("Local Tasks " + (actualNumberOfContainers*2 - remaingLocalTasksCount) + " Rack Tasks "
    		//			+ (remaingLocalTasksCount - remainingRackTasksCount) + " Off Switch Tasks " + (remainingRackTasksCount - offSwitchTasksCount));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one" +
                      "reducer finished ({1}).", appId, containerId));
              assignedReduces.remove(containerId);
              reduceFinished ++;
              finishedContainers ++;
            } else {
              // am container released event
              isFinished = true;
              LOG.info(MessageFormat.format("Application {0} goes to " +
                      "finish.", appId));
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one " +
                      "mapper killed ({1}).", appId, containerId));
              System.out.println(" maps " + containerId);
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.debug(MessageFormat.format("Application {0} has one " +
                      "reducer killed ({1}).", appId, containerId));
              pendingFailedReduces.add(assignedReduces.remove(containerId));
              System.out.println(" reduce " + containerId);
            } else if (amContainer.getId().toString().equals(cs.getContainerId().toString())){
              LOG.info(MessageFormat.format("Application {0}'s AM is " +
                      "going to be killed. Restarting...", appId));
              System.out.println(" am " + containerId);
             restart();
            }
          }
        }
      }
      
      // check finished
      if (isAMContainerRunning &&
              (mapFinished == mapTotal) &&
              (reduceFinished == reduceTotal)) {
        // to release the AM container
        se.getNmMap().get(amContainer.getNodeId())
                .cleanupContainer(amContainer.getId());
        isAMContainerRunning = false;
        LOG.debug(MessageFormat.format("Application {0} sends out event " +
                "to clean up its AM container.", appId));
        isFinished = true;
        break;
      }

      /**** Commented for now ... Changing the policy of assigning containers. *****************/
      // check allocated containers
/*     for (Container container : response.getAllocatedContainers()) {
        if (! scheduledMaps.isEmpty()) {
          ContainerSimulator cs = scheduledMaps.remove();
          LOG.debug(MessageFormat.format("Application {0} starts a " +
                  "launch a mapper ({1}).", appId, container.getId()));
          assignedMaps.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime());
        } else if (! this.scheduledReduces.isEmpty()) {
          ContainerSimulator cs = scheduledReduces.remove();
          LOG.debug(MessageFormat.format("Application {0} starts a " +
                  "launch a reducer ({1}).", appId, container.getId()));
          assignedReduces.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime());
        }
      }*/
     /**** Commented for now ... Changing the policy of assigning containers. *****************/

      List<Container> allocatedContainers = response.getAllocatedContainers();
      for (Container c : allocatedContainers){
    	  containerResourceMap.put(c.getId().toString(), c.getNodeId().getHost());
    	  releasedContainers.add(c.getId());
      }
      if (amContainer!=null)
    	  releasedContainers.remove(amContainer.getId());
/*      while (! scheduledMaps.isEmpty() && !allocatedContainers.isEmpty()) {
    	  ContainerSimulator cs = scheduledMaps.element();
    	  Container container = assignContainerWithLocality(cs,allocatedContainers);
    	  if (container != null) {
    	  assignedMaps.put(container.getId(), cs);
    	  //System.out.println(cs.getPreferedLocations() + "   " + container.getNodeId().getHost() + " " + cs.getPreferedLocations().containsKey(container.getNodeId().getHost()));
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime());
          scheduledMaps.remove();
          releasedContainers.remove(container.getId());
          System.out.println(totalNodeTasksCount + " " + totalRackTasksCount + " " + totalOffTasksCount);
    	  }
    	  else {
    		 break;
    	  }
      }
      
*/    HashMap<String, HashSet<String>> newBlocks = response.getNewBlocks(); //key is block id and value is the set of nodes hosting this block (old and new)
	  synchronized (allNewBlocks) {
		  //System.out.println("processResponseQueue allNewBlocks " + allNewBlocks.size());
		  allNewBlocks.putAll(newBlocks);
	 }
      if (!scheduledMaps.isEmpty()) {
      for (Container container : allocatedContainers) {
    	  ContainerSimulator cs = assignNodeLocalContainer(container,newBlocks);
    	  if (cs!=null) {
    		  cs.setLocalityType(LocalityType.NODE_LOCAL);
    		  adjustLocalityTiming(cs , LocalityType.NODE_LOCAL);
    		  assignedMaps.put(container.getId(), cs);
    		  se.getNmMap().get(container.getNodeId())
              .addNewContainer(container, cs,cs.getLifeTime());
    		  releasedContainers.remove(container.getId());
    		  blocksPopularity.put(cs.getSplitId(), blocksPopularity.get(cs.getSplitId()) - 1);
    		  container.setBlockId(cs.getSplitId()); // This will be used by RM to decrease the popularity of this block when completed container.
    		  try {
    		  SetView<String> newNodes =  Sets.difference(newBlocks.get(cs.getSplitId()), cs.getPreferedLocations().keySet());
    		  String newNodesNames = "";
    		  for (String nn : newNodes) {
    			  newNodesNames+= "," + nn;
    		  }
    		  
    		  
    		  //update node metrics 
    		  updateNodesMetrics(topology.get(container.getNodeId().getHost()), container.getNodeId().getHost(), true);
    		  measuresOutput.append(String.format(MEASURES_OUTPUT_FORMAT, this.getOldId() , cs.getTaskAttemptId() 
    				  , cs.getSplitId() ,container.getNodeId().getHost() , topology.get(cs.getHostname()) , "Local" ,newNodes.contains(container.getNodeId().getHost()) ,newNodesNames  ));
    		  }
    		  catch (Exception x ) {
    			  System.err.println("One of the sets are null while calculating the difference " + x.getMessage());
    		  }
    	  }
    	  
      }
      
      for (Container container : allocatedContainers) {
    	  ContainerSimulator cs = assignRackContainer(container);
    	  if (cs!=null) {
    		  cs.setLocalityType(LocalityType.RACK_LOCAL);
    		  adjustLocalityTiming(cs , LocalityType.RACK_LOCAL);
    		  assignedMaps.put(container.getId(), cs);
    		  se.getNmMap().get(container.getNodeId())
              .addNewContainer(container,cs, cs.getLifeTime());
    		  releasedContainers.remove(container.getId());
    		  blocksPopularity.put(cs.getSplitId(), blocksPopularity.get(cs.getSplitId()) - 1);
    		  container.setBlockId(cs.getSplitId()); // This will be used by RM to decrease the popularity of this block when completed container.  
    		//update node metrics 
    		  updateNodesMetrics(topology.get(container.getNodeId().getHost()), container.getNodeId().getHost(), true);
    		  measuresOutput.append(String.format(MEASURES_OUTPUT_FORMAT, this.getOldId() , cs.getTaskAttemptId() 
    				  , cs.getSplitId() ,cs.getHostname() , topology.get(cs.getHostname()) , "Rack" ,"False" ,","  ));
    	  }
      }
      
      for (Container container : allocatedContainers) {
    	  ContainerSimulator cs = assignOffSwitchContainer(container);
    	  if (cs!=null) {
    		  cs.setLocalityType(LocalityType.OFF_SWITCH);
    		  adjustLocalityTiming(cs , LocalityType.OFF_SWITCH);
    		  assignedMaps.put(container.getId(), cs);
    		  se.getNmMap().get(container.getNodeId())
              .addNewContainer(container,cs, cs.getLifeTime());
    		  releasedContainers.remove(container.getId());
    		  if (!cs.getSplitId().trim().isEmpty()) {
    			  blocksPopularity.put(cs.getSplitId(), blocksPopularity.get(cs.getSplitId()) - 1);
    			  container.setBlockId(cs.getSplitId()); // This will be used by RM to decrease the popularity of this block when completed container.
    		  }
    		//update node metrics 
    		  updateNodesMetrics(topology.get(container.getNodeId().getHost()), container.getNodeId().getHost(), true);
    		  measuresOutput.append(String.format(MEASURES_OUTPUT_FORMAT, this.getOldId() , cs.getTaskAttemptId() 
    				  , cs.getSplitId() ,cs.getHostname() , topology.get(cs.getHostname()) , "Off" ,"False" ,"," ));
    	  }
      }
      //System.out.println(totalNodeTasksCount + " " + totalRackTasksCount + " " + totalOffTasksCount);
      //System.out.println(blocksPopularity);
      writeBlocksPopularity();
	}
      
      if (! this.scheduledReduces.isEmpty() && scheduledMaps.isEmpty()) {
    	  //System.out.println("Starting to schedule Reducers ...........");
	      for (Container container: allocatedContainers) {
	    	  if (assignedContainerSet.contains(container.getId().toString()) || container.getPriority().equals(MAP_PRIORITY)) {
	    		  System.err.println("ERRORR ..................");
	    		  continue;
	    	  }
	    	  ContainerSimulator cs = scheduledReduces.remove();
	            LOG.debug(MessageFormat.format("Application {0} starts a " +
	                    "launch a reducer ({1}).", appId, container.getId()));
	            assignedReduces.put(container.getId(), cs);
	            assignedContainerSet.add(container.getId().toString());
	            se.getNmMap().get(container.getNodeId())
	                    .addNewContainer(container,cs, cs.getLifeTime());
	            releasedContainers.remove(container.getId());
	          //update node metrics 
	    	   updateNodesMetrics(topology.get(container.getNodeId().getHost()), container.getNodeId().getHost(), false);
	      }
      }
    }
  }
  
  private void adjustLocalityTiming(ContainerSimulator cs, LocalityType assignedLocality) {
	if (!cs.getOriginalLocality().equals(assignedLocality)) {
		if (cs.getOriginalLocality().equals(LocalityType.NODE_LOCAL)) {
			//cs.setLifeTime(cs.getLifeTime() + SLSConfiguration.REMOTE_CONTAINER_DELAY_MS);
			//long newContainerLifeTime = cs.getLifeTime() + (long)(cs.getLifeTime() * SLSConfiguration.REMOTE_CONTAINER_DELAY_RATIO) ;
			long newContainerLifeTime = cs.getLifeTime() + SLSConfiguration.REMOTE_CONTAINER_DELAY_MS;
			cs.setLifeTime(newContainerLifeTime);
		}
		else {
			//long newContainerLifeTime = cs.getLifeTime() - (long)(cs.getLifeTime() * SLSConfiguration.REMOTE_CONTAINER_DELAY_RATIO) ;
			long newContainerLifeTime = cs.getLifeTime() - SLSConfiguration.REMOTE_CONTAINER_DELAY_MS - 1000 ;
			cs.setLifeTime(newContainerLifeTime);
		}
	}
	
}

Set<ContainerId> releasedContainers = new HashSet<ContainerId>();
  private static volatile StringBuffer measuresOutput = new StringBuffer();
  private static volatile HashMap<String,HashSet<String>> allNewBlocks = new HashMap<>();
  private synchronized void writeBlocksPopularity () {
	  StringBuilder b = new StringBuilder();
	  SynchronizedDescriptiveStatistics ds = new SynchronizedDescriptiveStatistics();
	  for (int v : blocksPopularity.values()) {
		  ds.addValue(v);
	  }
	  for (int v : blocksPopularity.values()) {
		  b.append(",").append(v).append("_").append((v-ds.getMean())/ds.getStandardDeviation());
	  }
	  
	  b.append("\n");
/*	  try {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File("src/main/resources/blocksPopularity.csv"),true));
		writer.write(b.toString());
		writer.close();
	} catch (IOException e) {
		e.printStackTrace();
	}*/
	  
  }
  
  /***
   * 
   * @author Yehia Elshater
   * @param localityType
   * @param isDRM will be true only if the node local container has been assigned because of a presence of a new replica
   */
  private synchronized void updateSchedulerLocality(LocalityType localityType, boolean isDRM) {
	  if (schedulerWrapper.getScheduler() instanceof CustomYLocSimFairScheduler) {
		  CustomYLocSimFairScheduler cScheduler = (CustomYLocSimFairScheduler) schedulerWrapper.getScheduler();
		  cScheduler.incrementTaskLocality(appId.toString(), localityType);
		  if (isDRM && localityType.equals(LocalityType.NODE_LOCAL)) {
			  cScheduler.incrementDRMNodeLocaity();
		  }
	  }
}
  
  private synchronized void updateNodesMetrics(String rackName , String nodeName , boolean isMapTask) {
	  if (schedulerWrapper.getScheduler() instanceof CustomYLocSimFairScheduler) {
		  CustomYLocSimFairScheduler cScheduler = (CustomYLocSimFairScheduler) schedulerWrapper.getScheduler();
		  cScheduler.updateNodeContainrsMetrics(nodeName, rackName, isMapTask);
	  }
  }
  private ContainerSimulator assignNodeLocalContainer(Container c, HashMap<String, HashSet<String>> newBlocks) {
	    //synchronized (assignedContainerSet) {
    	if (!c.getPriority().equals(MAP_PRIORITY)  ||assignedContainerSet.contains(c.getId().toString()) || scheduledMaps.isEmpty()){
    		return null;
    	}
		//}
	    Iterator<ContainerSimulator> scheduledMapsIterator = scheduledMaps.iterator();
		//Comment this block if you want to disable dynamic replication effect
		//Checking the new nodes with the new replicas
		scheduledMapsIterator = scheduledMaps.iterator();
		while (scheduledMapsIterator.hasNext()){
			ContainerSimulator cs = scheduledMapsIterator.next();
			if (newBlocks.containsKey(cs.getSplitId())) {
				HashSet<String> newHosts = newBlocks.get(cs.getSplitId());
				if (newHosts.contains(c.getNodeId().getHost())) {
					boolean isTotallyNewHost = false;
					for (String newHost : newHosts) {
						if (!cs.getPreferedLocations().keySet().contains(newHost) && newHost.equals(c.getNodeId().getHost())) {
							isTotallyNewHost = true;
							break;
						}
					}
					if (isTotallyNewHost) { 
						synchronized (this) {
							totalNodeTasksCount ++;
							updateSchedulerLocality(LocalityType.NODE_LOCAL,true);
							//System.out.println("Found new Replica !");
							newReplicaGain ++;
							System.out.println(this.oldAppId + " New Replica Found! " +newReplicaGain);
						}
						scheduledMapsIterator.remove();
						assignedContainerSet.add(c.getId().toString());
						return cs;
					}
				}
			}
		}
		//Comment this block if you want to disable dynamic replication effect
		
		scheduledMapsIterator = scheduledMaps.iterator();
		while (scheduledMapsIterator.hasNext()){
			ContainerSimulator cs = scheduledMapsIterator.next();
			if (cs.getPriority()!=MAP_PRIORITY.getPriority())
				continue;
			for (String preferedHost : cs.getPreferedLocations().keySet()) {
				if (preferedHost.equals(c.getNodeId().getHost())) {
					synchronized (this) {
						totalNodeTasksCount ++;
						updateSchedulerLocality(LocalityType.NODE_LOCAL,false);						
					}
					scheduledMapsIterator.remove();
					assignedContainerSet.add(c.getId().toString());
					return cs;
				}
			}
		}
		
		scheduledMapsIterator = scheduledMaps.iterator();
		while (scheduledMapsIterator.hasNext()){
			//Checking the new nodes with the new replicas
			ContainerSimulator cs = scheduledMapsIterator.next();
			if (cs.getPriority()!=MAP_PRIORITY.getPriority())
				continue;
			
		}
		
		
		return null;
	}
  
private int newReplicaGain = 0;
private ContainerSimulator assignRackContainer (Container c) {
	//synchronized (assignedContainerSet) {
	if (!c.getPriority().equals(MAP_PRIORITY)  ||assignedContainerSet.contains(c.getId().toString()) || scheduledMaps.isEmpty()){
		return null;
	}
	//}
	  Iterator<ContainerSimulator> scheduledMapsIterator = scheduledMaps.iterator();
		while (scheduledMapsIterator.hasNext()){
			ContainerSimulator cs = scheduledMapsIterator.next();
			if (cs.getPriority()!=MAP_PRIORITY.getPriority())
				continue;
			for (Entry<String, String> preferredRack : cs.getPreferedLocations().entrySet()) {
				if (preferredRack.getValue().equals(topology.get(c.getNodeId().getHost()))) {
					synchronized (this) {
						totalRackTasksCount ++;
						updateSchedulerLocality(LocalityType.RACK_LOCAL,false);
						System.out.println(cs.getSplitId() + " " + cs.getPreferedLocations().entrySet() + " " + preferredRack.getKey() + " " + preferredRack.getValue() + " " + c.getNodeId());
					}
					scheduledMapsIterator.remove();
					assignedContainerSet.add(c.getId().toString());
					return cs;
				}
			}
		}
		return null;
  }
  
  private ContainerSimulator assignOffSwitchContainer (Container c) {
	  //synchronized (assignedContainerSet) {
    	if (!c.getPriority().equals(MAP_PRIORITY)  ||assignedContainerSet.contains(c.getId().toString()) || scheduledMaps.isEmpty()){
    		return null;
    	}
		//}
	  Iterator<ContainerSimulator> scheduledMapsIterator = scheduledMaps.iterator();
		while (scheduledMapsIterator.hasNext()){
			ContainerSimulator cs = scheduledMapsIterator.next();
			if (cs.getPriority()!=MAP_PRIORITY.getPriority())
				continue;
			synchronized (this) {
				totalOffTasksCount ++;
				updateSchedulerLocality(LocalityType.OFF_SWITCH,false);
			}
			scheduledMapsIterator.remove();
			assignedContainerSet.add(c.getId().toString());
			return cs; 
		}
		return null;
  }
	
  
  /**
   * restart running because of the am container killed
   */
  private void restart()
          throws YarnException, IOException, InterruptedException {
    // clear 
    finishedContainers = 0;
    isFinished = false;
    mapFinished = 0;
    reduceFinished = 0;
    pendingFailedMaps.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    pendingFailedReduces.clear();
    pendingMaps.addAll(allMaps);
    pendingReduces.addAll(pendingReduces);
    isAMContainerRunning = false;
    amContainer = null;
    // resent am container request
    requestAMContainer();
  }

  @Override
  protected void sendContainerRequest()
          throws YarnException, IOException, InterruptedException {
    if (isFinished) {
      return;
    }

    // send out request
    List<ResourceRequest> ask = null;
    if (isAMContainerRunning) {
      if (mapFinished != mapTotal) {
        // map phase
        if (! pendingMaps.isEmpty()) {
          //System.out.println("allNewBlocks in Send Containers " + allNewBlocks.size());
          ask = packageRequests(pendingMaps, PRIORITY_MAP,allNewBlocks);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
                  "request for {1} mappers.", appId, pendingMaps.size()));
          scheduledMaps.addAll(pendingMaps);
          pendingMaps.clear();
        } else if (! pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          ask = packageRequests(pendingFailedMaps, PRIORITY_MAP, null);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
                  "requests for {1} failed mappers.", appId,
                  pendingFailedMaps.size()));
          scheduledMaps.addAll(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
        decoupleRequests(ask);
        
      } else if (reduceFinished != reduceTotal) {
        // reduce phase
        if (! pendingReduces.isEmpty()) {
          ask = packageRequests(pendingReduces, PRIORITY_REDUCE,null);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
                  "requests for {1} reducers.", appId, pendingReduces.size()));
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (! pendingFailedReduces.isEmpty()
                && scheduledReduces.isEmpty()) {
          ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE,null);
          LOG.debug(MessageFormat.format("Application {0} sends out " +
                  "request for {1} failed reducers.", appId,
                  pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
    }
    if (ask == null) {
      ask = new ArrayList<ResourceRequest>();
    }
    if (amContainer!=null)
    	releasedContainers.remove(amContainer.getId());
    //System.out.println(releasedContainers.size());
    for (ContainerId cid : releasedContainers)
    	System.out.println(cid);
    final AllocateRequest request = createAllocateRequest(ask, new ArrayList<ContainerId>( releasedContainers ));
    //System.out.println("releasedContainers.size() " + releasedContainers.size());
    //final AllocateRequest request = createAllocateRequest(ask, new ArrayList<ContainerId>());
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }

    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
            .get(appAttemptId.getApplicationId())
            .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
            new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return rm.getApplicationMasterService().allocate(request);
      }
    });
    if (response != null) {
      responseQueue.put(response);
    }
    releasedContainers.clear();

  }

private void decoupleRequests(List<ResourceRequest> ask) {
	if (isResourceMapInit) {
		return;
	}
	for (ResourceRequest mapRequest : ask) {
		assert mapRequest.getPriority() == MAP_PRIORITY;
		String currentResource = mapRequest.getResourceName();
		if (currentResource.equals(ResourceRequest.ANY))
			offResourceMap.put(ResourceRequest.ANY, mapRequest.getNumContainers());
		else if (topology.containsKey(currentResource))
			localResourceMap.put(currentResource, mapRequest.getNumContainers());
		else
			rackResourceMap.put(currentResource, mapRequest.getNumContainers());  
	}
	isResourceMapInit = true;
}

/*private boolean decrementLocality(String key, Map<String,Integer> map) {
	  if (map.containsKey(key)) {
		  int currentValue = map.get(key);
		  if (currentValue < 1)
			  return false;
		  map.put(key, currentValue - 1);
	  }
	  else {
		  map.put(key, 0);
		  return false;
	  }
	  
	  return true;
}*/

private int sumMapValues (Map <String,Integer> map) {
	  int sum = 0 ;
	  for (int x : map.values()) {
		  sum+= x;
	  }
	  return sum;
}

@Override
  protected void checkStop() {
    if (isFinished) {
      super.setEndTime(System.currentTimeMillis());
    }
  }

  @Override
  public void lastStep(){
/*	for (String key : appsLocalTasksMap.keySet()) {
    	System.out.println(key + ": Local Tasks " + appsLocalTasksMap.get(key));
    }
    for (String key : appsRackTasksMap.keySet()) {
    	System.out.println(key + ": Rack Tasks " + appsRackTasksMap.get(key));
    }
    for (String key : appsOffTasksMap.keySet()) {
    	System.out.println(key + ": Off-switch Tasks " + appsOffTasksMap.get(key));
    }*/
    super.lastStep();
    if (schedulerWrapper.getScheduler() instanceof CustomYLocSimFairScheduler) {
    	CustomYLocSimFairScheduler scheduler = (CustomYLocSimFairScheduler) schedulerWrapper.getScheduler();
    	scheduler.updateGain(newReplicaGain);
    	System.out.println("newReplicaGain in scheduler has been updated with  " + newReplicaGain + " scheduler.getGain() " + scheduler.getGain());
    }
    // clear data structures
    allMaps.clear();
    allReduces.clear();
    assignedMaps.clear();
    assignedReduces.clear();
    pendingFailedMaps.clear();
    pendingFailedReduces.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    scheduledMaps.clear();
    scheduledReduces.clear();
    responseQueue.clear();
    newReplicaGain = 0;
 
    //writing the measures
	System.out.println("Writing now ... ");
	MeasuresWriter.writeMeasures("src/test/resources/measures.csv", measuresOutput.toString(),true);
	measuresOutput = new StringBuffer(0);
	System.out.println("Finished Writing now ... ");
    
  }
  
  private Set<String> assignedContainerSet = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
 
	
}
