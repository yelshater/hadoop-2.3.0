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

package org.apache.hadoop.yarn.sls.nodemanager;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords
        .RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.MeasuresWriter;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;

public class NMSimulator extends TaskRunner.Task {
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private DelayQueue<ContainerSimulator> containerQueue;
  private Map<ContainerId, ContainerSimulator> runningContainers;
  private List<ContainerId> amContainerList;
  // resource manager
  private ResourceManager rm;
  // heart beat response id
  private int RESPONSE_ID = 1;
  private final static Logger LOG = Logger.getLogger(NMSimulator.class);
  private String nodeId ;
  
  public void init(String nodeIdStr, int memory, int cores,
          int dispatchTime, int heartBeatInterval, ResourceManager rm)
          throws IOException, YarnException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
            heartBeatInterval);
    // create resource
    String rackHostName[] = SLSUtils.getRackHostName(nodeIdStr);
    this.nodeId = nodeIdStr;
    this.node = NodeInfo.newNodeInfo(rackHostName[0], rackHostName[1], 
                  BuilderUtils.newResource(memory, cores));
    //added for issue #32
    this.node.setRackName(rackHostName[0]);
    this.rm = rm;
    // init data structures
    completedContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers =
            new ConcurrentHashMap<ContainerId, ContainerSimulator>();
    
 /*  completedContainerList =
            new ArrayList<ContainerId>();
    releasedContainerList =
            new ArrayList<ContainerId>();
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList =
            new ArrayList<ContainerId>();
    runningContainers =
            new ConcurrentHashMap<ContainerId, ContainerSimulator>();*/
    // register NM with RM
    
    RegisterNodeManagerRequest req =
            Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    //req.setRackName( rackHostName[0]);
    RegisterNodeManagerResponse response = rm.getResourceTrackerService()
            .registerNodeManager(req);
    masterKey = response.getNMTokenMasterKey();
  }

  @Override
  public void firstStep() throws YarnException, IOException {
    // do nothing
  }
  
  
private static StringBuffer containersDistribution = new StringBuffer();
private int maxBlockBudget;
private double scoreThreshold;
private String replicaPlacementPolicy;



  @Override
  public void middleStep() {
    // we check the lifetime for each running containers
    ContainerSimulator cs = null;
    synchronized(completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        LOG.debug(MessageFormat.format("Container {0} has completed",
                cs.getId()));
        
        /*****Generating statistics about the containers distributions allocated in the current NodeManager***/
        //if (cs.getPriority() == 20){
	        containersDistribution.append(cs.getJobId()).append("^");
	        containersDistribution.append(cs.getTaskAttemptId()).append("^");
	        if (cs.getSplitId()!=null && !cs.getSplitId().isEmpty())
	        	containersDistribution.append(cs.getSplitId()).append("^");
	        else
	        	containersDistribution.append("None").append("^");
	        containersDistribution.append(nodeId.split("/")[2]).append("^");
	        containersDistribution.append(cs.getHostname().split("/")[2]).append("^");
	        containersDistribution.append(cs.getLocalityType()).append("^");
	        containersDistribution.append(maxBlockBudget).append("^");
	        containersDistribution.append(scoreThreshold).append("^");
	        containersDistribution.append(cs.getPriority()).append("^");
	        containersDistribution.append(replicaPlacementPolicy);
	        containersDistribution.append(System.getProperty("line.separator"));
	        //containersDistribution.append(cs.getPreferedLocations()).append(System.getProperty("line.separator"));
        //}
      }
    }
    
    // send heart beat
    NodeHeartbeatRequest beatRequest =
            Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);
    
    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID ++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    beatRequest.setNodeStatus(ns);
    try {
      NodeHeartbeatResponse beatResponse =
              rm.getResourceTrackerService().nodeHeartbeat(beatRequest);
      if (! beatResponse.getContainersToCleanup().isEmpty()) {
        // remove from queue
        synchronized(releasedContainerList) {
          for (ContainerId containerId : beatResponse.getContainersToCleanup()){
            if (amContainerList.contains(containerId)) {
              // AM container (not killed?, only release)
              synchronized(amContainerList) {
                amContainerList.remove(containerId);
              }
              LOG.debug(MessageFormat.format("NodeManager {0} releases " +
                      "an AM ({1}).", node.getNodeID(), containerId));
            } else {
              cs = runningContainers.remove(containerId);
              containerQueue.remove(cs);
              releasedContainerList.add(containerId);
              LOG.debug(MessageFormat.format("NodeManager {0} releases a " +
                      "container ({1}).", node.getNodeID(), containerId));
            }
          }
        }
      }
      if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
        lastStep();
      }
    } catch (YarnException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void lastStep() {
	  
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
    // add running containers
    for (ContainerSimulator container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(),
        ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
    }
    synchronized(amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId,
            ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
      }
    }
    // add complete containers
    synchronized(completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        LOG.debug(MessageFormat.format("NodeManager {0} completed" +
                " container ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized(releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        LOG.debug(MessageFormat.format("NodeManager {0} released container" +
                " ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED)); // TODO changed from aborted to success
      }
      releasedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId, 
                                             ContainerState state,
                                             int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public RMNode getNode() {
    return node;
  }

  /**
   * launch a new container with the given life time
   */
  
  /**TODO: change the signature of this method **/
  public void addNewContainer(Container container, ContainerSimulator assignedCS, long lifeTimeMS) {
    LOG.debug(MessageFormat.format("NodeManager {0} launches a new " +
            "container ({1}).", node.getNodeID(), container.getId()));
    if (lifeTimeMS != -1) {
      // normal container
      ContainerSimulator cs = new ContainerSimulator(container.getId(),
              container.getResource(), lifeTimeMS + System.currentTimeMillis(),
              lifeTimeMS);
      
      if(assignedCS !=null) {
    	  cs.setPriority(container.getPriority().getPriority());
    	  cs.setLocalityType(assignedCS.getLocalityType());
      	  cs.setJobId(assignedCS.getJobId());
      	  cs.setFileName(assignedCS.getFileName());
      	  cs.setSplitId(assignedCS.getSplitId());
      	  cs.setHostName(assignedCS.getHostname());
      	  cs.setTaskAttemptId(assignedCS.getTaskAttemptId());
      	  cs.setPreferedLocations(assignedCS.getPreferedLocations());
      	  cs.setOriginalLocality(assignedCS.getOriginalLocality());
      }
      containerQueue.add(cs);
      runningContainers.put(cs.getId(), cs);
    } else {
      // AM container
      // -1 means AMContainer
      synchronized(amContainerList) {
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized(amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized(completedContainerList) {
      completedContainerList.add(containerId);
    }
    synchronized (containersDistribution) {
		MeasuresWriter.writeMeasures("src/test/resources/containers_dist.csv", containersDistribution.toString(), true);
	}
  }

public void setMaxBudget(int maxBlockBudget) {
	this.maxBlockBudget = maxBlockBudget;
}

public void setZScoreThreshold(double scoreThreshold) {
	this.scoreThreshold = scoreThreshold;
}

public String getReplicaPlacementPolicy() {
	return replicaPlacementPolicy;
}

public void setReplicaPlacementPolicy(String replicaPlacementPolicy) {
	this.replicaPlacementPolicy = replicaPlacementPolicy;
}


}
