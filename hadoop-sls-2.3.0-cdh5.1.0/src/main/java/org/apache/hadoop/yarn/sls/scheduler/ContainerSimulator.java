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

package org.apache.hadoop.yarn.sls.scheduler;

import java.util.HashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.CustomYLocSimFairScheduler.LocalityType;

public class ContainerSimulator implements Delayed {
  // id
  private ContainerId id;
  // resource allocated
  private Resource resource;
  // end time
  private long endTime;
  // life time (ms)
  private long lifeTime;
  // host name
  private String hostname;
  // priority
  private int priority;
  // type 
  private String type;
  
  private HashMap<String,String> preferedLocations ;
  
  private LocalityType originalLocality;
  
  private String fileName ;
  private String splitId;
  
  private String taskId ;
  private String taskAttemptId;
  private String jobId;
  private LocalityType localityType;

  public String getTaskId() {
	return taskId;
}

public void setTaskId(String taskId) {
	this.taskId = taskId;
}

public String getTaskAttemptId() {
	return taskAttemptId;
}

public void setTaskAttemptId(String taskAttemptId) {
	this.taskAttemptId = taskAttemptId;
}

public String getJobId() {
	return jobId;
}

public void setJobId(String jobId) {
	this.jobId = jobId;
}

/**
   * invoked when AM schedules containers to allocate
   */
  public ContainerSimulator(Resource resource, long lifeTime,
      String hostname, int priority, String type) {
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.hostname = hostname;
    this.priority = priority;
    this.type = type;
  }
  
  /***
   * @author Yehia Elshater
   * @param resource
   * @param lifeTime
   * @param hostname
   * @param priority
   * @param type
   * @param preferredLocations
   */
  public ContainerSimulator(Resource resource, long lifeTime,
	      String hostname, int priority, String type, HashMap<String,String> preferredLocations, String originalLocality) {
	  	this(resource, lifeTime, hostname, priority, type);	    
	  	this.setPreferedLocations(preferredLocations);
		if (originalLocality.equals("NODE_LOCAL")) {
			this.originalLocality = LocalityType.NODE_LOCAL;
		}
		else if (originalLocality.equals("OFF_SWITCH")) {
			this.originalLocality = LocalityType.OFF_SWITCH;
		}
		else {
			this.originalLocality = LocalityType.RACK_LOCAL;
		}
	  }
  
  /***
   * @author Yehia Elshater
   * @param resource
   * @param lifeTime
   * @param hostname
   * @param priority
   * @param type
   * @param preferredLocations
   * @param taskId
   * @param taskAttemptId
   * @param jobId
   */
  public ContainerSimulator(Resource resource, long lifeTime,
	      String hostname, int priority, String type, HashMap<String,String> preferredLocations, String jobId, String taskId , String taskAttemptId, String originalLocality ) {
		  	this(resource, lifeTime, hostname, priority, type, preferredLocations,originalLocality);
		  	this.jobId = jobId;
		  	this.taskId = taskId;
		  	this.taskAttemptId = taskAttemptId;
	  }

  /**
   * invoke when NM schedules containers to run
   */
  public ContainerSimulator(ContainerId id, Resource resource, long endTime,
      long lifeTime) {
    this.id = id;
    this.resource = resource;
    this.endTime = endTime;
    this.lifeTime = lifeTime;
  }
  
  public Resource getResource() {
    return resource;
  }
  
  public ContainerId getId() {
    return id;
  }

  @Override
  public int compareTo(Delayed o) {
    if (!(o instanceof ContainerSimulator)) {
      throw new IllegalArgumentException(
              "Parameter must be a ContainerSimulator instance");
    }
    ContainerSimulator other = (ContainerSimulator) o;
    return (int) Math.signum(endTime - other.endTime);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(endTime - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
  }
  
  public long getLifeTime() {
    return lifeTime;
  }
  
  public void setLifeTime(long newLifeTime) {
	  this.lifeTime = newLifeTime;
  }
  
  public String getHostname() {
    return hostname;
  }
  
  public long getEndTime() {
    return endTime;
  }
  
  public int getPriority() {
    return priority;
  }
  
  public String getType() {
    return type;
  }
  
  public void setPriority(int p) {
    priority = p;
  }

public HashMap<String,String> getPreferedLocations() {
	return preferedLocations;
}

public void setPreferedLocations(HashMap<String,String> preferedLocations) {
	this.preferedLocations = preferedLocations;
}

public LocalityType getOriginalLocality() {
	return originalLocality;
}

public void setOriginalLocality(LocalityType originalLocality) {
	if (originalLocality.equals("NODE_LOCAL")) {
		this.originalLocality = LocalityType.NODE_LOCAL;
	}
	else if (originalLocality.equals("OFF_SWITCH")) {
		this.originalLocality = LocalityType.OFF_SWITCH;
	}
	else {
		this.originalLocality = LocalityType.RACK_LOCAL;
	}
}

public String getFileName() {
	return fileName;
}

public void setFileName(String fileSplitName) {
	this.fileName = fileSplitName;
}

public String getSplitId() {
	return splitId;
}

public void setSplitId(String splitId) {
	this.splitId = splitId;
}


public void setHostName(String hostname) {
	this.hostname = hostname;
}

public LocalityType getLocalityType() {
	return localityType;
}

public void setLocalityType(LocalityType localityType) {
	this.localityType = localityType;
}

}
