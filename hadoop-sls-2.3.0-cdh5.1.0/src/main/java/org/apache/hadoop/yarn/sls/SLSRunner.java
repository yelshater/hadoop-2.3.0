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
package org.apache.hadoop.yarn.sls;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.custom.SchedulerCustomUtils.NodeSelection;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.CustomYLocSimFairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.appmaster.MRAMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.types.TopologyLocation;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SLSRunner {
  // RM, Runner
  private ResourceManager rm;
  private static TaskRunner runner = new TaskRunner();
  private String[] inputTraces;
  private Configuration conf;
  private Map<String, Integer> queueAppNumMap;
  
  // NM simulator
  private HashMap<NodeId, NMSimulator> nmMap;
  private int nmMemoryMB, nmVCores;
  private String nodeFile;
  
  // AM simulator
  private int AM_ID;
  private Map<String, AMSimulator> amMap;
  private Set<String> trackedApps;
  private Map<String, Class> amClassMap;
  private static int remainingApps = 0;

  // metrics
  private String metricsOutputDir;
  private boolean printSimulation;
  

  // other simulation information
  private int numNMs, numRacks, numAMs, numTasks;
  private long maxRuntime;
  public final static Map<String, Object> simulateInfoMap =
          new HashMap<String, Object>();

  // logger
  public final static Logger LOG = Logger.getLogger(SLSRunner.class);

  // input traces, input-rumen or input-sls
  private boolean isSLS;
  
  private int MAX_BLOCK_BUDGET = 0;
  private double SCORE_THRESHOLD = 0;
  private NodeSelection nodeSelectionPolicy = NodeSelection.RANDOM;
  private String []inputFilesTraces = new String [10];
  private int t1 ; //Fair Scheduler Node Locality Threshold
  private int t2 ; //Fair Scheduler Rack Locality Threshold
  
  private int cpuCores ;
  private int memory ;
  
  private boolean isDistributeDataEnabled = true;
  
  public SLSRunner() {
	  
  }
  public SLSRunner(boolean isSLS, String inputTraces[], String nodeFile,
                   String outputDir, Set<String> trackedApps,
                   boolean printsimulation)
          throws IOException, ClassNotFoundException {
    this.isSLS = isSLS;
    this.inputTraces = inputTraces.clone();
    this.nodeFile = nodeFile;
    this.trackedApps = trackedApps;
    this.printSimulation = printsimulation;
    metricsOutputDir = outputDir;
    
    nmMap = new HashMap<NodeId, NMSimulator>();
    queueAppNumMap = new HashMap<String, Integer>();
    amMap = new HashMap<String, AMSimulator>();
    amClassMap = new HashMap<String, Class>();
    
    // runner configuration
    conf = new Configuration(false);
    conf.addResource(new FileInputStream(new File("src/main/resources/sls-runner.xml")));
    // runner
    int poolSize = conf.getInt(SLSConfiguration.RUNNER_POOL_SIZE, 
                                SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT); 
    SLSRunner.runner.setQueueSize(poolSize);
    // <AMType, Class> map
    for (Map.Entry e : conf) {
      String key = e.getKey().toString();
      if (key.startsWith(SLSConfiguration.AM_TYPE)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE.length());
        amClassMap.put(amType, Class.forName(conf.get(key)));
      }
    }
  }
  
  
  public SLSRunner (boolean isSLS, String inputTraces[], String nodeFile,
          String outputDir, Set<String> trackedApps,
          boolean printsimulation , int maxBlockBudget , double scoreThreshold , NodeSelection ns , int t1 , int t2 , boolean isDistributeDataEnabled , int cpu , int memory) throws IOException, ClassNotFoundException{
	  this(isSLS, inputTraces, nodeFile, outputDir, trackedApps, printsimulation);
	  MAX_BLOCK_BUDGET = maxBlockBudget;
	  SCORE_THRESHOLD = scoreThreshold;
	  nodeSelectionPolicy = ns;
	  inputFilesTraces = inputTraces;
	  this.t1 = t1;
	  this.t2 = t2;
	  this.isDistributeDataEnabled = isDistributeDataEnabled;
	  this.cpuCores = cpu;
	  this.memory = memory;
  }
  

  
  public void start() throws Exception {
	
	LogManager.getRootLogger().setLevel(Level.ERROR);	  
    // start resource manager
	startRM();
	
    // start node managers
	startNM();
	
    // start application masters
    startAM();
    // set queue & tracked apps information
    ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                            .setQueueSet(this.queueAppNumMap.keySet());
    ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                            .setTrackedAppSet(this.trackedApps);
    // print out simulation info
    printSimulationInfo();
    // blocked until all nodes RUNNING
    waitForNodesRunning();
    // starting the runner once everything is ready to go,
    runner.start();
  }
 
  private void startRM() throws IOException, ClassNotFoundException {
    Configuration rmConf = new YarnConfiguration();
    //String schedulerClass = rmConf.get(YarnConfiguration.RM_SCHEDULER);
    //String schedulerClass = CapacityScheduler.class.getCanonicalName();
    //String schedulerClass = CustomFairScheduler.class.getCanonicalName();
    String schedulerClass = CustomYLocSimFairScheduler.class.getCanonicalName();
    //CustomFairScheduler
    rmConf.set(SLSConfiguration.RM_SCHEDULER, schedulerClass);
    rmConf.set(YarnConfiguration.RM_SCHEDULER,
            ResourceSchedulerWrapper.class.getName());
    rmConf.set(SLSConfiguration.METRICS_OUTPUT_DIR, metricsOutputDir);
    //rmConf.set(FairSchedulerConfiguration.ALLOCATION_FILE , "src/test/resources/fair-scheduler-allocation.xml");
    rmConf.set(FairSchedulerConfiguration.ALLOCATION_FILE , "src/main/resources/fair-scheduler-allocation.xml");
    
    //rmConf.set(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY, "src/main/resources/topology.py");
    rmConf.addResource(new FileInputStream(new File("src/main/resources/yarn-site.xml")));
    
    
    rmConf.setInt("yarn.scheduler.fair.locality-delay-node-ms", t1);
    rmConf.setInt("yarn.scheduler.fair.locality-delay-rack-ms", t2);
    rmConf.setBoolean(CustomYLocSimFairScheduler.IS_DISTRIBUTE_DATA_ENABLED, isDistributeDataEnabled);
    rmConf.setInt(CustomYLocSimFairScheduler.CPU, cpuCores);
    rmConf.setInt(CustomYLocSimFairScheduler.MEMORY, memory);
    
    rmConf.setInt(CustomYLocSimFairScheduler.MAX_BLOCK_BUDGET_KEY, MAX_BLOCK_BUDGET);
    rmConf.setDouble(CustomYLocSimFairScheduler.SCORE_THRESHOLD_KEY, SCORE_THRESHOLD);
    rmConf.setEnum(CustomYLocSimFairScheduler.NODE_SELECTION_KEY, nodeSelectionPolicy);
    rmConf.set(CustomYLocSimFairScheduler.TOPOLOGY_FILE_PATH, nodeFile);
    rmConf.set(CustomYLocSimFairScheduler.INPUT_FILE_TRACE, inputFilesTraces[0].substring(inputFilesTraces[0].lastIndexOf("/")+1,inputFilesTraces[0].length()));
    
    List<TopologyLocation> nodeRackTopologyList = SLSUtils.getTopologyLocationsFromFile(nodeFile);
    String []rackNodes = new String[nodeRackTopologyList.size()];
    int i = 0;
    for (TopologyLocation tl : nodeRackTopologyList) {
    	rackNodes[i++] = tl.getNodeName() + "-" + tl.getRackName();
    }
    
    rmConf.setStrings(CustomYLocSimFairScheduler.TOPOLOGY_INFO, rackNodes);
    
    rm = new ResourceManager();
    rm.init(rmConf);
    rm.start();
    
  }

  private void startNM() throws YarnException, IOException {
    // nm configuration
	nmMemoryMB = memory;
	nmVCores = cpuCores;
	if (memory ==0 )
		nmMemoryMB = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
				SLSConfiguration.NM_MEMORY_MB_DEFAULT);
	if (cpuCores == 0)
		nmVCores = conf.getInt(SLSConfiguration.NM_VCORES,
				SLSConfiguration.NM_VCORES_DEFAULT);
    System.out.println("vmcores " + nmVCores + " nmMemoryMB " + nmMemoryMB);
    int heartbeatInterval = conf.getInt(
            SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    // nm information (fetch from topology file, or from sls/rumen json file)
    Set<String> nodeSet = new HashSet<String>();
    if (nodeFile.isEmpty()) {
      if (isSLS) {
        for (String inputTrace : inputTraces) {
          nodeSet.addAll(SLSUtils.parseNodesFromSLSTrace(inputTrace));
        }
      } else {
        for (String inputTrace : inputTraces) {
          nodeSet.addAll(SLSUtils.parseNodesFromRumenTrace(inputTrace));
        }
      }

    } else {
      nodeSet.addAll(SLSUtils.parseNodesFromNodeFile(nodeFile));
    }
    // create NM simulators
    Random random = new Random();
    Set<String> rackSet = new HashSet<String>();
    for (String hostName : nodeSet) {
      // we randomize the heartbeat start time from zero to 1 interval
      NMSimulator nm = new NMSimulator();
      nm.init(hostName, nmMemoryMB, nmVCores, 
          random.nextInt(heartbeatInterval), heartbeatInterval, rm);
      nm.setMaxBudget(MAX_BLOCK_BUDGET);
      nm.setZScoreThreshold(SCORE_THRESHOLD);
      nm.setReplicaPlacementPolicy(nodeSelectionPolicy.name());
     
      nmMap.put(nm.getNode().getNodeID(), nm);
      runner.schedule(nm);
      rackSet.add(nm.getNode().getRackName());
    }
    numRacks = rackSet.size();
    numNMs = nmMap.size();
  }

  private void waitForNodesRunning() throws InterruptedException {
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      int numRunningNodes = 0;
      for (RMNode node : rm.getRMContext().getRMNodes().values()) {
        if (node.getState() == NodeState.RUNNING) {
          numRunningNodes ++;
        }
      }
      if (numRunningNodes == numNMs) {
        break;
      }
      LOG.info(MessageFormat.format("SLSRunner is waiting for all " +
              "nodes RUNNING. {0} of {1} NMs initialized.",
              numRunningNodes, numNMs));
      Thread.sleep(1000);
    }
    LOG.info(MessageFormat.format("SLSRunner takes {0} ms to launch all nodes.",
            (System.currentTimeMillis() - startTimeMS)));
  }

  @SuppressWarnings("unchecked")
  private void startAM() throws YarnException, IOException {
    // application/container configuration
	//conf.set(YARN_APP_MAPREDUCE_AM_COMMAND_OPTS, conf.get(YARN_APP_MAPREDUCE_AM_COMMAND_OPTS) + " -agentlib:jdwp=transport=dt_socket,address=127.0.0.1:8888,server=n,suspend=n");
    int heartbeatInterval = conf.getInt(
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    int containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
            SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(SLSConfiguration.CONTAINER_VCORES,
            SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    Resource containerResource =
            BuilderUtils.newResource(containerMemoryMB, containerVCores);

    // application workload
    queueAppNumMap.clear();
    trackedApps.clear();
    if (isSLS) {
      startAMFromSLSTraces(containerResource, heartbeatInterval);
    } else {
      startAMFromRumenTraces(containerResource, heartbeatInterval);
    }
    numAMs = amMap.size();
    remainingApps = numAMs;
  }

  /**
   * parse workload information from sls trace files
   */
  static Map<String,Integer> accessCounts = new HashMap<>();
  static Map<String,Double> jobsRuntime = new HashMap<>();
  
  @SuppressWarnings("unchecked")
  private void startAMFromSLSTraces(Resource containerResource,
                                    int heartbeatInterval) throws IOException {
    // parse from sls traces
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    for (String inputTrace : inputTraces) {
      Reader input = new FileReader(inputTrace);
      try {
        Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
                Map.class);
        while (i.hasNext()) {
          Map jsonJob = i.next();

          // load job information
          long jobStartTime = Long.parseLong(
                  jsonJob.get("job.start.ms").toString());
          long jobFinishTime = Long.parseLong(
                  jsonJob.get("job.end.ms").toString());
          
          
          double jobLifeTime = (jobFinishTime - jobStartTime) / 1000;
          
          
          String user = (String) jsonJob.get("job.user");
          if (user == null)  user = "default";
          String queue = jsonJob.get("job.queue.name").toString();
          String jobName = jsonJob.get("job.name").toString(); 

          String oldAppId = jsonJob.get("job.id").toString();
          
          
          jobsRuntime.put(oldAppId, jobLifeTime);
          
          BufferedWriter tasksDurationsFile = new BufferedWriter(new FileWriter(new File("src/test/resources/tasks_durations.csv"),true));
          
          boolean isTracked = trackedApps.contains(oldAppId);
          int queueSize = queueAppNumMap.containsKey(queue) ?
                  queueAppNumMap.get(queue) : 0;
          queueSize ++;
          queueAppNumMap.put(queue, queueSize);
          // tasks
          List tasks = (List) jsonJob.get("job.tasks");
          if (tasks == null || tasks.size() == 0) {
            continue;
          }
          List<ContainerSimulator> containerList =
                  new ArrayList<ContainerSimulator>();
          for (Object o : tasks) {
            Map jsonTask = (Map) o;
            String hostname = jsonTask.get("container.host").toString();
            long taskStart = Long.parseLong(
                    jsonTask.get("container.start.ms").toString());
            long taskFinish = Long.parseLong(
                    jsonTask.get("container.end.ms").toString());
            long lifeTime = taskFinish - taskStart;
            int priority = Integer.parseInt(
                    jsonTask.get("container.priority").toString());
            String type = jsonTask.get("container.type").toString();
            HashMap<String,String> preferredLocations = (HashMap<String,String>)jsonTask.get(RumenToSLSConverter.MAP_PREFERRED_LOCATIONS);
            if (preferredLocations != null) {
	            //all the rack names should start with "/"
	            for (Map.Entry<String, String> nodeRack : preferredLocations.entrySet()) {
	            	if (!nodeRack.getValue().startsWith("/")) {
	            		preferredLocations.put(nodeRack.getKey(), "/" + nodeRack.getValue());
	            	}
	            	
	            }
            }
            String jobId = jsonTask.get(RumenToSLSConverter.TASK_JOB_JOBID).toString();
            String taskId = jsonTask.get(RumenToSLSConverter.TASK_TASKID).toString();
            String taskAttemtId = jsonTask.get(RumenToSLSConverter.TASK_ATTEMPTID).toString();
            String originalLocality = jsonTask.get(RumenToSLSConverter.CONTAINER_ORIGINAL_LOCALITY).toString();
            String fileSplitName = jsonTask.get(RumenToSLSConverter.FILE_SPLIT_ID).toString();
            String splitId ="";
            if (jsonTask.get("splitId") !=null)
            	splitId = jsonTask.get("splitId").toString();
            ContainerSimulator cs = new ContainerSimulator(containerResource,
                    lifeTime, hostname, priority, type,preferredLocations,jobId,taskId , taskAttemtId,originalLocality);
            cs.setFileName(fileSplitName);
            cs.setSplitId(splitId);
            containerList.add(cs);
            
            tasksDurationsFile.write(oldAppId + "," + taskId + "," + type + "," + lifeTime/1000 + "," + jobName + ","+ jobLifeTime + "\n");
          }
          
          tasksDurationsFile.close();
          
          //calculating the access counts
          BufferedWriter bw = new BufferedWriter(new FileWriter(new File("src/test/resources/access_counts.csv")));
          
          //System.out.println("Start writing access counts for job " + oldAppId);
          
          for (ContainerSimulator cs : containerList) {
        	  synchronized (accessCounts) {
        		  if (accessCounts.containsKey(cs.getFileName())) {
        			  int newCount = accessCounts.get(cs.getFileName()) + 1;
        			  accessCounts.put(cs.getFileName(), newCount);
        		  }
        		  else {
        			  accessCounts.put(cs.getFileName(), 1);
        		  }				
			}
          }
          for(Map.Entry<String, Integer> e : accessCounts.entrySet()) {
        	  bw.write(e.getKey() + "," + e.getValue() + "\n");
          }
          bw.close();
          
          
          // create a new AM
          String amType = jsonJob.get("am.type").toString();
          String amContainerNode = jsonJob.get("am.container.node").toString();
          Configuration amConf = new Configuration();
          //amConf.set(YARN_APP_MAPREDUCE_AM_COMMAND_OPTS, amConf.get(YARN_APP_MAPREDUCE_AM_COMMAND_OPTS) + " -agentlib:jdwp=transport=dt_socket,address=127.0.0.1:8888,server=n,suspend=n");
          AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
                  amClassMap.get(amType), amConf);
          if (amSim != null) {
        	amSim.setToplogyFilePath(nodeFile);
            amSim.init(AM_ID++, heartbeatInterval, containerList, rm,
                    this, jobStartTime, jobFinishTime, user, queue,
                    isTracked, oldAppId,amContainerNode);
            runner.schedule(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTime);
            numTasks += containerList.size();
            amMap.put(oldAppId, amSim);
          }
        }
        
        
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File ("src/test/resources/jobs_runtime.csv")));
        for (Map.Entry<String, Double> e : jobsRuntime.entrySet()) {
        	bw.write(e.getKey() + "," + e.getValue() + "\n");
        }
        bw.close();
      } finally {
        input.close();
      }
    }
  }

  /**
   * parse workload information from rumen trace files
   */
  //private static final String YARN_APP_MAPREDUCE_AM_COMMAND_OPTS = "yarn.app.mapreduce.am.command-opts";
  @SuppressWarnings("unchecked")
  private void startAMFromRumenTraces(Resource containerResource,
                                      int heartbeatInterval)
          throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    long baselineTimeMS = 0;
    for (String inputTrace : inputTraces) {
      File fin = new File(inputTrace);
      JobTraceReader reader = new JobTraceReader(
              new Path(fin.getAbsolutePath()), conf);
      try {
        LoggedJob job = null;
        while ((job = reader.getNext()) != null) {
          // only support MapReduce currently
          String jobType = "mapreduce";
          String user = job.getUser() == null ?
                  "default" : job.getUser().getValue();
          String jobQueue = job.getQueue().getValue();
          String oldJobId = job.getJobID().toString();
          long jobStartTimeMS = job.getSubmitTime();
          long jobFinishTimeMS = job.getFinishTime();
          if (baselineTimeMS == 0) {
            baselineTimeMS = jobStartTimeMS;
          }
          jobStartTimeMS -= baselineTimeMS;
          jobFinishTimeMS -= baselineTimeMS;
          if (jobStartTimeMS < 0) {
            LOG.warn("Warning: reset job " + oldJobId + " start time to 0.");
            jobFinishTimeMS = jobFinishTimeMS - jobStartTimeMS;
            jobStartTimeMS = 0;
          }

          boolean isTracked = trackedApps.contains(oldJobId);
          int queueSize = queueAppNumMap.containsKey(jobQueue) ?
                  queueAppNumMap.get(jobQueue) : 0;
          queueSize ++;
          queueAppNumMap.put(jobQueue, queueSize);

          List<ContainerSimulator> containerList =
                  new ArrayList<ContainerSimulator>();
          // map tasks
          for(LoggedTask mapTask : job.getMapTasks()) {
            LoggedTaskAttempt taskAttempt = mapTask.getAttempts()
                    .get(mapTask.getAttempts().size() - 1);
            String hostname = taskAttempt.getHostName().getValue();
            long containerLifeTime = taskAttempt.getFinishTime()
                    - taskAttempt.getStartTime();
            containerList.add(new ContainerSimulator(containerResource,
                    containerLifeTime, hostname,MRAMSimulator.PRIORITY_MAP, "map"));
          }

          // reduce tasks
          for(LoggedTask reduceTask : job.getReduceTasks()) {
            LoggedTaskAttempt taskAttempt = reduceTask.getAttempts()
                    .get(reduceTask.getAttempts().size() - 1);
            String hostname = taskAttempt.getHostName().getValue();
            long containerLifeTime = taskAttempt.getFinishTime()
                    - taskAttempt.getStartTime();
            containerList.add(new ContainerSimulator(containerResource,
                    containerLifeTime, hostname, MRAMSimulator.PRIORITY_REDUCE, "reduce"));
          }

          // create a new AM
          AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
                  amClassMap.get(jobType), conf);
          if (amSim != null) {
            amSim.init(AM_ID ++, heartbeatInterval, containerList,
                    rm, this, jobStartTimeMS, jobFinishTimeMS, user, jobQueue,
                    isTracked, oldJobId);
            runner.schedule(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTimeMS);
            numTasks += containerList.size();
            amMap.put(oldJobId, amSim);
          }
        }
      } finally {
        reader.close();
      }
    }
  }
  
  private void printSimulationInfo() {
    if (printSimulation) {
      // node
      LOG.info("------------------------------------");
      LOG.info(MessageFormat.format("# nodes = {0}, # racks = {1}, capacity " +
              "of each node {2} MB memory and {3} vcores.",
              numNMs, numRacks, nmMemoryMB, nmVCores));
      LOG.info("------------------------------------");
      // job
      LOG.info(MessageFormat.format("# applications = {0}, # total " +
              "tasks = {1}, average # tasks per application = {2}",
              numAMs, numTasks, (int)(Math.ceil((numTasks + 0.0) / numAMs))));
      LOG.info("JobId\tQueue\tAMType\tDuration\t#Tasks");
      for (Map.Entry<String, AMSimulator> entry : amMap.entrySet()) {
        AMSimulator am = entry.getValue();
        LOG.info(entry.getKey() + "\t" + am.getQueue() + "\t" + am.getAMType() 
            + "\t" + am.getDuration() + "\t" + am.getNumTasks());
      }
      LOG.info("------------------------------------");
      // queue
      LOG.info(MessageFormat.format("number of queues = {0}  average " +
              "number of apps = {1}", queueAppNumMap.size(),
              (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size()))));
      LOG.info("------------------------------------");
      // runtime
      LOG.info(MessageFormat.format("estimated simulation time is {0}" +
              " seconds", (long)(Math.ceil(maxRuntime / 1000.0))));
      LOG.info("------------------------------------");
    }
    // package these information in the simulateInfoMap used by other places
    simulateInfoMap.put("Number of racks", numRacks);
    simulateInfoMap.put("Number of nodes", numNMs);
    simulateInfoMap.put("Node memory (MB)", nmMemoryMB);
    simulateInfoMap.put("Node VCores", nmVCores);
    simulateInfoMap.put("Number of applications", numAMs);
    simulateInfoMap.put("Number of tasks", numTasks);
    simulateInfoMap.put("Average tasks per applicaion",
            (int)(Math.ceil((numTasks + 0.0) / numAMs)));
    simulateInfoMap.put("Number of queues", queueAppNumMap.size());
    simulateInfoMap.put("Average applications per queue",
            (int)(Math.ceil((numAMs + 0.0) / queueAppNumMap.size())));
    simulateInfoMap.put("Estimated simulate time (s)",
            (long)(Math.ceil(maxRuntime / 1000.0)));
  }

  public HashMap<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public static TaskRunner getRunner() {
    return runner;
  }

  public static void decreaseRemainingApps() {
	remainingApps --;
    if (remainingApps == 0) {
      LOG.info("SLSRunner tears down.");
      System.out.println("SLSRunner tears down.");
      //TODO: Just commented this temporarily to test multiple runs.
      System.exit(0); 
    }
  }
  
  public static int getRemainingApps () {
	  return remainingApps;
  }

  public static void main(String args[]) throws Exception {
    Options options = new Options();
    options.addOption("inputrumen", true, "input rumen files");
    options.addOption("inputsls", true, "input sls files");
    options.addOption("nodes", true, "input topology");
    options.addOption("output", true, "output directory");
    options.addOption("trackjobs", true,
            "jobs to be tracked during simulating");
    options.addOption("printsimulation", false,
            "print out simulation information");
    
    CommandLineParser parser = new GnuParser();
    CommandLine cmd = parser.parse(options, args);

    String inputRumen = cmd.getOptionValue("inputrumen");
    String inputSLS = cmd.getOptionValue("inputsls");
    String output = cmd.getOptionValue("output");
    
    if ((inputRumen == null && inputSLS == null) || output == null) {
      System.err.println();
      System.err.println("ERROR: Missing input or output file");
      System.err.println();
      System.err.println("Options: -inputrumen|-inputsls FILE,FILE... " +
              "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] " +
              "[-printsimulation]");
      System.err.println();
      System.exit(1);
    }
    
    File outputFile = new File(output);
    if (! outputFile.exists()
            && ! outputFile.mkdirs()) {
      System.err.println("ERROR: Cannot create output directory "
              + outputFile.getAbsolutePath());
      System.exit(1);
    }
    
    Set<String> trackedJobSet = new HashSet<String>();
    if (cmd.hasOption("trackjobs")) {
      String trackjobs = cmd.getOptionValue("trackjobs");
      String jobIds[] = trackjobs.split(",");
      trackedJobSet.addAll(Arrays.asList(jobIds));
    }
    
    String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";

    boolean isSLS = inputSLS != null;
    String inputFiles[] = isSLS ? inputSLS.split(",") : inputRumen.split(",");
    SLSRunner sls = new SLSRunner(isSLS, inputFiles, nodeFile, output,
        trackedJobSet, cmd.hasOption("printsimulation"));
    sls.start();
  }
 public static void startSLSRunner(String args[]) throws Exception {
	  Options options = new Options();
	    options.addOption("inputrumen", true, "input rumen files");
	    options.addOption("inputsls", true, "input sls files");
	    options.addOption("nodes", true, "input topology");
	    options.addOption("output", true, "output directory");
	    options.addOption("trackjobs", true,
	            "jobs to be tracked during simulating");
	    options.addOption("printsimulation", false,
	            "print out simulation information");
	    options.addOption("maxbudgetperblock", true,"The maximum budget per data block");
	    options.addOption("scorethreshold", true,"The Z-Score Threshold");
	    options.addOption("selection", true,"The Node Selection Policy");
	    options.addOption("config_file_path" , true , "YLocSim configuration files");
	    
	    options.addOption("t1" , true , "Fair Scheduler Node Locality Threshold");
	    options.addOption("t2" , true , "Fair Scheduler Rack Locality Threshold");
	    options.addOption("distribute" , true , "True if it is required to redistribute the data blocks over the new topology");
	    
	    options.addOption("cpu" , true , "CPU Cores");
	    options.addOption("memory" , true , "Memory");
	    
	  
	    
	    CommandLineParser parser = new GnuParser();
	    CommandLine cmd = parser.parse(options, args);

	    String inputRumen = cmd.getOptionValue("inputrumen");
	    String inputSLS = cmd.getOptionValue("inputsls");
	    String nodeFile = cmd.hasOption("nodes") ? cmd.getOptionValue("nodes") : "";
	    String output = cmd.getOptionValue("output");
	    
	    int MAX_BUDGET_BLOCK = Integer.parseInt(cmd.getOptionValue("maxbudgetperblock"));
	    double SCORE_THRESHOLD = Double.parseDouble(cmd.getOptionValue("scorethreshold"));
	    
	    int t1MS = Integer.parseInt(cmd.getOptionValue("t1"));
	    int t2MS = Integer.parseInt(cmd.getOptionValue("t2"));
	    
	    int cpuCores = Integer.parseInt(cmd.getOptionValue("cpu"));
	    int memory = Integer.parseInt(cmd.getOptionValue("memory"));
	    
	    NodeSelection ns = NodeSelection.RANDOM;
	    if (cmd.getOptionValue("selection").equalsIgnoreCase(NodeSelection.MIN.name())) {
	    	ns = NodeSelection.MIN;
	    }
	    
	    System.out.println(MAX_BUDGET_BLOCK + " " + SCORE_THRESHOLD + " " + ns);
	    Config ylocSimConfig = null; 
	    if (cmd.hasOption("config_file_path")) {
	       ylocSimConfig = ConfigFactory.parseFile(new File(cmd.getOptionValue("config_file_path")));
	       inputSLS = ylocSimConfig
					.getConfig(SLSConfiguration.YLOC_SIM_ROOT)
					.getString(SLSConfiguration.NEW_SLS_TRACE_FILE);
	       output =  ylocSimConfig
					.getConfig(SLSConfiguration.YLOC_SIM_ROOT)
					.getConfig(SLSConfiguration.SLS_CONF_ROOT)
					.getString(SLSConfiguration.SLS_OUTPUT_FOLDER);
	       
	    }
	    
	    
	    boolean isDistributeDataEnabled = cmd.getOptionValue("distribute").equalsIgnoreCase("true") ? true : false;
	    
	    if (!isDistributeDataEnabled) {
	    	inputSLS = cmd.getOptionValue("inputsls");
	    }
	    
	    
	    System.out.println("output : " + output);
	    System.out.println("inputsls : " + inputSLS);
	    System.out.println("nodeFile : " + nodeFile);
	    
	    if ((inputRumen == null && inputSLS == null) || output == null) {
	      System.err.println();
	      System.err.println("ERROR: Missing input or output file");
	      System.err.println();
	      System.err.println("Options: -inputrumen|-inputsls FILE,FILE... " +
	              "-output FILE [-nodes FILE] [-trackjobs JobId,JobId...] " +
	              "[-printsimulation]");
	      System.err.println();
	      System.exit(1);
	    }
	    
	    File outputFile = new File(output);
	    if (! outputFile.exists()
	            && ! outputFile.mkdirs()) {
	      System.err.println("ERROR: Cannot create output directory "
	              + outputFile.getAbsolutePath());
	      System.exit(1);
	    }
	    
	    Set<String> trackedJobSet = new HashSet<String>();
	    if (cmd.hasOption("trackjobs")) {
	      String trackjobs = cmd.getOptionValue("trackjobs");
	      String jobIds[] = trackjobs.split(",");
	      trackedJobSet.addAll(Arrays.asList(jobIds));
	    }
	    
	   

	    boolean isSLS = inputSLS != null;
	    String inputFiles[] = isSLS ? inputSLS.split(",") : inputRumen.split(",");
	    SLSRunner sls = new SLSRunner(isSLS, inputFiles, nodeFile, output,
	        trackedJobSet, cmd.hasOption("printsimulation"),MAX_BUDGET_BLOCK,SCORE_THRESHOLD, ns, t1MS , t2MS, isDistributeDataEnabled, cpuCores , memory);
	    /*SLSRunner sls = new SLSRunner(isSLS, inputFiles, nodeFile, output,
		        trackedJobSet, cmd.hasOption("printsimulation"));*/
	    sls.start();
  }
}
