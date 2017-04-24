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

package org.apache.hadoop.yarn.sls.web;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
        .SchedulerEventType;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.Request;

import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.FairSchedulerMetrics;
import org.apache.hadoop.yarn.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import org.mortbay.jetty.handler.ResourceHandler;

public class SLSWebApp extends HttpServlet {
  private static final long serialVersionUID = 1905162041950251407L;
  private transient Server server;
  private transient ResourceSchedulerWrapper wrapper;
  private transient MetricRegistry metrics;
  private transient SchedulerMetrics schedulerMetrics;
  // metrics objects
  private transient Gauge jvmFreeMemoryGauge;
  private transient Gauge jvmMaxMemoryGauge;
  private transient Gauge jvmTotalMemoryGauge;
  private transient Gauge numRunningAppsGauge;
  private transient Gauge numRunningContainersGauge;
  private transient Gauge allocatedMemoryGauge;
  private transient Gauge allocatedVCoresGauge;
  private transient Gauge availableMemoryGauge;
  private transient Gauge availableVCoresGauge;
  private transient Histogram allocateTimecostHistogram;
  private transient Histogram handleTimecostHistogram;
  private Map<SchedulerEventType, Histogram> handleOperTimecostHistogramMap;
  private Map<String, Counter> queueAllocatedMemoryCounterMap;
  private Map<String, Counter> queueAllocatedVCoresCounterMap;
  private int port;
  private int ajaxUpdateTimeMS = 1000;
  // html page templates
  private String simulateInfoTemplate;
  private String simulateTemplate;
  private String trackTemplate;
  private String localityTemplate;

  {
    // load templates
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
     /* simulateInfoTemplate = FileUtils.readFileToString(new File(
              cl.getResource("src/main/resources/simulate.info.html.template").getFile()));
      simulateTemplate = FileUtils.readFileToString(new File(
              cl.getResource("src/main/resources/simulate.html.template").getFile()));
      trackTemplate = FileUtils.readFileToString(new File(
              cl.getResource("src/main/resourcestrack.html.template").getFile()));*/
      
      simulateInfoTemplate = FileUtils.readFileToString(new File("src/main/resources/simulate.info.html.template"));
      simulateTemplate = FileUtils.readFileToString(new File("src/main/resources/simulate.html.template"));
      trackTemplate = FileUtils.readFileToString(new File("src/main/resources/track.html.template"));
      localityTemplate = FileUtils.readFileToString(new File("src/main/resources/locality.html.template"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public SLSWebApp(ResourceSchedulerWrapper wrapper, int metricsAddressPort) {
    this.wrapper = wrapper;
    metrics = wrapper.getMetrics();
    handleOperTimecostHistogramMap =
            new HashMap<SchedulerEventType, Histogram>();
    queueAllocatedMemoryCounterMap = new HashMap<String, Counter>();
    queueAllocatedVCoresCounterMap = new HashMap<String, Counter>();
    schedulerMetrics = wrapper.getSchedulerMetrics();
    port = metricsAddressPort;
  }

  public void start(){
    // static files
    final ResourceHandler staticHandler = new ResourceHandler();
    staticHandler.setResourceBase("src/main/resources/html");

    Handler handler = new AbstractHandler() {
      @Override
      public void handle(String target, HttpServletRequest request,
                         HttpServletResponse response, int dispatch) {
        try{
          // timeunit
          int timeunit = 1000;   // second, divide millionsecond / 1000
          String timeunitLabel = "second";
          if (request.getParameter("u")!= null &&
                  request.getParameter("u").equalsIgnoreCase("m")) {
            timeunit = 1000 * 60;
            timeunitLabel = "minute";
          }
          response.addHeader("Access-Control-Allow-Origin", "*");
          response.addHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
          response.addHeader("Access-Control-Allow-Headers", "x-requested-with, Content-Type, origin, authorization, accept, client-security-token");
          
          // http request
          if (target.equals("/")) {
            printPageIndex(request, response);
          } else if (target.equals("/simulate")) {
            printPageSimulate(request, response, timeunit, timeunitLabel);
          } else if (target.equals("/track")) {
            printPageTrack(request, response, timeunit, timeunitLabel, trackTemplate);
          } else if (target.equals("/trackLocality")) {
        	  printPageTrack(request, response, timeunit, timeunitLabel, localityTemplate);
          } else
            // js/css request
            if (target.startsWith("/js") || target.startsWith("/css")) {
              response.setCharacterEncoding("utf-8");
              staticHandler.handle(target, request, response, dispatch);
            } else
              // json request
              if (target.contains("/simulateMetrics")) {
                printJsonMetrics(request, response);
              } else if (target.equals("/trackMetrics")) {
                printJsonTrack(request, response);
              }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    server = new Server(port);
    
    server.setHandler(handler);

    try {
    if (server.isRunning()) {
    	server.stop();
    	Thread.sleep(3000);
    }
    server.start();
    }
    catch (Exception x ) {
    	System.err.println("Web Application is Already Started !");
    }
  }

  public void stop() throws Exception {
	  
    if (server != null) {
      server.stop();
    }

  }

  /**
   * index html page, show simulation info
   * path ""
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageIndex(HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    String simulateInfo;
    if (SLSRunner.simulateInfoMap.isEmpty()) {
      String empty = "<tr><td colspan='2' align='center'>" +
              "No information available</td></tr>";
      simulateInfo = MessageFormat.format(simulateInfoTemplate, empty);
    } else {
      StringBuilder info = new StringBuilder();
      for (Map.Entry<String, Object> entry :
              SLSRunner.simulateInfoMap.entrySet()) {
        info.append("<tr>");
        info.append("<td class='td1'>").append(entry.getKey()).append("</td>");
        info.append("<td class='td2'>").append(entry.getValue())
                .append("</td>");
        info.append("</tr>");
      }
      simulateInfo =
              MessageFormat.format(simulateInfoTemplate, info.toString());
    }
    response.getWriter().println(simulateInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * simulate html page, show several real-runtime chart
   * path "/simulate"
   * use d3.js
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageSimulate(HttpServletRequest request,
                                 HttpServletResponse response, int timeunit,
                                 String timeunitLabel)
          throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    // queues {0}
    Set<String> queues = wrapper.getQueueSet();
    StringBuilder queueInfo = new StringBuilder();

    int i = 0;
    for (String queue : queues) {
      queueInfo.append("legends[4][").append(i).append("] = 'queue.")
              .append(queue).append(".allocated.memory';");
      queueInfo.append("legends[5][").append(i).append("] = 'queue.")
              .append(queue).append(".allocated.vcores';");
      i ++;
    }

    // time unit label {1}
    // time unit {2}
    // ajax update time interval {3}
    String simulateInfo = MessageFormat.format(simulateTemplate,
            queueInfo.toString(), timeunitLabel, "" + timeunit,
            "" + ajaxUpdateTimeMS);
    response.getWriter().println(simulateInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * html page for tracking one queue or job
   * use d3.js
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageTrack(HttpServletRequest request,
                               HttpServletResponse response, int timeunit,
                               String timeunitLabel, String templateContent)
          throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    // tracked queues {0}
    StringBuilder trackedQueueInfo = new StringBuilder();
    Set<String> trackedQueues = wrapper.getQueueSet();
    for(String queue : trackedQueues) {
      trackedQueueInfo.append("<option value='Queue ").append(queue)
              .append("'>").append(queue).append("</option>");
    }

    // tracked apps {1}
    StringBuilder trackedAppInfo = new StringBuilder();
    Set<String> trackedApps = wrapper.getTrackedAppSet();
    for(String job : trackedApps) {
      trackedAppInfo.append("<option value='Job ").append(job)
              .append("'>").append(job).append("</option>");
    }

    // timeunit label {2}
    // time unit {3}
    // ajax update time {4}
    // final html
    String trackInfo = MessageFormat.format(templateContent,
            trackedQueueInfo.toString(), trackedAppInfo.toString(),
            timeunitLabel, "" + timeunit, "" + ajaxUpdateTimeMS);
    response.getWriter().println(trackInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * package metrics information in a json and return
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printJsonMetrics(HttpServletRequest request,
                                HttpServletResponse response)
          throws IOException {
    response.setContentType("text/json");
    response.setStatus(HttpServletResponse.SC_OK);

    response.getWriter().println(generateRealTimeTrackingMetrics());
    ((Request) request).setHandled(true);
  }

  public String generateRealTimeTrackingMetrics() {
    // JVM
    double jvmFreeMemoryGB, jvmMaxMemoryGB, jvmTotalMemoryGB;
    if (jvmFreeMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.free.memory")) {
      jvmFreeMemoryGauge = metrics.getGauges().get("variable.jvm.free.memory");
    }
    if (jvmMaxMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.max.memory")) {
      jvmMaxMemoryGauge = metrics.getGauges().get("variable.jvm.max.memory");
    }
    if (jvmTotalMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.total.memory")) {
      jvmTotalMemoryGauge = metrics.getGauges()
              .get("variable.jvm.total.memory");
    }
    jvmFreeMemoryGB = jvmFreeMemoryGauge == null ? 0 :
            Double.parseDouble(jvmFreeMemoryGauge.getValue().toString())
                    /1024/1024/1024;
    jvmMaxMemoryGB = jvmMaxMemoryGauge == null ? 0 :
            Double.parseDouble(jvmMaxMemoryGauge.getValue().toString())
                    /1024/1024/1024;
    jvmTotalMemoryGB = jvmTotalMemoryGauge == null ? 0 :
            Double.parseDouble(jvmTotalMemoryGauge.getValue().toString())
                    /1024/1024/1024;

    // number of running applications/containers
    String numRunningApps, numRunningContainers;
    if (numRunningAppsGauge == null &&
            metrics.getGauges().containsKey("variable.running.application")) {
      numRunningAppsGauge =
              metrics.getGauges().get("variable.running.application");
    }
    if (numRunningContainersGauge == null &&
            metrics.getGauges().containsKey("variable.running.container")) {
      numRunningContainersGauge =
              metrics.getGauges().get("variable.running.container");
    }
    numRunningApps = numRunningAppsGauge == null ? "0" :
            numRunningAppsGauge.getValue().toString();
    numRunningContainers = numRunningContainersGauge == null ? "0" :
            numRunningContainersGauge.getValue().toString();

    // cluster available/allocate resource
    double allocatedMemoryGB, allocatedVCoresGB,
            availableMemoryGB, availableVCoresGB;
    if (allocatedMemoryGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.allocated.memory")) {
      allocatedMemoryGauge = metrics.getGauges()
              .get("variable.cluster.allocated.memory");
    }
    if (allocatedVCoresGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.allocated.vcores")) {
      allocatedVCoresGauge = metrics.getGauges()
              .get("variable.cluster.allocated.vcores");
    }
    if (availableMemoryGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.available.memory")) {
      availableMemoryGauge = metrics.getGauges()
              .get("variable.cluster.available.memory");
    }
    if (availableVCoresGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.available.vcores")) {
      availableVCoresGauge = metrics.getGauges()
              .get("variable.cluster.available.vcores");
    }
    allocatedMemoryGB = allocatedMemoryGauge == null ? 0 :
            Double.parseDouble(allocatedMemoryGauge.getValue().toString())/1024;
    allocatedVCoresGB = allocatedVCoresGauge == null ? 0 :
            Double.parseDouble(allocatedVCoresGauge.getValue().toString());
    availableMemoryGB = availableMemoryGauge == null ? 0 :
            Double.parseDouble(availableMemoryGauge.getValue().toString())/1024;
    availableVCoresGB = availableVCoresGauge == null ? 0 :
            Double.parseDouble(availableVCoresGauge.getValue().toString());

    // scheduler operation
    double allocateTimecost, handleTimecost;
    if (allocateTimecostHistogram == null &&
            metrics.getHistograms().containsKey(
                    "sampler.scheduler.operation.allocate.timecost")) {
      allocateTimecostHistogram = metrics.getHistograms()
              .get("sampler.scheduler.operation.allocate.timecost");
    }
    if (handleTimecostHistogram == null &&
            metrics.getHistograms().containsKey(
                    "sampler.scheduler.operation.handle.timecost")) {
      handleTimecostHistogram = metrics.getHistograms()
              .get("sampler.scheduler.operation.handle.timecost");
    }
    allocateTimecost = allocateTimecostHistogram == null ? 0.0 :
            allocateTimecostHistogram.getSnapshot().getMean()/1000000;
    handleTimecost = handleTimecostHistogram == null ? 0.0 :
            handleTimecostHistogram.getSnapshot().getMean()/1000000;
    // various handle operation
    Map<SchedulerEventType, Double> handleOperTimecostMap =
            new HashMap<SchedulerEventType, Double>();
    for (SchedulerEventType e : SchedulerEventType.values()) {
      String key = "sampler.scheduler.operation.handle." + e + ".timecost";
      if (! handleOperTimecostHistogramMap.containsKey(e) &&
              metrics.getHistograms().containsKey(key)) {
        handleOperTimecostHistogramMap.put(e, metrics.getHistograms().get(key));
      }
      double timecost = handleOperTimecostHistogramMap.containsKey(e) ?
          handleOperTimecostHistogramMap.get(e).getSnapshot().getMean()/1000000
              : 0;
      handleOperTimecostMap.put(e, timecost);
    }

    // allocated resource for each queue
    Map<String, Double> queueAllocatedMemoryMap = new HashMap<String, Double>();
    Map<String, Long> queueAllocatedVCoresMap = new HashMap<String, Long>();
    Map<String , Integer> queueNodeLocalityCounts = new HashMap<String, Integer>();
    Map<String , Integer> queueRackLocalityCounts = new HashMap<String, Integer>();
    Map<String , Integer> queueOffLocalityCounts = new HashMap<String, Integer>();
    
    for (String queue : wrapper.getQueueSet()) {
      // memory
      String counterKey = "counter.queue." + queue + ".allocated.memory";
      if (! queueAllocatedMemoryCounterMap.containsKey(queue) &&
              metrics.getCounters().containsKey(counterKey)) {
        queueAllocatedMemoryCounterMap.put(queue,
                metrics.getCounters().get(counterKey));
      }
      double queueAllocatedMemoryGB =
              queueAllocatedMemoryCounterMap.containsKey(queue) ?
                  queueAllocatedMemoryCounterMap.get(queue).getCount()/1024.0
                      : 0;
      queueAllocatedMemoryMap.put(queue, queueAllocatedMemoryGB);
      // vCores
      counterKey = "counter.queue." + queue + ".allocated.cores";
      if (! queueAllocatedVCoresCounterMap.containsKey(queue) &&
              metrics.getCounters().containsKey(counterKey)) {
        queueAllocatedVCoresCounterMap.put(
                queue, metrics.getCounters().get(counterKey));
      }
      long queueAllocatedVCores =
              queueAllocatedVCoresCounterMap.containsKey(queue) ?
                      queueAllocatedVCoresCounterMap.get(queue).getCount(): 0;
      queueAllocatedVCoresMap.put(queue, queueAllocatedVCores);
      
      counterKey = "variable.queue." + queue + ".locality.node";
      Integer queueNodeLocalityCount = metrics.getGauges().containsKey(counterKey) ?  (Integer)metrics.getGauges().get(counterKey).getValue() : 0;
      queueNodeLocalityCounts.put(queue, queueNodeLocalityCount.intValue());
      counterKey = "variable.queue." + queue + ".locality.rack";
      int queueRackLocalityCount = metrics.getGauges().containsKey(counterKey) ?  (int)metrics.getGauges().get(counterKey).getValue() : 0;
      queueRackLocalityCounts.put(queue, queueRackLocalityCount);
      counterKey = "variable.queue." + queue + ".locality.off";
      int queueOffLocalityCount = metrics.getGauges().containsKey(counterKey)?  (int)metrics.getGauges().get(counterKey).getValue() : 0;
      queueOffLocalityCounts.put(queue,queueOffLocalityCount);
    }

    // package results
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"time\":" ).append(System.currentTimeMillis())
            .append(",\"jvm.free.memory\":").append(jvmFreeMemoryGB)
            .append(",\"jvm.max.memory\":").append(jvmMaxMemoryGB)
            .append(",\"jvm.total.memory\":").append(jvmTotalMemoryGB)
            .append(",\"running.applications\":").append(numRunningApps)
            .append(",\"running.containers\":").append(numRunningContainers)
            .append(",\"cluster.allocated.memory\":").append(allocatedMemoryGB)
            .append(",\"cluster.allocated.vcores\":").append(allocatedVCoresGB)
            .append(",\"cluster.available.memory\":").append(availableMemoryGB)
            .append(",\"cluster.available.vcores\":").append(availableVCoresGB);

    for (String queue : wrapper.getQueueSet()) {
      sb.append(",\"queue.").append(queue).append(".allocated.memory\":")
              .append(queueAllocatedMemoryMap.get(queue));
      sb.append(",\"queue.").append(queue).append(".allocated.vcores\":")
              .append(queueAllocatedVCoresMap.get(queue));
      sb.append(",\"queue.").append(queue).append(".locality.node\":")
      .append(queueNodeLocalityCounts.get(queue));
      sb.append(",\"queue.").append(queue).append(".locality.rack\":")
      .append(queueRackLocalityCounts.get(queue));
      sb.append(",\"queue.").append(queue).append(".locality.off\":")
      .append(queueOffLocalityCounts.get(queue));
      
    }
    
    //Nodes containers metrics
    for (String gaugeKey : metrics.getGauges().keySet()) {
    	if (gaugeKey.contains("maps")) {
    		sb.append(",\"" + gaugeKey  + "\"" + ":").append((int)metrics.getGauges().get(gaugeKey).getValue());
    	}
    }
    for (String gaugeKey : metrics.getGauges().keySet()) {
    	if (gaugeKey.contains("reducers")) {
    		sb.append(",\"" + gaugeKey  + "\"" + ":").append((int)metrics.getGauges().get(gaugeKey).getValue());
    	}
    }
    
    
    // scheduler allocate & handle
    sb.append(",\"scheduler.allocate.timecost\":").append(allocateTimecost);
    sb.append(",\"scheduler.handle.timecost\":").append(handleTimecost);
    for (SchedulerEventType e : SchedulerEventType.values()) {
      sb.append(",\"scheduler.handle-").append(e).append(".timecost\":")
              .append(handleOperTimecostMap.get(e));
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * package metrics information for one tracked queue/app
   * only support FairScheduler currently
   * @throws java.io.IOException
   */
  private void printJsonTrack(HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
    response.setContentType("text/json");
    response.setStatus(HttpServletResponse.SC_OK);

    StringBuilder sb = new StringBuilder();
    if(schedulerMetrics instanceof FairSchedulerMetrics) {
      String para = request.getParameter("t");
      if (para.startsWith("Job ")) {
        String appId = para.substring("Job ".length());

        sb.append("{");
        sb.append("\"time\": ").append(System.currentTimeMillis()).append(",");
        sb.append("\"appId\": \"").append(appId).append("\"");
        for(String metric : this.schedulerMetrics.getAppTrackedMetrics()) {
          String key = "variable.app." + appId + "." + metric;
          sb.append(",\"").append(metric).append("\": ");
          if (metrics.getGauges().containsKey(key)) {
            double memoryGB =
                    Double.parseDouble(
                            metrics.getGauges().get(key).getValue().toString())
                            / 1024;
            sb.append(memoryGB);
          } else {
            sb.append(-1);
          }
        }
        sb.append("}");

      } else if(para.startsWith("Queue ")) {
        String queueName = para.substring("Queue ".length());
        sb.append("{");
        sb.append("\"time\": ").append(System.currentTimeMillis()).append(",");
        sb.append("\"queueName\": \"").append(queueName).append("\"");
        for(String metric : this.schedulerMetrics.getQueueTrackedMetrics()) {
          String key = "variable.queue." + queueName + "." + metric;
          sb.append(",\"").append(metric).append("\": ");
          if (metrics.getGauges().containsKey(key)) {
            double memoryGB =
                    Double.parseDouble(
                            metrics.getGauges().get(key).getValue().toString())
                            / 1024;
            sb.append(memoryGB);
          } else {
            sb.append(-1);
          }
        }
        sb.append("}");
      }
    }
    String output = sb.toString();
    if (output.isEmpty()) {
      output = "[]";
    }
    response.getWriter().println(output);
    // package result
    ((Request) request).setHandled(true);
  }
}
