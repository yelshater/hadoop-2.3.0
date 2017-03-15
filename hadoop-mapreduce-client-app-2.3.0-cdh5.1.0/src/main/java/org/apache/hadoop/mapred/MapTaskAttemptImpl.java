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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.file.LogMessageFileWriter;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;


@SuppressWarnings("rawtypes")
public class MapTaskAttemptImpl extends TaskAttemptImpl {

  private TaskSplitMetaInfo splitInfo = null;

  public MapTaskAttemptImpl(TaskId taskId, int attempt, 
      EventHandler eventHandler, Path jobFile, 
      int partition, TaskSplitMetaInfo splitInfo, JobConf conf,
      TaskAttemptListener taskAttemptListener, 
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      AppContext appContext) {
    super(taskId, attempt, eventHandler, 
        taskAttemptListener, jobFile, partition, conf, splitInfo.getLocations(),
        jobToken, credentials, clock, appContext);

	  org.apache.hadoop.mapreduce.InputSplit input = null;
	  try {
	  if (splitInfo!=null && splitInfo.getSplitIndex()!=null) {
	  	  input =   getSplitDetails(new Path(splitInfo.getSplitIndex().getSplitLocation()),
		  splitInfo.getSplitIndex().getStartOffset());
	  	  if (input !=null) {
		  	  String startEndBytes = input.toString().substring(input.toString().lastIndexOf(':') + 1, input.toString().length());
		  	  TaskSplitIndex splitIndex = new TaskSplitIndex(input.toString(), Long.parseLong(startEndBytes.split("\\+")[0]) ); //note that input.toString() returns a string including the URL of the file, start and end indicies (e.g. hdfs://.../filename.txt:0+1024
		  	  TaskSplitMetaInfo metaInfo = new TaskSplitMetaInfo(splitIndex, splitInfo.getLocations(), splitInfo.getInputDataLength());
		  	  setTaskSplitMetaInfo(metaInfo);
	  	  }
	  }
	  }
	  catch (Exception x ) {
		  x.printStackTrace();
	  }
	  this.splitInfo = splitInfo;
  }

  @Override
  public Task createRemoteTask() {
    //job file name is set in TaskAttempt, setting it null here
    MapTask mapTask =
      new MapTask("", TypeConverter.fromYarn(getID()), partition,
          splitInfo.getSplitIndex(), 1); // YARN doesn't have the concept of slots per task, set it as 1.
    mapTask.setUser(conf.get(MRJobConfig.USER_NAME));
    mapTask.setConf(conf);
    return mapTask;
  }
  
  
  /***
   * Added to serialize job.split file
   * @author Yehia Elshater
   * @param file
   * @param offset
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private <T> T getSplitDetails(Path file, long offset) {
		try {
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream inFile = fs.open(file);
			inFile.seek(offset);
			String className = StringInterner.weakIntern(Text
					.readString(inFile));
			Class<T> cls;
			try {
				cls = (Class<T>) conf.getClassByName(className);
			} catch (ClassNotFoundException ce) {
				IOException wrap = new IOException("Split class " + className
						+ " not found");
				wrap.initCause(ce);
				throw wrap;
			}
			SerializationFactory factory = new SerializationFactory(conf);
			Deserializer<T> deserializer = (Deserializer<T>) factory
					.getDeserializer(cls);
			deserializer.open(inFile);
			T split = deserializer.deserialize(null);
			long pos = inFile.getPos();
			getCounters().findCounter(TaskCounter.SPLIT_RAW_BYTES).increment(
					pos - offset);
			inFile.close();
			return split;
		} catch (IOException io) {
			io.printStackTrace();
		}
		return null;
  }

}
