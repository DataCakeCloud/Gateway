/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.spark.connector.hive.write

import java.util.Date

import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataSingleWriter, SingleDirectoryDataWriter, WriteJobDescription}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.sparkHadoopWriterUtils

/**
 * This class is rewritten because of SPARK-42478, which affects Spark 3.3.2
 */
case class FileWriterFactory(
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends DataWriterFactory {

  // SPARK-42478: jobId across tasks should be consistent to meet the contract
  // expected by Hadoop committers, but `JobId` cannot be serialized.
  // thus, persist the serializable jobTrackerID in the class and make jobId a
  // transient lazy val which recreates it each time to ensure jobId is unique.
  private[this] val jobTrackerID = sparkHadoopWriterUtils.createJobTrackerID(new Date)
  @transient private lazy val jobId = createJobID(jobTrackerID, 0)

  override def createWriter(partitionId: Int, realTaskId: Long): DataWriter[InternalRow] = {
    val taskAttemptContext = createTaskAttemptContext(partitionId)
    committer.setupTask(taskAttemptContext)
    if (description.partitionColumns.isEmpty) {
      new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
    } else {
      new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
    }
  }

  private def createTaskAttemptContext(partitionId: Int): TaskAttemptContextImpl = {
    val taskId = new TaskID(jobId, TaskType.MAP, partitionId)
    val taskAttemptId = new TaskAttemptID(taskId, 0)
    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskId.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }

  /**
   * Create a job ID.
   *
   * @param jobTrackerID unique job track id
   * @param id job number
   * @return a job ID
   */
  def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }
}
