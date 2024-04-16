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

package org.apache.kyuubi.session.trino

import java.util.concurrent.TimeUnit._

import io.airlift.units.{DataSize, Duration}
import io.trino.client.StatementClient

import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.trino.KyuubiProgressFormatUtils._

/**
 * Copy from trino-cli
 *   1. use OperationLog instead PrintStream to out status info
 */
object KyuubiTrinoStatusPrinter {

  def printFinalInfo(
      client: StatementClient,
      operationLog: OperationLog,
      debug: Boolean = false): Unit = {
    val out = new KyuubiTrinoConsoleProgressBar(operationLog)
    val results = client.finalStatusInfo()
    val stats = results.getStats

    val wallTime = Duration.succinctDuration(stats.getElapsedTimeMillis(), MILLISECONDS)

    val nodes = stats.getNodes
    if ((nodes == 0) || (stats.getTotalSplits == 0)) {
      return
    }

    // Query 12, FINISHED, 1 node
    val querySummary = s"Query ${results.getId}, ${stats.getState}," +
      s" ${"%,d".format(nodes)} ${pluralize("node", nodes)}"
    out.printLine(querySummary)

    if (debug) {
      out.printLine(results.getInfoUri.toString)
    }

    // Splits: 1000 total, 842 done (84.20%)
    val splitsSummary = s"Splits: ${"%,d".format(stats.getTotalSplits)} total," +
      s" ${"%,d".format(stats.getCompletedSplits)}" +
      s" done (${"%.2f".format(stats.getProgressPercentage.orElse(0.0))}%)"
    out.printLine(splitsSummary)

    if (debug) {
      // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
      val cpuTime = new Duration(stats.getCpuTimeMillis(), MILLISECONDS)
      val cpuTimeSummary = s"CPU Time: ${"%.1f".format(cpuTime.getValue(SECONDS))}s total, " +
        s"${"%5s".format(formatCountRate(
            stats.getProcessedRows(),
            cpuTime,
            false))} rows/s, " +
        s"${"%8s".format(formatDataRate(
            DataSize.ofBytes(stats.getProcessedBytes),
            cpuTime,
            true))}, " +
        s"${"%d".format(percentage(
            stats.getCpuTimeMillis(),
            stats.getWallTimeMillis()))}% active"
      out.printLine(cpuTimeSummary)

      val parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS)

      // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
      val perNodeSummary = s"Per Node: ${"%.1f".format(parallelism / nodes)} parallelism, " +
        s"${"%5s".format(formatCountRate(
            stats.getProcessedRows.toDouble / nodes,
            wallTime,
            false))} rows/s, " +
        s"${"%8s".format(formatDataRate(
            DataSize.ofBytes(stats.getProcessedBytes / nodes),
            wallTime,
            true))}"
      out.reprintLine(perNodeSummary)

      // Parallelism: 5.3
      out.printLine(s"Parallelism: ${"%.1f".format(parallelism)}")

      // Peak Memory: 1.97GB
      out.reprintLine("Peak Memory: " + formatDataSize(
        DataSize.ofBytes(stats.getPeakMemoryBytes),
        true))

      // Spilled Data: 20GB
      if (stats.getSpilledBytes > 0) {
        out.reprintLine("Spilled: " + formatDataSize(
          DataSize.ofBytes(stats.getSpilledBytes),
          true))
      }
    }

    // 0:32 [2.12GB, 15M rows] [67MB/s, 463K rows/s]
    val statsLine = s"${formatFinalTime(wallTime)} " +
      s"[${formatCount(stats.getProcessedRows)} rows, " +
      s"${formatDataSize(DataSize.ofBytes(stats.getProcessedBytes), true)}] " +
      s"[${formatCountRate(stats.getProcessedRows(), wallTime, false)} rows/s, " +
      s"${formatDataRate(DataSize.ofBytes(stats.getProcessedBytes), wallTime, true)}]"
    out.printLine(statsLine)
  }

  private def percentage(count: Double, total: Double): Int = {
    if (total == 0) 0 else Math.min(100, (count * 100.0) / total).toInt
  }

}
