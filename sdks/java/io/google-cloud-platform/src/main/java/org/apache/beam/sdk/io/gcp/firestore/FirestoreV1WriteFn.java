/*
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
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;

import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.common.collect.ImmutableList;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.WindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.FailedWritesException;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1Fn.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of {@link org.apache.beam.sdk.transforms.DoFn DoFn}s for each of the supported write
 * RPC methods from the Cloud Firestore V1 API.
 */
final class FirestoreV1WriteFn {

  static final class DefaultBatchWriteFn extends BaseBatchWriteFn<Void> {
    DefaultBatchWriteFn(JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    protected void handleWriteFailures(ContextAdapter<Void> context, Instant timestamp,
        List<WriteFailureDetails> writeFailures, Runnable logMessage) {
      throw new FailedWritesException(writeFailures.stream().map(w -> w.failure).collect(Collectors.toList()));
    }
  }

  static final class BatchWriteFnWithDeadLetterQueue extends BaseBatchWriteFn<WriteFailure> {
    BatchWriteFnWithDeadLetterQueue(JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    protected void handleWriteFailures(ContextAdapter<WriteFailure> context, Instant timestamp,
        List<WriteFailureDetails> writeFailures, Runnable logMessage) {
      logMessage.run();
      for (WriteFailureDetails details : writeFailures) {
        context.output(details.failure, timestamp, details.window);
      }
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link BatchWriteRequest}s.
   * <p/>
   * Writes will be enqueued to be sent at a potentially
   * later time when more writes are available. This Fn attempts to maximize throughput while
   * maintaining a high request success rate.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static abstract class BaseBatchWriteFn<Out> extends WindowAwareDoFn<Write, Out> implements
      HasRpcAttemptContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirestoreV1Fn.V1FnRpcAttemptContext.BatchWrite.getNamespace());
    private final JodaClock clock;
    private final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    private final RpcQosOptions rpcQosOptions;

    // transient running state information, not important to any possible checkpointing
    private transient FirestoreRpc firestoreRpc;
    private transient RpcQos rpcQos;
    private transient String projectId;
    @VisibleForTesting
    transient Queue<@NonNull WriteElement> writes = new PriorityQueue<>(WriteElement.COMPARATOR);
    @VisibleForTesting
    transient int queueNextEntryPriority = 0;

    @SuppressWarnings("initialization.fields.uninitialized") // allow transient fields to be managed by component lifecycle
    protected BaseBatchWriteFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      this.clock = clock;
      this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      this.rpcQosOptions = rpcQosOptions;
    }

    protected Logger getLogger() {
      return LOGGER;
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.BatchWrite;
    }

    @Override
    public final void populateDisplayData(@edu.umd.cs.findbugs.annotations.NonNull DisplayData.Builder builder) {
      builder
          .include("rpcQosOptions", rpcQosOptions);
    }

    @Override
    public void setup() {
      rpcQos = firestoreStatefulComponentFactory.getRpcQos(rpcQosOptions);
      writes = new PriorityQueue<>(WriteElement.COMPARATOR);
    }

    @Override
    public final void startBundle(StartBundleContext c) {
      String project = c.getPipelineOptions().as(GcpOptions.class).getProject();
      projectId = requireNonNull(project, "project must be defined on GcpOptions of PipelineOptions");
      firestoreRpc = firestoreStatefulComponentFactory.getFirestoreRpc(c.getPipelineOptions());
    }

    /**
     * For each element extract and enqueue all writes from the commit. Then potentially flush any
     * previously and currently enqueued writes.
     * <p/>
     * In order for writes to be enqueued the value of {@link BatchWriteRequest#getDatabase()} must
     * match exactly with the database name this instance is configured for via the provided {@link
     * org.apache.beam.sdk.options.PipelineOptions PipelineOptions}
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public void processElement(ProcessContext context, BoundedWindow window) throws Exception {
      @SuppressWarnings("nullness") // for some reason requireNonNull thinks its parameter must be non-null...
      Write write = requireNonNull(context.element(), "context.element() must be non null");
      ProcessContextAdapter<Out> contextAdapter = new ProcessContextAdapter<>(context);
      int serializedSize = write.getSerializedSize();
      boolean tooLarge = rpcQos.bytesOverLimit(serializedSize);
      if (tooLarge) {
        String message = String.format(
            "%s for document '%s' larger than configured max allowed bytes per batch",
            getWriteType(write),
            getName(write)
        );
        handleWriteFailures(contextAdapter, clock.instant(),
            ImmutableList.of(new WriteFailureDetails(
                new WriteFailure(
                    write,
                    WriteResult.newBuilder().build(),
                    Status.newBuilder()
                        .setCode(Code.INVALID_ARGUMENT.getNumber())
                        .setMessage(message)
                        .build()
                ),
                window
            )),
            () -> LOGGER.info(message)
        );
      } else {
        writes.offer(new WriteElement(queueNextEntryPriority++, write, window));
        flushBatch(/* finalFlush */ false, contextAdapter);
      }
    }

    /**
     * Attempt to flush any outstanding enqueued writes before cleaning up any bundle related state.
     * {@inheritDoc}
     */
    @SuppressWarnings("nullness") // allow clearing transient fields
    @Override
    public void finishBundle(FinishBundleContext context) throws Exception {
      try {
        flushBatch(/* finalFlush */ true, new FinishBundleContextAdapter<>(context));
      } finally {
        projectId = null;
        firestoreRpc.close();
      }
    }

    /**
     * Possibly flush enqueued writes to Firestore.
     * <p/>
     * This flush attempts to maximize throughput and success rate of RPCs. When a flush should
     * happen and how many writes are included is determined and managed by the {@link RpcQos}
     * instance of this class.
     *
     * @param finalFlush A boolean specifying if this call is from {@link #finishBundle(DoFn.FinishBundleContext)}. If
     * {@code true}, this method will not return until a terminal state (success, attempts
     * exhausted) for all enqueued writes is reached.
     * @throws InterruptedException If the current thread is interrupted at anytime, such as while
     * waiting for the next attempt
     * @see RpcQos
     * @see RpcQos.RpcWriteAttempt
     * @see BackOffUtils#next(org.apache.beam.sdk.util.Sleeper, org.apache.beam.sdk.util.BackOff)
     */
    private void flushBatch(boolean finalFlush, ContextAdapter<Out> context) throws InterruptedException {
      while (!writes.isEmpty()) {
        RpcWriteAttempt attempt = rpcQos.newWriteAttempt(getRpcAttemptContext());
        Instant begin = clock.instant();
        if (!attempt.awaitSafeToProceed(begin)) {
          continue;
        }

        FlushBuffer<WriteElement> flushBuffer = getFlushBuffer(attempt, begin);
        if (flushBuffer.isFull() || finalFlush) {
          doFlush(attempt, flushBuffer, context);
        } else {
          // since we're not going to perform a flush, we need to return the writes that were
          // preemptively removed
          flushBuffer.forEach(writes::offer);
          // we're not on the final flush, so yield until more elements are delivered or
          // finalFlush is true
          return;
        }
      }
      // now that the queue has been emptied reset our priority back to 0 to try and ensure
      // we won't run into overflow issues if a worker runs for a long time and processes
      // many writes.
      queueNextEntryPriority = 0;
    }

    private FlushBuffer<WriteElement> getFlushBuffer(RpcWriteAttempt attempt, Instant start) {
      FlushBuffer<WriteElement> buffer = attempt.newFlushBuffer(start);

      WriteElement peek;
      while ((peek = writes.peek()) != null) {
        if (buffer.offer(peek)) {
          writes.poll();
        } else {
          break;
        }
      }
      return buffer;
    }

    private BatchWriteRequest getBatchWriteRequest(FlushBuffer<WriteElement> flushBuffer) {
      BatchWriteRequest.Builder commitBuilder = BatchWriteRequest.newBuilder()
          .setDatabase(getDatabaseName());
      for (WriteElement element : flushBuffer) {
        commitBuilder.addWrites(element.getValue());
      }
      return commitBuilder.build();
    }

    private void doFlush(
        RpcWriteAttempt attempt,
        FlushBuffer<WriteElement> flushBuffer,
        ContextAdapter<Out> context
    ) throws InterruptedException {
      int writesCount = flushBuffer.getBufferedElementsCount();
      long bytes = flushBuffer.getBufferedElementsBytes();
      BatchWriteRequest request = getBatchWriteRequest(flushBuffer);
      while (true) {
        LOGGER.debug(
            "Sending BatchWrite request with {} writes totalling {} bytes",
            writesCount, bytes
        );
        Instant start = clock.instant();
        Instant end;
        BatchWriteResponse response;
        try {
          attempt.recordStartRequest(start);
          response = firestoreRpc.batchWriteCallable().call(request);
          end = clock.instant();
        } catch (RuntimeException exception) {
          end = clock.instant();
          String exceptionMessage = exception.getMessage();
          LOGGER.warn(
              "Sending BatchWrite request with {} writes totalling {} bytes failed due to error: {}",
              writesCount,
              bytes,
              exceptionMessage != null ? exceptionMessage : exception.getClass().getName()
          );
          attempt.recordFailedRequest(end, writesCount);
          flushBuffer.forEach(writes::offer);
          attempt.checkCanRetry(exception);
          continue;
        }

        long elapsedMillis = end.minus(start.getMillis()).getMillis();

        int okCount = 0;
        List<WriteFailureDetails> nonRetryableWrites = new ArrayList<>();

        List<WriteResult> writeResultList = response.getWriteResultsList();
        List<Status> statusList = response.getStatusList();

        Iterator<WriteElement> iterator = flushBuffer.iterator();
        for (int i = 0; iterator.hasNext() && i < statusList.size(); i++) {
          WriteElement writeElement = iterator.next();
          Status writeStatus = statusList.get(i);
          Code code = Code.forNumber(writeStatus.getCode());

          if (code == Code.OK) {
            okCount++;
          } else {
            if (attempt.isCodeRetryable(code)) {
              writes.offer(writeElement);
            } else {
              nonRetryableWrites.add(
                  new WriteFailureDetails(
                      new WriteFailure(writeElement.getValue(), writeResultList.get(i), writeStatus),
                      writeElement.window
                  )
              );
            }
          }
        }

        int nonRetryableCount = nonRetryableWrites.size();
        int retryableCount = writesCount - okCount - nonRetryableCount;

        if (okCount == writesCount) {
          LOGGER.debug(
              "Sending BatchWrite request with {} writes totalling {} bytes was completely applied in {}ms",
              writesCount, bytes, elapsedMillis
          );
          attempt.recordSuccessfulRequest(end, okCount);
          attempt.completeSuccess();
        } else {
          if (okCount > 0) {
            attempt.recordSuccessfulRequest(end, okCount);
          }
          if (nonRetryableCount > 0) {
            attempt.recordFailedRequest(end, nonRetryableCount + retryableCount);
            int finalOkCount = okCount;
            handleWriteFailures(context, end, ImmutableList.copyOf(nonRetryableWrites), () ->
                LOGGER.warn(
                    "Sending BatchWrite request with {} writes totalling {} bytes was partially applied in {}ms ({} ok, {} retryable, {} non-retryable)",
                    writesCount, bytes, elapsedMillis, finalOkCount, retryableCount, nonRetryableCount
                )
            );
          } else if (retryableCount > 0) {
            attempt.recordFailedRequest(end, retryableCount);
            LOGGER.debug(
                "Sending BatchWrite request with {} writes totalling {} bytes was partially applied in {}ms ({} ok, {} retryable)",
                writesCount, bytes, elapsedMillis, okCount, retryableCount
            );
          }
        }
        // attempt complete, break the retry loop
        break;
      }
    }

    protected abstract void handleWriteFailures(ContextAdapter<Out> context, Instant timestamp, List<WriteFailureDetails> writeFailures, Runnable logMessage);

    private String getDatabaseName() {
      return String.format("projects/%s/databases/(default)", projectId);
    }

    private static String getWriteType(Write w) {
      if (w.hasUpdate()) {
        return "UPDATE";
      } else if (w.hasTransform()) {
        return "TRANSFORM";
      } else {
        return "DELETE";
      }
    }

    private static String getName(Write w) {
      if (w.hasUpdate()) {
        return w.getUpdate().getName();
      } else if (w.hasTransform()) {
        return w.getTransform().getDocument();
      } else {
        return w.getDelete();
      }
    }

    /**
     * Adapter interface which provides a common parent for {@link ProcessContext}
     * and {@link FinishBundleContext} so that we are able to
     * use a single common invocation to output from.
     */
    protected interface ContextAdapter<T> {
      void output(T t, Instant timestamp, BoundedWindow window);
    }

    private static final class ProcessContextAdapter<T> implements ContextAdapter<T> {
      private final DoFn<Write, T>.ProcessContext context;

      private ProcessContextAdapter(DoFn<Write, T>.ProcessContext context) {
        this.context = context;
      }

      @Override
      public void output(T t, Instant timestamp, BoundedWindow window) {
        context.outputWithTimestamp(t, timestamp);
      }
    }

    private static final class FinishBundleContextAdapter<T> implements ContextAdapter<T> {
      private final DoFn<Write, T>.FinishBundleContext context;

      private FinishBundleContextAdapter(DoFn<Write, T>.FinishBundleContext context) {
        this.context = context;
      }

      @Override
      public void output(T t, Instant timestamp, BoundedWindow window) {
        context.output(t, timestamp, window);
      }
    }
  }

  static final class WriteElement implements Element<Write> {

    private static final Comparator<WriteElement> COMPARATOR = Comparator
        .comparing(WriteElement::getQueuePosition);
    private final int queuePosition;
    private final Write value;

    private final BoundedWindow window;

    @VisibleForTesting
    boolean serializedSizeComputed;
    @VisibleForTesting
    long serializedSize;

    WriteElement(int queuePosition, Write value, BoundedWindow window) {
      this.value = value;
      this.queuePosition = queuePosition;
      this.window = window;
    }

    public int getQueuePosition() {
      return queuePosition;
    }

    @Override
    public Write getValue() {
      return value;
    }

    @Override
    public long getSerializedSize() {
      if (!serializedSizeComputed) {
        serializedSize = value.toByteArray().length;
        serializedSizeComputed = true;
      }
      return serializedSize;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WriteElement)) {
        return false;
      }
      WriteElement that = (WriteElement) o;
      return queuePosition == that.queuePosition &&
          value.equals(that.value) &&
          window.equals(that.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queuePosition, value, window);
    }

    @Override
    public String toString() {
      return "WriteElement{" +
          "queuePosition=" + queuePosition +
          ", value=" + value +
          ", window=" + window +
          '}';
    }
  }

  /**
   * Tuple class which contains all the necessary context to report on a write failure, either
   * as part of an exception or a message output to the next stage of the pipeline.
   */
  private static final class WriteFailureDetails {
    private final WriteFailure failure;
    private final BoundedWindow window;

    public WriteFailureDetails(WriteFailure failure, BoundedWindow window) {
      this.failure = failure;
      this.window = window;
    }
  }
}
