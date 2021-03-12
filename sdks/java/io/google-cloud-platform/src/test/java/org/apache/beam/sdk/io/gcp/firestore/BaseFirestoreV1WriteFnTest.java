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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Write;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BaseBatchWriteFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1Fn.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.WriteElement;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.FlushBufferImpl;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
abstract class BaseFirestoreV1WriteFnTest<Out, Fn extends BaseBatchWriteFn<Out> & HasRpcAttemptContext> extends BaseFirestoreV1FnTest<Write, Out, Fn> {

  protected static final Status STATUS_OK = Status.newBuilder().setCode(Code.OK.getNumber()).build();
  @Mock(lenient = true)
  protected BoundedWindow window;
  @Mock
  protected DoFn<Write, Out>.FinishBundleContext finishBundleContext;
  @Mock
  protected UnaryCallable<BatchWriteRequest, BatchWriteResponse> callable;
  @Mock
  protected RpcQos.RpcWriteAttempt attempt;
  @Mock
  protected RpcQos.RpcWriteAttempt attempt2;

  @Before
  public final void setUp() {
    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt, attempt2);

    when(ff.getRpcQos(any()))
        .thenReturn(rpcQos);
    when(ff.getFirestoreRpc(pipelineOptions))
        .thenReturn(rpc);
    when(rpc.batchWriteCallable())
        .thenReturn(callable);
  }

  @After
  public abstract void tearDown();

  @Override
  @Test
  public final void attemptsExhaustedForRetryableError() throws Exception {
    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpc1Sta = Instant.ofEpochMilli(1);
    Instant rpc1End = Instant.ofEpochMilli(2);
    Instant rpc2Sta = Instant.ofEpochMilli(3);
    Instant rpc2End = Instant.ofEpochMilli(4);
    Instant rpc3Sta = Instant.ofEpochMilli(5);
    Instant rpc3End = Instant.ofEpochMilli(6);
    Write write = FirestoreProtoHelpers.newWrite();
    Element<Write> element1 = new WriteElement(0, write, window);

    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newWriteAttempt(FirestoreV1Fn.V1FnRpcAttemptContext.BatchWrite)).thenReturn(attempt);
    when(rpc.batchWriteCallable()).thenReturn(callable);

    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(RPC_QOS_OPTIONS));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    when(flushBuffer.offer(element1)).thenReturn(true);
    when(flushBuffer.iterator()).thenReturn(newArrayList(element1).iterator());
    when(flushBuffer.getBufferedElementsCount()).thenReturn(1);
    when(flushBuffer.isFull()).thenReturn(true);

    when(callable.call(any())).thenThrow(RETRYABLE_ERROR, RETRYABLE_ERROR, RETRYABLE_ERROR);
    doNothing().when(attempt).recordFailedRequest(any(), anyInt());
    doNothing().doNothing().doThrow(RETRYABLE_ERROR).when(attempt).checkCanRetry(RETRYABLE_ERROR);

    when(processContext.element()).thenReturn(write);

    try {
      runFunction(getFn(clock, ff, RPC_QOS_OPTIONS));
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (ApiException e) {
      assertSame(RETRYABLE_ERROR, e);
    }

    verify(attempt, times(1)).awaitSafeToProceed(attemptStart);
    verify(attempt, times(1)).recordStartRequest(rpc1Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc1End, 1);
    verify(attempt, times(1)).recordStartRequest(rpc2Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc2End, 1);
    verify(attempt, times(1)).recordStartRequest(rpc3Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc3End, 1);
    verify(attempt, times(0)).recordSuccessfulRequest(any(), anyInt());
    verify(attempt, never()).completeSuccess();
  }

  @Override
  @Test
  public final void noRequestIsSentIfNotSafeToProceed() throws Exception {
    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newWriteAttempt(FirestoreV1Fn.V1FnRpcAttemptContext.BatchWrite)).thenReturn(attempt);

    InterruptedException interruptedException = new InterruptedException();
    when(attempt.awaitSafeToProceed(any())).thenReturn(false).thenThrow(interruptedException);

    when(processContext.element()).thenReturn(FirestoreProtoHelpers.newWrite());

    try {
      runFunction(getFn(clock, ff, RPC_QOS_OPTIONS));
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (InterruptedException e) {
      assertSame(interruptedException, e);
    }

    verify(rpc, times(1)).close();
    verifyNoMoreInteractions(rpc);
    verifyNoMoreInteractions(callable);
    verify(attempt, times(0)).recordFailedRequest(any(), anyInt());
    verify(attempt, times(0)).recordSuccessfulRequest(any(), anyInt());
  }

  @Test
  public abstract void enqueueingWritesValidateBytesSize() throws Exception;

  @Test
  public final void endToEnd_success() throws Exception {

    Write write = FirestoreProtoHelpers.newWrite();
    BatchWriteRequest expectedRequest = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)")
        .addWrites(write)
        .build();

    BatchWriteResponse response = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .build();

    Element<Write> element1 = new WriteElement(0, write, window);

    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpcSta = Instant.ofEpochMilli(1);
    Instant rpcEnd = Instant.ofEpochMilli(2);

    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withBatchMaxCount(1)
        .build();
    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(options));
    when(processContext.element()).thenReturn(write);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    ArgumentCaptor<BatchWriteRequest> requestCaptor = ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor.capture())).thenReturn(response);

    runFunction(getFn(clock, ff, options));

    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(flushBuffer, times(1)).offer(element1);
    verify(flushBuffer, times(1)).isFull();
    verify(attempt, times(1)).recordStartRequest(rpcSta);
    verify(attempt, times(1)).recordSuccessfulRequest(rpcEnd, 1);
    verify(attempt, never()).recordFailedRequest(any(), anyInt());
    verify(attempt, never()).checkCanRetry(any());
  }

  @Test
  public final void endToEnd_exhaustingAttemptsResultsInException() throws Exception {
    ApiException err1 = ApiExceptionFactory.createException(new IOException("err1"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED),false);
    ApiException err2 = ApiExceptionFactory.createException(new IOException("err2"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED),false);
    ApiException err3 = ApiExceptionFactory.createException(new IOException("err3"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED),false);

    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpc1Sta = Instant.ofEpochMilli(1);
    Instant rpc1End = Instant.ofEpochMilli(2);
    Instant rpc2Sta = Instant.ofEpochMilli(3);
    Instant rpc2End = Instant.ofEpochMilli(4);
    Instant rpc3Sta = Instant.ofEpochMilli(5);
    Instant rpc3End = Instant.ofEpochMilli(6);

    Write write = FirestoreProtoHelpers.newWrite();

    Element<Write> element1 = new WriteElement(0, write, window);

    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(RPC_QOS_OPTIONS));
    when(processContext.element()).thenReturn(write);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    when(flushBuffer.isFull()).thenReturn(true);
    when(flushBuffer.offer(element1)).thenReturn(true);
    when(flushBuffer.iterator()).thenReturn(newArrayList(element1).iterator());
    when(flushBuffer.getBufferedElementsCount()).thenReturn(1);
    when(callable.call(any()))
        .thenThrow(err1, err2, err3);
    doNothing().when(attempt).checkCanRetry(err1);
    doNothing().when(attempt).checkCanRetry(err2);
    doThrow(err3).when(attempt).checkCanRetry(err3);

    try {
      Fn fn = getFn(clock, ff, RPC_QOS_OPTIONS);
      runFunction(fn);
      fail("Expected exception");
    } catch (ApiException e) {
      assertNotNull(e.getMessage());
      assertTrue(e.getMessage().contains("err3"));
    }

    verify(flushBuffer, times(1)).offer(element1);
    verify(flushBuffer, times(1)).isFull();
    verify(attempt, times(1)).recordStartRequest(rpc1Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc1End, 1);
    verify(attempt, times(1)).recordStartRequest(rpc2Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc2End, 1);
    verify(attempt, times(1)).recordStartRequest(rpc3Sta);
    verify(attempt, times(1)).recordFailedRequest(rpc3End, 1);
    verify(attempt, never()).recordSuccessfulRequest(any(), anyInt());
    verify(attempt, never()).completeSuccess();
  }

  @Test
  public final void endToEnd_awaitSafeToProceed_falseIsTerminalForAttempt() throws Exception {
    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withBatchMaxCount(2)
        .build();

    Instant rpc1Sta = Instant.ofEpochMilli(3);
    Instant rpc1End = Instant.ofEpochMilli(4);
    ArgumentCaptor<BatchWriteRequest> requestCaptor = ArgumentCaptor.forClass(BatchWriteRequest.class);

    Write write = FirestoreProtoHelpers.newWrite();
    BatchWriteRequest expectedRequest = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)")
        .addWrites(write)
        .build();
    BatchWriteResponse response = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .build();

    when(processContext.element()).thenReturn(write);
    when(attempt.awaitSafeToProceed(any()))
        .thenReturn(false)
        .thenThrow(new IllegalStateException("too many attempt1#awaitSafeToProceed"));
    when(attempt2.awaitSafeToProceed(any()))
        .thenReturn(true, true)
        .thenThrow(new IllegalStateException("too many attempt2#awaitSafeToProceed"));
    when(attempt2.<Write, Element<Write>>newFlushBuffer(any())).thenAnswer(invocation -> newFlushBuffer(options));
    when(callable.call(requestCaptor.capture())).thenReturn(response);

    Fn fn = getFn(clock, ff, options);
    runFunction(fn);

    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(attempt, times(1)).awaitSafeToProceed(any());
    verifyNoMoreInteractions(attempt);
    verify(attempt2, times(1)).recordStartRequest(rpc1Sta);
    verify(attempt2, times(1)).recordSuccessfulRequest(rpc1End, 1);
    verify(attempt2, times(1)).completeSuccess();
    verify(attempt2, never()).checkCanRetry(any());
  }

  @Test
  public final void endToEnd_maxBatchSizeRespected() throws Exception {

    Instant enqueue0 = Instant.ofEpochMilli(0);
    Instant enqueue1 = Instant.ofEpochMilli(1);
    Instant enqueue2 = Instant.ofEpochMilli(2);
    Instant enqueue3 = Instant.ofEpochMilli(3);
    Instant enqueue4 = Instant.ofEpochMilli(4);
    Instant group1Rpc1Sta = Instant.ofEpochMilli(5);
    Instant group1Rpc1End = Instant.ofEpochMilli(6);

    Instant enqueue5 = Instant.ofEpochMilli(7);
    Instant finalFlush = Instant.ofEpochMilli(8);
    Instant group2Rpc1Sta = Instant.ofEpochMilli(9);
    Instant group2Rpc1End = Instant.ofEpochMilli(10);

    Write write0 = FirestoreProtoHelpers.newWrite(0);
    Write write1 = FirestoreProtoHelpers.newWrite(1);
    Write write2 = FirestoreProtoHelpers.newWrite(2);
    Write write3 = FirestoreProtoHelpers.newWrite(3);
    Write write4 = FirestoreProtoHelpers.newWrite(4);
    Write write5 = FirestoreProtoHelpers.newWrite(5);
    int maxValuesPerGroup = 5;

    BatchWriteRequest.Builder builder = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)");

    BatchWriteRequest expectedGroup1Request = builder.build().toBuilder()
        .addWrites(write0)
        .addWrites(write1)
        .addWrites(write2)
        .addWrites(write3)
        .addWrites(write4)
        .build();

    BatchWriteRequest expectedGroup2Request = builder.build().toBuilder()
        .addWrites(write5)
        .build();

    BatchWriteResponse group1Response = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .build();

    BatchWriteResponse group2Response = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .build();

    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withBatchMaxCount(maxValuesPerGroup)
        .build();
    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(options));
    FlushBuffer<Element<Write>> flushBuffer2 = spy(newFlushBuffer(options));

    when(processContext.element()).thenReturn(write0, write1, write2, write3, write4, write5);

    when(rpcQos.newWriteAttempt(any()))
        .thenReturn(attempt, attempt, attempt, attempt, attempt, attempt2, attempt2, attempt2)
        .thenThrow(new IllegalStateException("too many attempts"));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt2.awaitSafeToProceed(any())).thenReturn(true);

    when(attempt.<Write, Element<Write>>newFlushBuffer(enqueue0)).thenReturn(newFlushBuffer(options));
    when(attempt.<Write, Element<Write>>newFlushBuffer(enqueue1)).thenReturn(newFlushBuffer(options));
    when(attempt.<Write, Element<Write>>newFlushBuffer(enqueue2)).thenReturn(newFlushBuffer(options));
    when(attempt.<Write, Element<Write>>newFlushBuffer(enqueue3)).thenReturn(newFlushBuffer(options));
    when(attempt.<Write, Element<Write>>newFlushBuffer(enqueue4)).thenReturn(flushBuffer);
    when(callable.call(expectedGroup1Request)).thenReturn(group1Response);

    when(attempt2.<Write, Element<Write>>newFlushBuffer(enqueue5)).thenReturn(newFlushBuffer(options));
    when(attempt2.<Write, Element<Write>>newFlushBuffer(finalFlush)).thenReturn(flushBuffer2);
    when(callable.call(expectedGroup2Request)).thenReturn(group2Response);

    runFunction(getFn(clock, ff, options), maxValuesPerGroup + 1);

    verify(attempt, times(1)).recordStartRequest(group1Rpc1Sta);
    verify(attempt, times(1)).recordSuccessfulRequest(group1Rpc1End, 5);
    verify(attempt, times(1)).completeSuccess();
    verify(attempt2, times(1)).recordStartRequest(group2Rpc1Sta);
    verify(attempt2, times(1)).recordSuccessfulRequest(group2Rpc1End, 1);
    verify(attempt2, times(1)).completeSuccess();
    verify(callable, times(1)).call(expectedGroup1Request);
    verify(callable, times(1)).call(expectedGroup2Request);
    verifyNoMoreInteractions(callable);
    verify(flushBuffer, times(maxValuesPerGroup)).offer(any());
    verify(flushBuffer2, times(1)).offer(any());
  }

  @Test
  public final void endToEnd_partialSuccessReturnsWritesToQueue() throws Exception {
    Write write0 = FirestoreProtoHelpers.newWrite(0);
    Write write1 = FirestoreProtoHelpers.newWrite(1);
    Write write2 = FirestoreProtoHelpers.newWrite(2);
    Write write3 = FirestoreProtoHelpers.newWrite(3);
    Write write4 = FirestoreProtoHelpers.newWrite(4);

    BatchWriteRequest expectedRequest1 = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)")
        .addWrites(write0)
        .addWrites(write1)
        .addWrites(write2)
        .addWrites(write3)
        .addWrites(write4)
        .build();

    BatchWriteResponse response1 = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .addStatus(statusForCode(Code.INVALID_ARGUMENT))
        .addStatus(statusForCode(Code.FAILED_PRECONDITION))
        .addStatus(statusForCode(Code.UNAUTHENTICATED))
        .addStatus(STATUS_OK)
        .build();

    BatchWriteRequest expectedRequest2 = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)")
        .addWrites(write1)
        .addWrites(write2)
        .addWrites(write3)
        .build();

    BatchWriteResponse response2 = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .addStatus(STATUS_OK)
        .build();

    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withMaxAttempts(1)
        .withBatchMaxCount(5)
        .build();

    when(processContext.element())
        .thenReturn(write0, write1, write2, write3, write4)
        .thenThrow(new IllegalStateException("too many calls"));

    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(any())).thenAnswer(invocation -> newFlushBuffer(options));
    when(attempt.isCodeRetryable(Code.INVALID_ARGUMENT)).thenReturn(true);
    when(attempt.isCodeRetryable(Code.FAILED_PRECONDITION)).thenReturn(true);
    when(attempt.isCodeRetryable(Code.UNAUTHENTICATED)).thenReturn(true);

    ArgumentCaptor<BatchWriteRequest> requestCaptor1 = ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor1.capture())).thenReturn(response1);

    Fn fn = getFn(clock, ff, options);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window); // write0
    fn.processElement(processContext, window); // write1
    fn.processElement(processContext, window); // write2
    fn.processElement(processContext, window); // write3
    fn.processElement(processContext, window); // write4

    assertEquals(expectedRequest1, requestCaptor1.getValue());

    List<WriteElement> expectedRemainingWrites = newArrayList(
        new WriteElement(1, write1, window),
        new WriteElement(2, write2, window),
        new WriteElement(3, write3, window)
    );
    List<WriteElement> actualWrites = new ArrayList<>(fn.writes);

    Instant flush1Attempt1Begin = Instant.ofEpochMilli(4);
    Instant flush1Attempt1RpcSta = Instant.ofEpochMilli(5);
    Instant flush1Attempt1RpcEnd = Instant.ofEpochMilli(6);
    Instant flush1Attempt2Begin = Instant.ofEpochMilli(7);

    assertEquals(expectedRemainingWrites, actualWrites);
    assertEquals(5, fn.queueNextEntryPriority);
    Instant flush2Attempt1Begin = Instant.ofEpochMilli(8);
    Instant flush2Attempt1Sta = Instant.ofEpochMilli(9);
    Instant flush2Attempt1End = Instant.ofEpochMilli(10);

    ArgumentCaptor<BatchWriteRequest> requestCaptor2 = ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor2.capture())).thenReturn(response2);
    fn.finishBundle(finishBundleContext);
    assertEquals(expectedRequest2, requestCaptor2.getValue());

    assertEquals(0, fn.queueNextEntryPriority);

    verify(attempt, times(1)).newFlushBuffer(flush1Attempt1Begin);
    verify(attempt, times(1)).recordStartRequest(flush1Attempt1RpcSta);
    verify(attempt, times(1)).recordSuccessfulRequest(flush1Attempt1RpcEnd, 2);
    verify(attempt, times(1)).recordFailedRequest(flush1Attempt1RpcEnd, 3);
    verify(attempt, times(1)).newFlushBuffer(flush1Attempt2Begin);

    verify(attempt, times(1)).newFlushBuffer(flush2Attempt1Begin);
    verify(attempt, times(1)).recordStartRequest(flush2Attempt1Sta);
    verify(attempt, times(1)).recordSuccessfulRequest(flush2Attempt1End, 3);
    verify(attempt, times(1)).completeSuccess();
    verify(callable, times(2)).call(any());
    verifyNoMoreInteractions(callable);
  }

  @Test
  public final void writesRemainInQueueWhenFlushIsNotReadyAndThenFlushesInFinishBundle() throws Exception {
    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withMaxAttempts(1)
        .build();

    Write write = FirestoreProtoHelpers.newWrite();
    BatchWriteRequest expectedRequest = BatchWriteRequest.newBuilder()
        .setDatabase("projects/testing-project/databases/(default)")
        .addWrites(write)
        .build();
    BatchWriteResponse response = BatchWriteResponse.newBuilder()
        .addStatus(STATUS_OK)
        .build();

    when(processContext.element()).thenReturn(write)
        .thenThrow(new IllegalStateException("too many element calls"));
    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt, attempt2)
        .thenThrow(new IllegalStateException("too many attempt calls"));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt2.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));
    when(attempt2.<Write, Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));

    Fn fn = getFn(clock, ff, options);
    fn.populateDisplayData(displayDataBuilder);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window);

    assertEquals(1, fn.writes.size());
    verify(attempt, never()).recordSuccessfulRequest(any(), anyInt());
    verify(attempt, never()).recordFailedRequest(any(), anyInt());
    verify(attempt, never()).checkCanRetry(any());
    verify(attempt, never()).completeSuccess();

    Instant attempt2RpcSta = Instant.ofEpochMilli(2);
    Instant attempt2RpcEnd = Instant.ofEpochMilli(3);

    ArgumentCaptor<BatchWriteRequest> requestCaptor = ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor.capture())).thenReturn(response);
    fn.finishBundle(finishBundleContext);

    assertEquals(0, fn.writes.size());
    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(attempt2, times(1)).recordStartRequest(attempt2RpcSta);
    verify(attempt2, times(1)).recordSuccessfulRequest(attempt2RpcEnd, 1);
    verify(attempt2, never()).recordFailedRequest(any(), anyInt());
    verify(attempt2, never()).checkCanRetry(any());
    verify(attempt2, times(1)).completeSuccess();
  }

  @Test
  public final void writeElementSerializationSizeCached() {
    Write write = FirestoreProtoHelpers.newWrite();
    int expectedSerializedSize = write.toByteArray().length;
    WriteElement element = new WriteElement(1, write, window);
    element.getSerializedSize();
    assertTrue(element.serializedSizeComputed);
    assertEquals(expectedSerializedSize, element.serializedSize);
  }

  @Test
  public final void queuedWritesMaintainPriorityIfNotFlushed() throws Exception {
    RpcQosOptions options = RPC_QOS_OPTIONS.toBuilder()
        .withMaxAttempts(1)
        .build();

    Write write0 = FirestoreProtoHelpers.newWrite(0);
    Write write1 = FirestoreProtoHelpers.newWrite(1);
    Write write2 = FirestoreProtoHelpers.newWrite(2);
    Write write3 = FirestoreProtoHelpers.newWrite(3);
    Write write4 = FirestoreProtoHelpers.newWrite(4);
    Instant write4Start = Instant.ofEpochMilli(4);

    when(processContext.element())
        .thenReturn(write0, write1, write2, write3, write4)
        .thenThrow(new IllegalStateException("too many calls"));

    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Write, Element<Write>>newFlushBuffer(any())).thenAnswer(invocation -> newFlushBuffer(options));

    Fn fn = getFn(clock, ff, options);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window); // write0
    fn.processElement(processContext, window); // write1
    fn.processElement(processContext, window); // write2
    fn.processElement(processContext, window); // write3
    fn.processElement(processContext, window); // write4

    List<WriteElement> expectedWrites = newArrayList(
        new WriteElement(0, write0, window),
        new WriteElement(1, write1, window),
        new WriteElement(2, write2, window),
        new WriteElement(3, write3, window),
        new WriteElement(4, write4, window)
    );
    List<WriteElement> actualWrites = new ArrayList<>(fn.writes);

    assertEquals(expectedWrites, actualWrites);
    assertEquals(5, fn.queueNextEntryPriority);
    verify(attempt, times(1)).newFlushBuffer(write4Start);
    verifyNoMoreInteractions(callable);
  }

  @Override
  protected final Fn getFn() {
    return getFn(JodaClock.DEFAULT, FirestoreStatefulComponentFactory.INSTANCE, RPC_QOS_OPTIONS);
  }

  protected abstract Fn getFn(JodaClock clock, FirestoreStatefulComponentFactory ff, RpcQosOptions rpcQosOptions);

  @Override
  protected final void processElementsAndFinishBundle(Fn fn, int processElementCount) throws Exception {
    try {
      for (int i = 0; i < processElementCount; i++) {
        fn.processElement(processContext, window);
      }
    } finally {
      fn.finishBundle(finishBundleContext);
    }
  }

  protected FlushBufferImpl<Write, Element<Write>> newFlushBuffer(RpcQosOptions options) {
    return new FlushBufferImpl<>(
        options.getBatchMaxCount(),
        options.getBatchMaxBytes()
    );
  }

  protected static Status statusForCode(Code code) {
    return Status.newBuilder().setCode(code.getNumber()).build();
  }


}
