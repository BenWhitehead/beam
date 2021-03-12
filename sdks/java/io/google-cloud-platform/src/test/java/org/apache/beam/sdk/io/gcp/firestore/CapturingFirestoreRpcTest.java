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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchGetDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchWriteRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.CommitRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListCollectionIdsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.PartitionQueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.QueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdkFn.CapturingFirestoreRpc;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class CapturingFirestoreRpcTest {

  @Mock
  private DoFn<CommitRequest, CommitRequestHolder>.ProcessContext commitContext;
  @Mock
  private DoFn<BatchWriteRequest, BatchWriteRequestHolder>.ProcessContext batchWriteContext;

  @Mock
  private DoFn<ListCollectionIdsRequest, ListCollectionIdsRequestHolder>.ProcessContext listCollectionIdsContext;

  @Mock
  private DoFn<ListDocumentsRequest, ListDocumentsRequestHolder>.ProcessContext listDocumentsContext;

  @Mock
  private DoFn<PartitionQueryRequest, PartitionQueryRequestHolder>.ProcessContext partitionQueryContext;

  @Mock
  private DoFn<BatchGetDocumentsRequest, BatchGetDocumentsRequestHolder>.ProcessContext batchGetDocumentsContext;
  @Mock
  private ResponseObserver<BatchGetDocumentsResponse> batchGetDocumentsResponseObserver;

  @Mock
  private DoFn<RunQueryRequest, QueryRequestHolder>.ProcessContext runQueryContext;
  @Mock
  private ResponseObserver<RunQueryResponse> runQueryResponseObserver;

  @Test
  public void close() {
    CapturingFirestoreRpc<?, ?> rpc = new CapturingFirestoreRpc<>();
    assertNotNull(rpc.getExecutor());
    rpc.close();
    assertTrue(rpc.getExecutor().isShutdown());
  }

  @Test
  public void reset() {
    CommitRequest commitRequest = CommitRequest.newBuilder().build();
    CapturingFirestoreRpc<CommitRequest, CommitRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.commitCallable().call(commitRequest);

    rpc.reset();
    boolean output = rpc.output(CommitRequestHolder.class, commitContext);
    assertFalse(output);
    verify(commitContext, never()).output(any());
  }

  @Test
  public void commitCallable_captures() {
    CommitRequest commitRequest = CommitRequest.newBuilder().build();
    CapturingFirestoreRpc<CommitRequest, CommitRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.commitCallable().call(commitRequest);

    boolean output = rpc.output(CommitRequestHolder.class, commitContext);

    assertTrue(output);

    verify(commitContext, times(1)).output(any());
    verify(commitContext, times(1)).output(FirestoreRequest.create(commitRequest));
  }

  @Test
  public void batchWriteCallable() {
    BatchWriteRequest batchWriteRequest = BatchWriteRequest.newBuilder().build();
    CapturingFirestoreRpc<BatchWriteRequest, BatchWriteRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.batchWriteCallable().call(batchWriteRequest);

    boolean output = rpc.output(BatchWriteRequestHolder.class, batchWriteContext);

    assertTrue(output);

    verify(batchWriteContext, times(1)).output(any());
    verify(batchWriteContext, times(1)).output(FirestoreRequest.create(batchWriteRequest));
  }

  @Test(expected = IllegalStateException.class)
  public void beginTransactionCallable_IllegalStateException() {
    new CapturingFirestoreRpc<>().beginTransactionCallable();
  }

  @Test(expected = IllegalStateException.class)
  public void rollbackCallable_IllegalStateException() {
    new CapturingFirestoreRpc<>().rollbackCallable();
  }

  @Test
  public void listCollectionIdsCallable_captures() {
    ListCollectionIdsRequest listCollectionIdsRequest = ListCollectionIdsRequest.newBuilder().build();
    CapturingFirestoreRpc<ListCollectionIdsRequest, ListCollectionIdsRequestHolder> rpc = new CapturingFirestoreRpc<>();

    assertNull(rpc.listCollectionIdsPagedCallable().call(listCollectionIdsRequest));

    boolean output = rpc.output(ListCollectionIdsRequestHolder.class, listCollectionIdsContext);

    assertTrue(output);

    verify(listCollectionIdsContext, times(1)).output(any());
    verify(listCollectionIdsContext, times(1)).output(FirestoreRequest.create(listCollectionIdsRequest));
  }

  @Test
  public void listDocumentsCallable_captures() {
    ListDocumentsRequest listDocumentsRequest = ListDocumentsRequest.newBuilder().build();
    CapturingFirestoreRpc<ListDocumentsRequest, ListDocumentsRequestHolder> rpc = new CapturingFirestoreRpc<>();

    assertNull(rpc.listDocumentsPagedCallable().call(listDocumentsRequest));

    boolean output = rpc.output(ListDocumentsRequestHolder.class, listDocumentsContext);

    assertTrue(output);

    verify(listDocumentsContext, times(1)).output(any());
    verify(listDocumentsContext, times(1)).output(FirestoreRequest.create(listDocumentsRequest));
  }

  @Test
  public void partitionQueryCallable_captures() {
    PartitionQueryRequest partitionQueryRequest = PartitionQueryRequest.newBuilder().build();
    CapturingFirestoreRpc<PartitionQueryRequest, PartitionQueryRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.partitionQueryPagedCallable().call(partitionQueryRequest);

    boolean output = rpc.output(PartitionQueryRequestHolder.class, partitionQueryContext);

    assertTrue(output);

    verify(partitionQueryContext, times(1)).output(any());
    verify(partitionQueryContext, times(1)).output(FirestoreRequest.create(partitionQueryRequest));
  }

  @Test
  public void batchGetDocumentsCallable_captures() {
    BatchGetDocumentsRequest batchGetDocumentsRequest = BatchGetDocumentsRequest.newBuilder().build();
    CapturingFirestoreRpc<BatchGetDocumentsRequest, BatchGetDocumentsRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.batchGetDocumentsCallable().call(batchGetDocumentsRequest, batchGetDocumentsResponseObserver, GrpcCallContext.createDefault());


    boolean output = rpc.output(BatchGetDocumentsRequestHolder.class, batchGetDocumentsContext);

    assertTrue(output);

    verify(batchGetDocumentsResponseObserver, never()).onStart(any());
    verify(batchGetDocumentsResponseObserver, never()).onError(any());
    verify(batchGetDocumentsResponseObserver, never()).onResponse(any());
    verify(batchGetDocumentsContext, times(1)).output(any());
    verify(batchGetDocumentsContext, times(1)).output(FirestoreRequest.create(batchGetDocumentsRequest));
  }

  @Test
  public void runQueryCallable_captures() {
    RunQueryRequest runQueryRequest = RunQueryRequest.newBuilder().build();
    CapturingFirestoreRpc<RunQueryRequest, QueryRequestHolder> rpc = new CapturingFirestoreRpc<>();

    rpc.runQueryCallable().call(runQueryRequest, runQueryResponseObserver, GrpcCallContext.createDefault());

    boolean output = rpc.output(QueryRequestHolder.class, runQueryContext);

    assertTrue(output);

    verify(runQueryResponseObserver, never()).onStart(any());
    verify(runQueryResponseObserver, never()).onError(any());
    verify(runQueryResponseObserver, never()).onResponse(any());
    verify(runQueryContext, times(1)).output(any());
    verify(runQueryContext, times(1)).output(FirestoreRequest.create(runQueryRequest));
  }

  @Test(expected = IllegalStateException.class)
  public void listenCallable_IllegalStateException() {
    new CapturingFirestoreRpc<>().listenCallable();
  }
}
