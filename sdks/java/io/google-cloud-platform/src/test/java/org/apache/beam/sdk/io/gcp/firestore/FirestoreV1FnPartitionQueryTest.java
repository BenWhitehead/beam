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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPage;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.Value;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryPair;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class FirestoreV1FnPartitionQueryTest extends
    BaseFirestoreV1ReadFnTest<PartitionQueryRequest, PartitionQueryPair> {

  @Mock
  private UnaryCallable<PartitionQueryRequest, PartitionQueryPagedResponse> callable;
  @Mock
  private PartitionQueryPagedResponse pagedResponse1;
  @Mock
  private PartitionQueryPage page1;
  @Mock
  private PartitionQueryPagedResponse pagedResponse2;
  @Mock
  private PartitionQueryPage page2;

  @Test
  public void endToEnd() throws Exception {
    // First page of the response
    PartitionQueryRequest request1 = PartitionQueryRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .build();
    PartitionQueryResponse response1 = PartitionQueryResponse.newBuilder()
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-100"))
        )
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-200"))
        )
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-300"))
        )
        .setNextPageToken("page2")
        .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(pagedResponse1.getPage()).thenReturn(page1);
    when(callable.call(request1)).thenReturn(pagedResponse1);

    // Second page of the response
    PartitionQueryRequest request2 = PartitionQueryRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .setPageToken("page2")
        .build();
    PartitionQueryResponse response2 = PartitionQueryResponse.newBuilder()
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-400"))
        )
        .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse2.getPage()).thenReturn(page2);
    when(callable.call(request2)).thenReturn(pagedResponse2);

    when(rpc.partitionQueryPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any())).thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<PartitionQueryPair> responses = ArgumentCaptor.forClass(
        PartitionQueryPair.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    PartitionQueryFn fn = new PartitionQueryFn(clock, ff, options);

    runFunction(fn);

    List<PartitionQueryPair> expected = newArrayList(
        new PartitionQueryPair(request1, response1),
        new PartitionQueryPair(request1, response2)
    );
    List<PartitionQueryPair> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    // First page of the response
    PartitionQueryRequest request1 = PartitionQueryRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .build();
    PartitionQueryResponse response1 = PartitionQueryResponse.newBuilder()
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-100"))
        )
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-200"))
        )
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-300"))
        )
        .setNextPageToken("page2")
        .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(pagedResponse1.getPage()).thenReturn(page1);
    when(callable.call(request1)).thenReturn(pagedResponse1);

    // Second page of the response
    PartitionQueryRequest request2 = PartitionQueryRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .setPageToken("page2")
        .build();
    PartitionQueryResponse response2 = PartitionQueryResponse.newBuilder()
        .addPartitions(
            Cursor.newBuilder()
                .addValues(Value.newBuilder().setReferenceValue("doc-400"))
        )
        .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse2.getPage()).thenReturn(page2);
    when(callable.call(request2))
        .thenThrow(RETRYABLE_ERROR)
        .thenReturn(pagedResponse2);
    doNothing().when(attempt).checkCanRetry(RETRYABLE_ERROR);

    when(rpc.partitionQueryPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreRpc(any())).thenReturn(rpc);

    ArgumentCaptor<PartitionQueryPair> responses = ArgumentCaptor.forClass(
        PartitionQueryPair.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    PartitionQueryFn fn = new PartitionQueryFn(clock, ff, RPC_QOS_OPTIONS);

    runFunction(fn);

    List<PartitionQueryPair> expected = newArrayList(
        new PartitionQueryPair(request1, response1),
        new PartitionQueryPair(request1, response2)
    );
    List<PartitionQueryPair> allValues = responses.getAllValues();
    assertEquals(expected, allValues);

    verify(callable, times(1)).call(request1);
    verify(callable, times(2)).call(request2);
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public PartitionQueryRequest getRequest() {
        return PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreRpc rpc) {
        when(rpc.partitionQueryPagedCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(PartitionQueryRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected PartitionQueryFn getFn(JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory, RpcQosOptions rpcQosOptions) {
    return new PartitionQueryFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }
}
