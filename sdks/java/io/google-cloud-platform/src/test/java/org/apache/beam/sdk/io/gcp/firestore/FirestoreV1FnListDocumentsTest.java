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
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.common.collect.ImmutableMap;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.Value;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.ListDocumentsFn;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class FirestoreV1FnListDocumentsTest extends
    BaseFirestoreV1ReadFnTest<ListDocumentsRequest, ListDocumentsResponse> {

  @Mock
  private UnaryCallable<ListDocumentsRequest, ListDocumentsPagedResponse> callable;
  @Mock
  private ListDocumentsPagedResponse pagedResponse1;
  @Mock
  private ListDocumentsPage page1;
  @Mock
  private ListDocumentsPagedResponse pagedResponse2;
  @Mock
  private ListDocumentsPage page2;

  @Test
  public void endToEnd() throws Exception {
    // First page of the response
    ListDocumentsRequest request1 = ListDocumentsRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .build();
    ListDocumentsResponse response1 = ListDocumentsResponse.newBuilder()
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-1")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-2")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-3")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .setNextPageToken("page2")
        .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(pagedResponse1.getPage()).thenReturn(page1);
    when(callable.call(request1)).thenReturn(pagedResponse1);

    // Second page of the response
    ListDocumentsRequest request2 = ListDocumentsRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .setPageToken("page2")
        .build();
    ListDocumentsResponse response2 = ListDocumentsResponse.newBuilder()
        .addDocuments(
            Document.newBuilder()
                .setName("doc_2-1")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse2.getPage()).thenReturn(page2);
    when(callable.call(request2)).thenReturn(pagedResponse2);

    when(rpc.listDocumentsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any())).thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));


    ArgumentCaptor<ListDocumentsResponse> responses = ArgumentCaptor.forClass(
        ListDocumentsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListDocumentsFn fn = new ListDocumentsFn(clock, ff, options);

    runFunction(fn);

    List<ListDocumentsResponse> expected = newArrayList(response1, response2);
    List<ListDocumentsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    // First page of the response
    ListDocumentsRequest request1 = ListDocumentsRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .build();
    ListDocumentsResponse response1 = ListDocumentsResponse.newBuilder()
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-1")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-2")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .addDocuments(
            Document.newBuilder()
                .setName("doc_1-3")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .setNextPageToken("page2")
        .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(pagedResponse1.getPage()).thenReturn(page1);
    when(callable.call(request1)).thenReturn(pagedResponse1);

    // Second page of the response
    ListDocumentsRequest request2 = ListDocumentsRequest.newBuilder()
        .setParent(String.format("projects/%s/databases/(default)/document", projectId))
        .setPageToken("page2")
        .build();
    ListDocumentsResponse response2 = ListDocumentsResponse.newBuilder()
        .addDocuments(
            Document.newBuilder()
                .setName("doc_2-1")
                .putAllFields(ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build()
        )
        .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse2.getPage()).thenReturn(page2);
    when(callable.call(request2))
        .thenThrow(RETRYABLE_ERROR)
        .thenReturn(pagedResponse2);
    doNothing().when(attempt).checkCanRetry(RETRYABLE_ERROR);

    when(rpc.listDocumentsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreRpc(any())).thenReturn(rpc);

    ArgumentCaptor<ListDocumentsResponse> responses = ArgumentCaptor.forClass(
        ListDocumentsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListDocumentsFn fn = new ListDocumentsFn(clock, ff, RPC_QOS_OPTIONS);

    runFunction(fn);

    List<ListDocumentsResponse> expected = newArrayList(response1, response2);
    List<ListDocumentsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);

    verify(callable, times(1)).call(request1);
    verify(callable, times(2)).call(request2);
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public ListDocumentsRequest getRequest() {
        return ListDocumentsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreRpc rpc) {
        when(rpc.listDocumentsPagedCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(ListDocumentsRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected ListDocumentsFn getFn(JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory, RpcQosOptions rpcQosOptions) {
    return new ListDocumentsFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }
}
