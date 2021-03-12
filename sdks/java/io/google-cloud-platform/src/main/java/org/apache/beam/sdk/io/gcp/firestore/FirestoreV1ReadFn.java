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

import com.google.api.gax.paging.AbstractPage;
import com.google.api.gax.paging.AbstractPagedListResponse;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPage;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.Value;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1Fn.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * A collection of {@link org.apache.beam.sdk.transforms.DoFn DoFn}s for each of the supported read
 * RPC methods from the Cloud Firestore V1 API.
 */
final class FirestoreV1ReadFn {

  /**
   * {@link DoFn} for Firestore V1 {@link RunQueryRequest}s.
   * <p/>
   * This Fn uses a stream to obtain responses, each response from the stream will be output to the
   * next stage of the pipeline.
   * <p/>
   * If an error is encountered while reading from the stream, the stream will attempt to resume
   * rather than starting over. The restarting of the stream will continue within the scope of the
   * completion of the request (meaning any possibility of resumption is contingent upon an attempt
   * being available in the Qos budget).
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class RunQueryFn
      extends StreamingFirestoreV1ReadFn<
      RunQueryRequest,
      RunQueryResponse
      > {

    RunQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.RunQuery;
    }

    @Override
    protected ServerStreamingCallable<RunQueryRequest, RunQueryResponse> getCallable(
        FirestoreRpc firestoreRpc) {
      return firestoreRpc.runQueryCallable();
    }

    @Override
    protected RunQueryRequest setStartFrom(RunQueryRequest element, RunQueryResponse runQueryResponse) {
      StructuredQuery query = element.getStructuredQuery();
      StructuredQuery.Builder builder;
      List<Order> orderByList = query.getOrderByList();
      // if the orderByList is empty that means the default sort of "__name__ ASC" will be used
      // Before we can set the cursor to the last document name read, we need to explicitly add
      // the order of "__name__ ASC" because a cursor value must map to an order by
      if (orderByList.isEmpty()) {
        builder = query.toBuilder()
            .addOrderBy(
                Order.newBuilder()
                    .setField(FieldReference.newBuilder().setFieldPath("__name__").build())
                    .setDirection(Direction.ASCENDING)
                    .build()
            )
            .setStartAt(
                Cursor.newBuilder()
                    .setBefore(false)
                    .addValues(
                        Value.newBuilder()
                            .setReferenceValue(runQueryResponse.getDocument().getName())
                            .build()
                    )
            );
      } else {
        Cursor.Builder cursor = Cursor.newBuilder()
            .setBefore(false);
        Map<String, Value> fieldsMap = runQueryResponse.getDocument().getFieldsMap();
        for (Order order : orderByList) {
          Value value = fieldsMap.get(order.getField().getFieldPath());
          if (value != null) {
            cursor.addValues(value);
          }
        }
        builder = query.toBuilder()
            .setStartAt(cursor.build());
      }
      return element.toBuilder()
          .setStructuredQuery(builder.build())
          .build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link PartitionQueryRequest}s.
   * <p/>
   * This Fn uses pagination to obtain responses, the response from each page will be output to the
   * next stage of the pipeline.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class PartitionQueryFn
      extends PaginatedFirestoreV1ReadFn<
      PartitionQueryRequest,
      PartitionQueryPagedResponse,
      PartitionQueryPage,
      PartitionQueryResponse,
      PartitionQueryPair
      > {

    PartitionQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, PartitionQueryPair::new);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.PartitionQuery;
    }

    @Override
    protected UnaryCallable<PartitionQueryRequest, PartitionQueryPagedResponse> getCallable(
        FirestoreRpc firestoreRpc) {
      return firestoreRpc.partitionQueryPagedCallable();
    }

    @Override
    protected PartitionQueryRequest setPageToken(PartitionQueryRequest request,
        String nextPageToken) {
      return request.toBuilder().setPageToken(nextPageToken).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link ListDocumentsRequest}s.
   * <p/>
   * This Fn uses pagination to obtain responses, the response from each page will be output to the
   * next stage of the pipeline.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class ListDocumentsFn
      extends PaginatedFirestoreV1ReadFn<
      ListDocumentsRequest,
      ListDocumentsPagedResponse,
      ListDocumentsPage,
      ListDocumentsResponse,
      ListDocumentsResponse
      > {

    ListDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, PaginatedFirestoreV1ReadFn.outputResponse());
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.ListDocuments;
    }

    @Override
    protected UnaryCallable<ListDocumentsRequest, ListDocumentsPagedResponse> getCallable(
        FirestoreRpc firestoreRpc) {
      return firestoreRpc.listDocumentsPagedCallable();
    }

    @Override
    protected ListDocumentsRequest setPageToken(ListDocumentsRequest request,
        String nextPageToken) {
      return request.toBuilder().setPageToken(nextPageToken).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link ListCollectionIdsRequest}s.
   * <p/>
   * This Fn uses pagination to obtain responses, the response from each page will be output to the
   * next stage of the pipeline.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class ListCollectionIdsFn
      extends PaginatedFirestoreV1ReadFn<
      ListCollectionIdsRequest,
      ListCollectionIdsPagedResponse,
      ListCollectionIdsPage,
      ListCollectionIdsResponse,
      ListCollectionIdsResponse
      > {

    ListCollectionIdsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, PaginatedFirestoreV1ReadFn.outputResponse());
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.ListCollectionIds;
    }

    @Override
    protected UnaryCallable<ListCollectionIdsRequest, ListCollectionIdsPagedResponse> getCallable(
        FirestoreRpc firestoreRpc) {
      return firestoreRpc.listCollectionIdsPagedCallable();
    }

    @Override
    protected ListCollectionIdsRequest setPageToken(ListCollectionIdsRequest request,
        String nextPageToken) {
      return request.toBuilder().setPageToken(nextPageToken).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link BatchGetDocumentsRequest}s.
   * <p/>
   * This Fn uses a stream to obtain responses, each response from the stream will be output to the
   * next stage of the pipeline.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class BatchGetDocumentsFn
      extends StreamingFirestoreV1ReadFn<
      BatchGetDocumentsRequest,
      BatchGetDocumentsResponse
      > {

    BatchGetDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1Fn.V1FnRpcAttemptContext.BatchGetDocuments;
    }

    @Override
    protected ServerStreamingCallable<BatchGetDocumentsRequest, BatchGetDocumentsResponse> getCallable(
        FirestoreRpc firestoreRpc) {
      return firestoreRpc.batchGetDocumentsCallable();
    }

    @Override
    protected BatchGetDocumentsRequest setStartFrom(BatchGetDocumentsRequest originalRequest, BatchGetDocumentsResponse mostRecentResponse) {
      int startIndex = -1;
      ProtocolStringList documentsList = originalRequest.getDocumentsList();
      String missing = mostRecentResponse.getMissing();
      String foundName = mostRecentResponse.hasFound() ? mostRecentResponse.getFound().getName() : null;
      // we only scan until the second to last originalRequest. If the final element were to be reached
      // the full request would be complete and we wouldn't be in this scenario
      int maxIndex = documentsList.size() - 2;
      for (int i = 0; i <= maxIndex; i++) {
        String docName = documentsList.get(i);
        if (docName.equals(missing) || docName.equals(foundName)) {
          startIndex = i;
          break;
        }
      }
      if (0 <= startIndex) {
        BatchGetDocumentsRequest.Builder builder = originalRequest.toBuilder()
            .clearDocuments();
        documentsList.stream()
            .skip(startIndex + 1) // start from the next entry from the one we found
            .forEach(builder::addDocuments);
        return builder.build();
      }
      // unable to find a match, return the original request
      return originalRequest;
    }
  }

  /**
   * {@link DoFn} Providing support for a Read type RPC operation which uses a Stream rather than
   * pagination. Each response from the stream will be output to the next stage of the pipeline.
   * <p/>
   * All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   *
   * @param <In> Request type
   * @param <Out> Response type
   */
  private abstract static class StreamingFirestoreV1ReadFn<In extends Message, Out extends Message> extends
      BaseFirestoreV1ReadFn<In, Out> {

    protected StreamingFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions
    ) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    protected abstract ServerStreamingCallable<In, Out> getCallable(FirestoreRpc firestoreRpc);

    protected abstract In setStartFrom(In element, Out out);

    @Override
    public final void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings("nullness") // for some reason requireNonNull thinks its parameter but be non-null...
      final In element = requireNonNull(c.element(), "c.element() must be non null");

      RpcQos.RpcReadAttempt attempt = rpcQos.newReadAttempt(getRpcAttemptContext());
      Out lastReceivedValue = null;
      while (true) {
        if (!attempt.awaitSafeToProceed(clock.instant())) {
          continue;
        }

        Instant start = clock.instant();
        try {
          In request = lastReceivedValue == null ? element : setStartFrom(element, lastReceivedValue);
          attempt.recordStartRequest(start);
          ServerStream<Out> serverStream = getCallable(firestoreRpc).call(request);
          attempt.recordSuccessfulRequest(clock.instant());
          for (Out out : serverStream) {
            lastReceivedValue = out;
            attempt.recordStreamValue(clock.instant());
            c.output(out);
          }
          attempt.completeSuccess();
          break;
        } catch (RuntimeException exception) {
          attempt.recordFailedRequest(clock.instant());
          attempt.checkCanRetry(exception);
        }
      }
    }
  }

  /**
   * {@link DoFn} Providing support for a Read type RPC operation which uses pagination rather than
   * a Stream
   *
   * @param <Request> Request type
   * @param <Out> Response type
   */
  @SuppressWarnings({
      // errorchecker doesn't like the second ? on PagedResponse, seemingly because of different
      // recursion depth limits; 3 on the found vs 4 on the required.
      // The second ? is the type of collection the paged response uses to hold all responses if
      // trying to expand all pages to a single collection. We are emitting a single page at at time
      // while tracking read progress so we can resume if an error has occurred and we still have
      // attempt budget available.
      "type.argument.type.incompatible"
  })
  private abstract static class PaginatedFirestoreV1ReadFn<
      Request extends Message,
      PagedResponse extends AbstractPagedListResponse<Request, Response, ?, Page, ?>,
      Page extends AbstractPage<Request, Response, ?, Page>,
      Response extends Message,
      Out
      > extends BaseFirestoreV1ReadFn<Request, Out> {

    private final SerializableBiFunction<Request, Response, Out> outputTransform;

    protected PaginatedFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        SerializableBiFunction<Request, Response, Out> outputTransform) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
      this.outputTransform = outputTransform;
    }

    protected abstract UnaryCallable<Request, PagedResponse> getCallable(FirestoreRpc firestoreRpc);

    protected abstract Request setPageToken(Request request, String nextPageToken);

    @Override
    public final void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings("nullness") // for some reason requireNonNull thinks its parameter but be non-null...
      final Request element = requireNonNull(c.element(), "c.element() must be non null");

      RpcQos.RpcReadAttempt attempt = rpcQos.newReadAttempt(getRpcAttemptContext());
      String nextPageToken = null;
      while (true) {
        if (!attempt.awaitSafeToProceed(clock.instant())) {
          continue;
        }

        Instant start = clock.instant();
        try {
          Request request = nextPageToken == null ? element : setPageToken(element, nextPageToken);
          attempt.recordStartRequest(start);
          PagedResponse pagedResponse = getCallable(firestoreRpc).call(request);
          Page page = pagedResponse.getPage();
          Response response = page.getResponse();
          attempt.recordSuccessfulRequest(clock.instant());
          c.output(outputTransform.apply(element, response));
          if (page.hasNextPage()) {
            nextPageToken = page.getNextPageToken();
          } else {
            attempt.completeSuccess();
            break;
          }
        } catch (RuntimeException exception) {
          attempt.recordFailedRequest(clock.instant());
          attempt.checkCanRetry(exception);
        }
      }
    }

    private static <Request, Response> SerializableBiFunction<Request, Response, Response> outputResponse() {
      return (request, response) -> response;
    }
  }

  /**
   * Base class for all {@link org.apache.beam.sdk.transforms.DoFn DoFn}s which provide access to
   * RPCs from the Cloud Firestore V1 API.
   * <p/>
   * This class takes care of common lifecycle elements and transient state management for
   * subclasses allowing subclasses to provide the minimal implementation for {@link
   * NonWindowAwareDoFn#processElement(DoFn.ProcessContext)}}
   *
   * @param <In> The type of element coming into this {@link DoFn}
   * @param <Out> The type of element output from this {@link DoFn}
   */
  abstract static class BaseFirestoreV1ReadFn<In, Out> extends NonWindowAwareDoFn<In, Out> implements HasRpcAttemptContext {

    protected final JodaClock clock;
    protected final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    protected final RpcQosOptions rpcQosOptions;

    // transient running state information, not important to any possible checkpointing
    protected transient FirestoreRpc firestoreRpc;
    protected transient RpcQos rpcQos;
    protected transient String projectId;

    @SuppressWarnings("initialization.fields.uninitialized") // allow transient fields to be managed by component lifecycle
    protected BaseFirestoreV1ReadFn(JodaClock clock, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory, RpcQosOptions rpcQosOptions) {
      this.clock = requireNonNull(clock, "clock must be non null");
      this.firestoreStatefulComponentFactory = requireNonNull(firestoreStatefulComponentFactory, "firestoreFactory must be non null");
      this.rpcQosOptions = requireNonNull(rpcQosOptions, "rpcQosOptions must be non null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setup() {
      rpcQos = firestoreStatefulComponentFactory.getRpcQos(rpcQosOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void startBundle(StartBundleContext c) {
      String project = c.getPipelineOptions().as(GcpOptions.class).getProject();
      projectId = requireNonNull(project, "project must be defined on GcpOptions of PipelineOptions");
      firestoreRpc = firestoreStatefulComponentFactory.getFirestoreRpc(c.getPipelineOptions());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("nullness") // allow clearing transient fields
    @Override
    public void finishBundle() throws Exception {
      projectId = null;
      firestoreRpc.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void populateDisplayData(@edu.umd.cs.findbugs.annotations.NonNull DisplayData.Builder builder) {
      builder
          .include("rpcQosOptions", rpcQosOptions);
    }
  }

  /**
   * Tuple class for a PartitionQuery Request and Response Pair.
   * <p/>
   * When processing the response of a ParitionQuery it only is useful in the context of the
   * original request as the cursors from the response are tied to the index resolved from the
   * request. This class ties these two together so that they can be passed along the pipeline
   * together.
   */
  static final class PartitionQueryPair implements Serializable {
    private final PartitionQueryRequest request;
    private final PartitionQueryResponse response;

    @VisibleForTesting
    PartitionQueryPair(PartitionQueryRequest request, PartitionQueryResponse response) {
      this.request = request;
      this.response = response;
    }

    public PartitionQueryRequest getRequest() {
      return request;
    }

    public PartitionQueryResponse getResponse() {
      return response;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionQueryPair)) {
        return false;
      }
      PartitionQueryPair that = (PartitionQueryPair) o;
      return request.equals(that.request) &&
          response.equals(that.response);
    }

    @Override
    public int hashCode() {
      return Objects.hash(request, response);
    }
  }

}
