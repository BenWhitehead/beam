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

import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.util.Objects;

/**
 * A "sealed" class hierarchy providing a common base type to supported Firestore requests.
 * <p/>
 * This class hierarchy is strictly an internal API and should never be seen or referenced by anyone
 * using the public API.
 */
final class FirestoreRequest {

  /**
   * "Sealed" base class for all supported Request types that can be "sent" via the adapters
   * provided by {@link FirestoreSdk} and handled by {@link FirestoreSdkFn}. This class provides a
   * base type used as an upperbound on the types which can be emitted by a {@link FirestoreSdkFn}.
   * <p/>
   * Ideally this class would have an {@code unwrap} method declared directly on it, however due to
   * an outstanding issues on generic parameter type resolution the class {@link
   * org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.Unwrapper} has been created to take over this
   * role.
   */
  static abstract class FirestoreRequestHolder<Request extends Message> implements Serializable {

    private final Request request;

    private FirestoreRequestHolder(Request request) {
      this.request = requireNonNull(request, "request must be non null");
    }

    public Request getRequest() {
      return request;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FirestoreRequestHolder)) {
        return false;
      }
      FirestoreRequestHolder<?> that = (FirestoreRequestHolder<?>) o;
      return request.equals(that.request);
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public String toString() {
      return this.getClass().getName() + "{" +
          "request=" + request +
          '}';
    }
  }

  static QueryRequestHolder create(RunQueryRequest request) {
    return new QueryRequestHolder(request);
  }

  static CommitRequestHolder create(CommitRequest request) {
    return new CommitRequestHolder(request);
  }

  static BatchWriteRequestHolder create(BatchWriteRequest request) {
    return new BatchWriteRequestHolder(request);
  }

  static ListCollectionIdsRequestHolder create(ListCollectionIdsRequest request) {
    return new ListCollectionIdsRequestHolder(request);
  }

  static PartitionQueryRequestHolder create(PartitionQueryRequest request) {
    return new PartitionQueryRequestHolder(request);
  }

  static ListDocumentsRequestHolder create(ListDocumentsRequest request) {
    return new ListDocumentsRequestHolder(request);
  }

  static BatchGetDocumentsRequestHolder create(BatchGetDocumentsRequest request) {
    return new BatchGetDocumentsRequestHolder(request);
  }

  static final class QueryRequestHolder extends FirestoreRequestHolder<RunQueryRequest> {
    private QueryRequestHolder(RunQueryRequest runQueryRequest) {
      super(runQueryRequest);
    }
  }

  static final class CommitRequestHolder extends FirestoreRequestHolder<CommitRequest> {
    private CommitRequestHolder(CommitRequest commitRequest) {
      super(commitRequest);
    }
  }

  static final class BatchWriteRequestHolder extends FirestoreRequestHolder<BatchWriteRequest> {
    private BatchWriteRequestHolder(BatchWriteRequest batchWriteRequest) {
      super(batchWriteRequest);
    }
  }

  static final class ListCollectionIdsRequestHolder extends FirestoreRequestHolder<ListCollectionIdsRequest> {
    private ListCollectionIdsRequestHolder(ListCollectionIdsRequest listCollectionIdsRequest) {
      super(listCollectionIdsRequest);
    }
  }

  static final class PartitionQueryRequestHolder extends FirestoreRequestHolder<PartitionQueryRequest> {
    private PartitionQueryRequestHolder(PartitionQueryRequest partitionQueryRequest) {
      super(partitionQueryRequest);
    }
  }

  static final class ListDocumentsRequestHolder extends FirestoreRequestHolder<ListDocumentsRequest> {
    private ListDocumentsRequestHolder(ListDocumentsRequest listDocumentsRequest) {
      super(listDocumentsRequest);
    }
  }

  static final class BatchGetDocumentsRequestHolder extends FirestoreRequestHolder<BatchGetDocumentsRequest> {
    private BatchGetDocumentsRequestHolder(BatchGetDocumentsRequest batchGetDocumentsRequest) {
      super(batchGetDocumentsRequest);
    }
  }
}
