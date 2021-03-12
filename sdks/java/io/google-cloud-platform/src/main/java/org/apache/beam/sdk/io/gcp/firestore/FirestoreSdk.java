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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryPartition;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchGetDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchWriteRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.CommitRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.FirestoreRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListCollectionIdsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.PartitionQueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.QueryRequestHolder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * IO-like adapter layer providing a managed way to extract request protos from operations performed
 * via the {@link Firestore} SDK.
 * <p/>
 * This class is part of the Firestore Connector DSL and should be accessed via {@link
 * FirestoreIO#sdk()}.
 * <p/>
 * An instance of {@link Firestore} is not required to be {@link java.io.Serializable Serializable}
 * and is therefore not capable for being passed as part of a pipeline. This class provides the
 * ability to create {@link PTransform}s for most operations available via {@link Firestore}. This
 * is accomplished by providing a function which along with the input is invoked with a lifecycle
 * managed instance of {@link Firestore}. When the function is invoked the proto request will be
 * captured and output, at which point the next stage of the pipeline can determine what to do with
 * the request.
 * <p/>
 * For example, if you wanted to list all documents of a collection:
 * <pre>{@code
 * PCollection<String> collectionIds = ...;
 * PCollection<ListDocumentsResponse> listDocumentResponses = collectionIds
 *     .apply(
 *         FirestoreIO.sdk().read()
 *             .<String>listDocuments((firestore, collectionId) -> firestore.collection(collectionId).listDocuments())
 *             .withDescription("/{collectionId}/*")
 *             .build()
 *     )
 *     .apply(FirestoreIO.v1().read().listDocuments().build());
 * }</pre>
 * <p/>
 * Each supported operation has a type safe builder and corresponding PTransform accessible via
 * {@link FirestoreSdk#read()} and {@link FirestoreSdk#write()}.
 * <p/>
 * <b>NOTE</b> Transactions are explicitly not supported. The multi-leg nature of transactions
 * (begin then commit) is not able to be expressed as a single request proto.
 *
 * @see FirestoreIO#sdk()
 */
@Immutable
public final class FirestoreSdk {
  static final FirestoreSdk INSTANCE = new FirestoreSdk();

  private static final Unwrapper<BatchGetDocumentsRequestHolder, BatchGetDocumentsRequest> UNWRAPPER_BATCH_GET =
      new Unwrapper<>(BatchGetDocumentsRequestHolder.class, BatchGetDocumentsRequest.class);
  private static final Unwrapper<ListDocumentsRequestHolder, ListDocumentsRequest> UNWRAPPER_LIST_DOCUMENTS =
      new Unwrapper<>(ListDocumentsRequestHolder.class, ListDocumentsRequest.class);
  private static final Unwrapper<ListCollectionIdsRequestHolder, ListCollectionIdsRequest> UNWRAPPER_LIST_COLLECTIONS =
      new Unwrapper<>(ListCollectionIdsRequestHolder.class, ListCollectionIdsRequest.class);
  private static final Unwrapper<QueryRequestHolder, RunQueryRequest> UNWRAPPER_QUERY =
      new Unwrapper<>(QueryRequestHolder.class, RunQueryRequest.class);
  private static final Unwrapper<PartitionQueryRequestHolder, PartitionQueryRequest> UNWRAPPER_PARTITION_QUERY =
      new Unwrapper<>(PartitionQueryRequestHolder.class, PartitionQueryRequest.class);
  private static final CommitUnwrapper UNWRAPPER_COMMIT = new CommitUnwrapper();
  private static final BatchWriteUnwrapper UNWRAPPER_BATCH_WRITE = new BatchWriteUnwrapper(); 


  private FirestoreSdk() {
  }

  /**
   * The class returned by this method provides the ability to create {@link PTransform PTransforms}
   * for most read operations available via {@link Firestore}.
   * <p/>
   * This method is part of the Firestore Connector DSL and should be accessed via {@link
   * FirestoreIO#sdk()}.
   * <p/>
   *
   * @return Type safe builder factory for read operations.
   * @see FirestoreIO#sdk()
   */
  public Read read() {
    return Read.INSTANCE;
  }

  /**
   * This class provides the ability to create {@link PTransform PTransforms} for single write and
   * batch write operations available via {@link Firestore}.
   * <p/>
   * This method is part of the Firestore Connector DSL and should be accessed via {@link
   * FirestoreIO#sdk()}.
   * <p/>
   *
   * @return Type safe builder factory for write operations.
   * @see FirestoreIO#sdk()
   */
  public Write write() {
    return Write.INSTANCE;
  }

  /**
   * Type safe builder factory for read operations.
   * <p/>
   * This class is part of the Firestore Connector DSL and should be accessed via {@link #read()
   * FirestoreIO.sdk().read()}.
   * <p/>
   * This class provides access to a set of type safe builders for read operations. Each builder provides
   * the ability to create a {@link PTransform} which allows for the use of the respective read
   * operation available via {@link Firestore}. This is accomplished by providing a function which
   * is provided a lifecycle managed instance of {@link Firestore} along with the input. When the
   * function is invoked the proto request will be captured and output, at which point the next
   * stage of the pipeline can determine what to do with the request.
   *
   * @see FirestoreIO#sdk()
   * @see #read()
   */
  @Immutable
  public static final class Read {
    private static final Read INSTANCE = new Read();

    private Read() {
    }

    /**
     * Factory method to create a new type safe builder for {@link CollectionReference#listDocuments()}}
     * operations.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<String> collectionIds = ...;
     * PCollection<ListDocumentsResponse> listDocumentResponses = collectionIds
     *     .apply(
     *         FirestoreIO.sdk().read()
     *             .<String>listDocuments((firestore, collectionId) -> firestore.collection(collectionId).listDocuments())
     *             .withDescription("/{collectionId}/*")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().read().listDocuments().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for listing documents
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Read#listDocuments()
     * @see CollectionReference#listDocuments()
     */
    public <In> ListDocuments.Builder<In> listDocuments(ManagedFirestoreSdkIterableFunction<In, DocumentReference> f) {
      return new ListDocuments.Builder<>(f, "ListDocuments");
    }

    /**
     * Factory method to create a new type safe builder for {@link DocumentReference#listCollections()}}
     * operations.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<String> inventoryIds = ...;
     * PCollection<ListCollectionIdsResponse> listDocumentResponses = inventoryIds
     *     .apply(
     *         FirestoreIO.sdk().read()
     *             .<String>listCollections((firestore, inventoryId) -> firestore.collection("inventory").document(inventoryId).listCollections())
     *             .withDescription("/inventory/{inventoryId}/*")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().read().listCollectionIds().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for listing collections
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Read#listCollectionIds()
     * @see DocumentReference#listCollections()
     */
    public <In> ListCollections.Builder<In> listCollections(ManagedFirestoreSdkIterableFunction<In, CollectionReference> f) {
      return new ListCollections.Builder<>(f, "ListCollections");
    }

    /**
     * Factory method to create a new type safe builder for {@link Firestore#getAll(DocumentReference...)}
     * operations.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<List<String>> inventoryIdGroups = ...;
     * PCollection<BatchGetDocumentsResponse> batchGetDocumentsResponses = inventoryIdGroups
     *     .apply(
     *         FirestoreIO.sdk().read()
     *             .<List<String>>getAll((firestore, inventoryIds) -> {
     *               DocumentReference[] documentRefs = inventoryIds.stream()
     *                   .map(docId -> firestore.collection("inventory").document(docId))
     *                   .toArray(DocumentReference[]::new);
     *               return firestore.getAll(documentRefs);
     *             })
     *             .withDescription("/inventory/{inventoryId}")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().read().batchGetDocuments().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for performing a getAll operation
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Read#batchGetDocuments()
     * @see Firestore#getAll(DocumentReference...)
     * @see Firestore#getAll(DocumentReference[], FieldMask)
     */
    public <In> BatchGetDocuments.Builder<In> getAll(ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>> f) {
      return new BatchGetDocuments.Builder<>(f, "BatchGetDocuments");
    }

    /**
     * Factory method to create a new type safe builder for {@link com.google.cloud.firestore.Query
     * Query} operations like those that can be performed on a {@link CollectionReference}.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<String> collectionIds = ...;
     * PCollection<RunQueryResponse> runQueryResponses = collectionIds
     *     .apply(
     *         FirestoreIO.sdk().read()
     *             .<String>query((firestore, collectionId) -> firestore.collection(collectionId).whereEqualTo("foo", "bar").get())
     *             .withDescription("select * from ? where foo = 'bar'")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().read().runQuery().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for performing a query operation
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Read#runQuery()
     * @see com.google.cloud.firestore.Query
     * @see CollectionReference
     */
    public <In> QueryDocuments.Builder<In> query(ManagedFirestoreSdkFutureFunction<In, QuerySnapshot> f) {
      return new QueryDocuments.Builder<>(f, "Query");
    }

    /**
     * Factory method to create a new type safe builder for {@link com.google.cloud.firestore.QueryPartition
     * QueryPartition} operations like those that can be performed on a {@link
     * com.google.cloud.firestore.CollectionGroup CollectionGroup}.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre><code>
     * PCollection<String> collectionGroups = null;
     * PCollection<PartitionQueryResponse> partitionQueryResponses = collectionGroups
     *     .apply(
     *         FirestoreIO.sdk().read()
     *             .<String>collectionGroupPartitionQuery((firestore, collectionGroup) -> firestore.collectionGroup(collectionGroup).getPartitions(10))
     *             .withDescription("&#47;&#42;&#42;/{collectionId} | 10 partitions")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().read().partitionQuery().build());
     * </code></pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for performing a query operation
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Read#partitionQuery()
     * @see com.google.cloud.firestore.CollectionGroup#getPartitions(long)
     */
    // The HTML escape sequence on the withDescription line is escaped because it is '/**' which
    // looks like a javadoc comment to javadoc parsers.
    public <In> CollectionGroupPartitionQuery.Builder<In> collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>> f) {
      return new CollectionGroupPartitionQuery.Builder<>(f, "CollectionGroupPartitionQuery");
    }
  }

  /**
   * Type safe builder factory for write operations.
   * <p/>
   * This class is part of the Firestore Connector DSL and should be accessed via {@link #write()
   * FirestoreIO.sdk().write()}.
   * <p/>
   * This class provides the ability to create {@link PTransform PTransforms} for single write and
   * batch write operations available via {@link Firestore}. This is accomplished by providing a
   * function which is provided a lifecycle managed instance of {@link Firestore} along with the
   * input. When the function is invoked the proto request will be captured and emitted, at which
   * point the next stage of the pipeline can determine what to do with the request.
   *
   * @see FirestoreIO#sdk()
   * @see #write()
   */
  @Immutable
  public static final class Write {
    private static final Write INSTANCE = new Write();

    private Write() {
    }

    /**
     * Factory method to create a new type safe builder for {@link com.google.cloud.firestore.WriteBatch
     * WriteBatch} operations like those that can be performed via {@link Firestore#batch()}.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<List<String>> inventoryIdGroups = ...;
     * PDone done = inventoryIdGroups
     *     .apply(
     *         FirestoreIO.sdk().write()
     *             .<List<String>>batch((firestore, inventoryIdGroup) -> {
     *               WriteBatch writeBatch = firestore.batch();
     *               inventoryIdGroup.forEach(inventoryId ->
     *                   writeBatch.set(
     *                       firestore.collection("inventory").document(inventoryId),
     *                       ImmutableMap.of("foo", "bar")
     *                   )
     *               );
     *               return writeBatch.commit();
     *             })
     *             .withDescription("update inventory set foo = 'bar' where id in ?")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().write().batchWrite().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for performing a query operation
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Write#batchWrite()
     * @see Firestore#batch()
     * @see com.google.cloud.firestore.WriteBatch
     * @see com.google.cloud.firestore.WriteBatch#commit()
     */
    public <In> BatchWrite.Builder<In> batch(ManagedFirestoreSdkFutureFunction<In, List<WriteResult>> f) {
      return new BatchWrite.Builder<>(f, "BatchWrite");
    }

    public <In> BulkWrite.Builder<In> bulkWriter(ManagedFirestoreSdkFutureFunction<In, Void> f) {
      return new BulkWrite.Builder<>(f, "BatchWrite");
    }

    /**
     * Factory method to create a new type safe builder for operations which perform a singular
     * write like those that can be performed via {@link DocumentReference#set(Object)}, {@link
     * DocumentReference#update(Map)} or {@link DocumentReference#delete()}.
     * <p/>
     * This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#sdk()}.
     * <p/>
     *
     * <b>Example Usage</b>
     * <pre>{@code
     * PCollection<String> inventoryIds = ...;
     * PDone done = inventoryIds
     *     .apply(
     *         FirestoreIO.sdk().write()
     *             .<String>single((firestore, inventoryId) -> firestore.collection("inventory").document(inventoryId).delete())
     *             .withDescription("delete from inventory where id = ?")
     *             .build()
     *     )
     *     .apply(FirestoreIO.v1().write().batchWrite().build());
     * }</pre>
     *
     * @param f The function which will be invoked with the managed instance of {@link Firestore}
     * and an {@code In} for each element of the pipeline.
     * @return A new type safe builder for performing a query operation
     * @see FirestoreIO#sdk()
     * @see FirestoreV1.Write#batchWrite()
     * @see DocumentReference#set(Object)
     * @see DocumentReference#update(Map)
     * @see DocumentReference#delete()
     */
    public <In> SingleWrite.Builder<In> single(ManagedFirestoreSdkFutureFunction<In, WriteResult> f) {
      return new SingleWrite.Builder<>(f, "SingleWrite");
    }
  }

  /**
   * A "sealed" interface providing a root type to each of the available managed function interfaces
   * available for use.
   *
   * @param <In> Type of value passed to the defined function along with the {@link Firestore}
   *             instance.
   * @param <Out> Type of value expected to be returned by the defined function when invoked.
   * @see ManagedFirestoreSdkFutureFunction
   * @see ManagedFirestoreSdkIterableFunction
   */
  @FunctionalInterface
  interface ManagedFirestoreSdkFunction<In, Out>
      extends SerializableBiFunction<Firestore, In, Out> {}

  /**
   * This function allows for a serialization and lifecycle friendly capture of an operation
   * performed with the {@link Firestore} SDK which results in an {@link ApiFuture}.
   * <p/>
   * An instance of {@link Firestore} is not required to be {@link java.io.Serializable
   * Serializable} and is therefor not capable for being passed as part of a pipeline. This function
   * will be invoked against a lifecycle managed instance of Firestore and the result of the
   * invocation will be emitted to the next state of the pipeline.
   * <p/>
   * <b>IMPORTANT</b> An instance of this function MUST not close over anything which is not {@link
   * java.io.Serializable Serializable} otherwise Beam will not be able to serialize and pass it in
   * the pipeline.
   *
   * @param <In> Type of value passed to the defined function along with the {@link Firestore}
   * instance.
   * @param <Result> Type of value expected to be returned by the {@link ApiFuture} returned when
   * invoked.
   * @see Read#collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction)
   * @see Read#getAll(ManagedFirestoreSdkFutureFunction)
   * @see Read#query(ManagedFirestoreSdkFutureFunction)
   * @see Write#batch(ManagedFirestoreSdkFutureFunction)
   * @see Write#single(ManagedFirestoreSdkFutureFunction)
   */
  @FunctionalInterface
  public interface ManagedFirestoreSdkFutureFunction<In, Result>
      extends ManagedFirestoreSdkFunction<In, ApiFuture<Result>> {}

  /**
   * This function allows for a serialization and lifecycle friendly capture of an operation
   * performed with the {@link Firestore} SDK which results in an {@link Iterable}.
   * <p/>
   * An instance of {@link Firestore} is not required to be {@link java.io.Serializable
   * Serializable} and is therefor not capable for being passed as part of a pipeline. This function
   * will be invoked against a lifecycle managed instance of Firestore and the result of the
   * invocation will be emitted to the next state of the pipeline.
   * <p/>
   * <b>IMPORTANT</b> An instance of this function MUST not close over anything which is not {@link
   * java.io.Serializable Serializable} otherwise Beam will not be able to serialize and pass it in
   * the pipeline.
   *
   * @param <In> Type of value passed to the defined function along with the {@link Firestore}
   * instance.
   * @param <Result> Type of value expected to be returned by the {@link Iterable} returned when
   * invoked.
   * @see Read#listCollections(ManagedFirestoreSdkIterableFunction)
   * @see Read#listDocuments(ManagedFirestoreSdkIterableFunction)
   */
  @FunctionalInterface
  public interface ManagedFirestoreSdkIterableFunction<In, Result>
      extends ManagedFirestoreSdkFunction<In, Iterable<Result>> {}

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link ListDocumentsRequest}{@code
   * >} which can extract the proto request from a call to {@link CollectionReference#listDocuments()}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read() read()}{@code .}{@link
   * Read#listDocuments(ManagedFirestoreSdkIterableFunction) listDocuments()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#read()
   * @see FirestoreSdk.Read#listDocuments(ManagedFirestoreSdkIterableFunction)
   * @see FirestoreSdk.ListDocuments.Builder
   * @see FirestoreV1.Read#listDocuments()
   * @see CollectionReference#listDocuments()
   */
  public static final class ListDocuments<In>
      extends Transform<
      In,
      Iterable<DocumentReference>,
      ListDocumentsRequest,
      ListDocumentsRequestHolder,
      ListDocumentsRequest,
      ManagedFirestoreSdkIterableFunction<In, DocumentReference>,
      ListDocuments<In>,
      ListDocuments.Builder<In>
      > {

    private ListDocuments(ManagedFirestoreSdkIterableFunction<In, DocumentReference> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, ListDocumentsRequestHolder.class, UNWRAPPER_LIST_DOCUMENTS);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link CollectionReference#listDocuments()}} operations.
     * <p/>
     * This class is part of the Firestore Connector DSL, it is a type safe builder for {@link
     * ListDocuments} accessible via {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read()
     * read()}{@code .}{@link Read#listDocuments(ManagedFirestoreSdkIterableFunction)
     * listDocuments()}.
     * <p/>
     *
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#read()
     * @see FirestoreSdk.ListDocuments
     * @see FirestoreV1.Read#listDocuments()
     * @see CollectionReference#listDocuments()
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        Iterable<DocumentReference>,
        ListDocumentsRequest,
        ListDocumentsRequestHolder,
        ListDocumentsRequest,
        ManagedFirestoreSdkIterableFunction<In, DocumentReference>,
        ListDocuments<In>,
        ListDocuments.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkIterableFunction<In, DocumentReference> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkIterableFunction<In, DocumentReference> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public ListDocuments<In> build() {
        return genericBuild();
      }

      @Override
      protected ListDocuments<In> buildSafe(ManagedFirestoreSdkIterableFunction<In, DocumentReference> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new ListDocuments<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link
   * ListCollectionIdsRequest}{@code >} which can extract the proto request from a call to {@link
   * DocumentReference#listCollections()}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read() read()}{@code .}{@link
   * Read#listCollections(ManagedFirestoreSdkIterableFunction) listCollections()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#read()
   * @see FirestoreSdk.Read#listCollections(ManagedFirestoreSdkIterableFunction)
   * @see FirestoreSdk.ListCollections.Builder
   * @see FirestoreV1.Read#listCollectionIds()
   * @see DocumentReference#listCollections()
   */
  public static final class ListCollections<In>
      extends Transform<
      In,
      Iterable<CollectionReference>,
      ListCollectionIdsRequest,
      ListCollectionIdsRequestHolder,
      ListCollectionIdsRequest,
      ManagedFirestoreSdkIterableFunction<In, CollectionReference>,
      ListCollections<In>,
      ListCollections.Builder<In>
      > {

    private ListCollections(ManagedFirestoreSdkIterableFunction<In, CollectionReference> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, ListCollectionIdsRequestHolder.class, UNWRAPPER_LIST_COLLECTIONS);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link DocumentReference#listCollections()}}
     * operations.
     * <p/>
     * This class is part of the Firestore Connector DSL, it is a type safe builder for {@link
     * ListCollections} accessible via {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read()
     * read()}{@code .}{@link Read#listCollections(ManagedFirestoreSdkIterableFunction)
     * listCollections()}.
     * <p/>
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#read()
     * @see FirestoreV1.Read#listCollectionIds()
     * @see FirestoreSdk.ListCollections
     * @see DocumentReference#listCollections()
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        Iterable<CollectionReference>,
        ListCollectionIdsRequest,
        ListCollectionIdsRequestHolder,
        ListCollectionIdsRequest,
        ManagedFirestoreSdkIterableFunction<In, CollectionReference>,
        ListCollections<In>,
        ListCollections.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkIterableFunction<In, CollectionReference> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkIterableFunction<In, CollectionReference> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public ListCollections<In> build() {
        return genericBuild();
      }

      @Override
      protected ListCollections<In> buildSafe(ManagedFirestoreSdkIterableFunction<In, CollectionReference> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new ListCollections<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link
   * BatchGetDocumentsRequest}{@code >} which can extract the proto request from a call to {@link
   * Firestore#getAll(DocumentReference[])}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read() read()}{@code .}{@link
   * Read#getAll(ManagedFirestoreSdkFutureFunction) getAll()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#read()
   * @see FirestoreSdk.Read#getAll(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.BatchGetDocuments.Builder
   * @see FirestoreV1.Read#batchGetDocuments()
   * @see Firestore#getAll(DocumentReference...)
   * @see Firestore#getAll(DocumentReference[], FieldMask)
   */
  public static final class BatchGetDocuments<In>
      extends Transform<
      In,
      ApiFuture<List<DocumentSnapshot>>,
      BatchGetDocumentsRequest,
      BatchGetDocumentsRequestHolder,
      BatchGetDocumentsRequest,
      ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>>,
      BatchGetDocuments<In>,
      BatchGetDocuments.Builder<In>
      > {

    private BatchGetDocuments(ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, BatchGetDocumentsRequestHolder.class, UNWRAPPER_BATCH_GET);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link Firestore#getAll(DocumentReference...)}
     * operations.
     * <p/>
     * This class is part of the Firestore Connector DSL, it is a type safe builder for {@link
     * BatchGetDocuments} accessible via {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read()
     * read()}{@code .}{@link Read#getAll(ManagedFirestoreSdkFutureFunction)
     * getAll()}.
     * <p/>
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#read()
     * @see FirestoreSdk.Read#getAll(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.BatchGetDocuments
     * @see FirestoreV1.Read#batchGetDocuments()
     * @see Firestore#getAll(DocumentReference...)
     * @see Firestore#getAll(DocumentReference[], FieldMask)
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<List<DocumentSnapshot>>,
        BatchGetDocumentsRequest,
        BatchGetDocumentsRequestHolder,
        BatchGetDocumentsRequest,
        ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>>,
        BatchGetDocuments<In>,
        BatchGetDocuments.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public BatchGetDocuments<In> build() {
        return genericBuild();
      }

      @Override
      protected BatchGetDocuments<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, List<DocumentSnapshot>> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new BatchGetDocuments<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link RunQueryRequest}{@code >}
   * which can extract the proto request from {@link com.google.cloud.firestore.Query Query}
   * operations like those that can be performed on a {@link CollectionReference}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read() read()}{@code .}{@link
   * Read#query(ManagedFirestoreSdkFutureFunction) query()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#read()
   * @see FirestoreSdk.Read#query(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.QueryDocuments.Builder
   * @see FirestoreV1.Read#runQuery()
   * @see com.google.cloud.firestore.Query
   * @see CollectionReference
   */
  public static final class QueryDocuments<In>
      extends Transform<
      In,
      ApiFuture<QuerySnapshot>,
      RunQueryRequest,
      QueryRequestHolder,
      RunQueryRequest,
      ManagedFirestoreSdkFutureFunction<In, QuerySnapshot>,
      QueryDocuments<In>,
      QueryDocuments.Builder<In>
      > {

    private QueryDocuments(ManagedFirestoreSdkFutureFunction<In, QuerySnapshot> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, QueryRequestHolder.class, UNWRAPPER_QUERY);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link com.google.cloud.firestore.Query
     * Query} operations like those that can be performed on a {@link CollectionReference}.
     * <p/>
     * This class is part of the Firestore Connector DSL, it is a type safe builder for {@link
     * QueryDocuments} accessible via {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read()
     * read()}{@code .}{@link Read#query(ManagedFirestoreSdkFutureFunction)
     * query()}.
     * <p/>
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#read()
     * @see FirestoreSdk.Read#query(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.QueryDocuments
     * @see FirestoreV1.Read#runQuery()
     * @see com.google.cloud.firestore.Query
     * @see CollectionReference
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<QuerySnapshot>,
        RunQueryRequest,
        QueryRequestHolder,
        RunQueryRequest,
        ManagedFirestoreSdkFutureFunction<In, QuerySnapshot>,
        QueryDocuments<In>,
        QueryDocuments.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, QuerySnapshot> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, QuerySnapshot> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public QueryDocuments<In> build() {
        return genericBuild();
      }

      @Override
      protected QueryDocuments<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, QuerySnapshot> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new QueryDocuments<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link
   * PartitionQueryRequest}{@code >} which can extract the proto request from  {@link
   * com.google.cloud.firestore.QueryPartition QueryPartition} operations like those that can be
   * performed on a {@link com.google.cloud.firestore.CollectionGroup CollectionGroup}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#read() read()}{@code .}{@link
   * Read#collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction)
   * collectionGroupPartitionQuery()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#read()
   * @see FirestoreSdk.Read#collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.CollectionGroupPartitionQuery.Builder
   * @see FirestoreV1.Read#partitionQuery()
   * @see com.google.cloud.firestore.CollectionGroup#getPartitions(long)
   */
  public static final class CollectionGroupPartitionQuery<In>
      extends Transform<
      In,
      ApiFuture<List<QueryPartition>>,
      PartitionQueryRequest,
      PartitionQueryRequestHolder,
      PartitionQueryRequest,
      ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>>,
      CollectionGroupPartitionQuery<In>,
      CollectionGroupPartitionQuery.Builder<In>
      > {

    private CollectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, PartitionQueryRequestHolder.class, UNWRAPPER_PARTITION_QUERY);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link com.google.cloud.firestore.QueryPartition QueryPartition}
     * operations like those that can be performed on a {@link com.google.cloud.firestore.CollectionGroup
     * CollectionGroup}.
     * <p/>
     * This class is part of the Firestore Connector DSL, it is a type safe builder for {@link
     * CollectionGroupPartitionQuery} accessible via {@link FirestoreIO#sdk()}{@code .}{@link
     * FirestoreSdk#read() read()}{@code .}{@link Read#collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction)
     * collectionGroupPartitionQuery()}.
     * <p/>
     *
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#read()
     * @see FirestoreSdk.Read#collectionGroupPartitionQuery(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.CollectionGroupPartitionQuery
     * @see FirestoreV1.Read#partitionQuery()
     * @see com.google.cloud.firestore.CollectionGroup#getPartitions(long)
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<List<QueryPartition>>,
        PartitionQueryRequest,
        PartitionQueryRequestHolder,
        PartitionQueryRequest,
        ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>>,
        CollectionGroupPartitionQuery<In>,
        CollectionGroupPartitionQuery.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public CollectionGroupPartitionQuery<In> build() {
        return genericBuild();
      }

      @Override
      protected CollectionGroupPartitionQuery<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, List<QueryPartition>> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new CollectionGroupPartitionQuery<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link CommitRequest}{@code >}
   * which can extract the proto request from {@link com.google.cloud.firestore.WriteBatch
   * WriteBatch} operations like those that can be performed via {@link Firestore#batch()}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
   * Write#batch(ManagedFirestoreSdkFutureFunction) batch()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#write()
   * @see FirestoreSdk.Write#batch(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.BatchWrite.Builder
   * @see FirestoreV1.Write#batchWrite()
   * @see Firestore#batch()
   * @see com.google.cloud.firestore.WriteBatch
   * @see com.google.cloud.firestore.WriteBatch#commit()
   */
  public static final class BatchWrite<In>
      extends Transform<
      In,
      ApiFuture<List<WriteResult>>,
      CommitRequest,
      CommitRequestHolder,
      com.google.firestore.v1.Write,
      ManagedFirestoreSdkFutureFunction<In, List<WriteResult>>,
      BatchWrite<In>,
      BatchWrite.Builder<In>
      > {

    private BatchWrite(ManagedFirestoreSdkFutureFunction<In, List<WriteResult>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, CommitRequestHolder.class, UNWRAPPER_COMMIT);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link com.google.cloud.firestore.WriteBatch WriteBatch} operations
     * like those that can be performed via {@link Firestore#batch()}.
     * <p/>
     * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
     * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
     * Write#batch(ManagedFirestoreSdkFutureFunction) batch()}.
     * <p/>
     *
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#write()
     * @see FirestoreSdk.Write#batch(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.BatchWrite
     * @see FirestoreV1.Write#batchWrite()
     * @see Firestore#batch()
     * @see com.google.cloud.firestore.WriteBatch
     * @see com.google.cloud.firestore.WriteBatch#commit()
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<List<WriteResult>>,
        CommitRequest,
        CommitRequestHolder,
        com.google.firestore.v1.Write,
        ManagedFirestoreSdkFutureFunction<In, List<WriteResult>>,
        BatchWrite<In>,
        BatchWrite.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, List<WriteResult>> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, List<WriteResult>> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public BatchWrite<In> build() {
        return genericBuild();
      }

      @Override
      protected BatchWrite<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, List<WriteResult>> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new BatchWrite<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link CommitRequest}{@code >}
   * which can extract the proto request from {@link com.google.cloud.firestore.BulkWriter
   * BulkWriter} operations like those that can be performed via {@link Firestore#bulkWriter()}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
   * Write#bulkWriter(ManagedFirestoreSdkFutureFunction) bulk()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#write()
   * @see FirestoreSdk.Write#bulkWriter(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.BulkWrite.Builder
   * @see FirestoreV1.Write#batchWrite() 
   * @see Firestore#bulkWriter()
   * @see com.google.cloud.firestore.BulkWriter
   * @see BulkWriter#flush() 
   */
  public static final class BulkWrite<In>
      extends Transform<
      In,
      ApiFuture<Void>,
      BatchWriteRequest,
      BatchWriteRequestHolder,
      com.google.firestore.v1.Write,
      ManagedFirestoreSdkFutureFunction<In, Void>,
      BulkWrite<In>,
      BulkWrite.Builder<In>
      > {

    private BulkWrite(ManagedFirestoreSdkFutureFunction<In, Void> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, BatchWriteRequestHolder.class, UNWRAPPER_BATCH_WRITE);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for {@link com.google.cloud.firestore.BulkWriter BulkWriter} operations
     * like those that can be performed via {@link Firestore#bulkWriter()}.
     * <p/>
     * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
     * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
     * Write#bulkWriter(ManagedFirestoreSdkFutureFunction) bulk()}.
     * <p/>
     *
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#write()
     * @see FirestoreSdk.Write#bulkWriter(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.BulkWrite
     * @see FirestoreV1.Write#batchWrite()
     * @see Firestore#bulkWriter()
     * @see com.google.cloud.firestore.BulkWriter
     * @see com.google.cloud.firestore.BulkWriter#flush()
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<Void>,
        BatchWriteRequest,
        BatchWriteRequestHolder,
        com.google.firestore.v1.Write,
        ManagedFirestoreSdkFutureFunction<In, Void>,
        BulkWrite<In>,
        BulkWrite.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, Void> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, Void> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public BulkWrite<In> build() {
        return genericBuild();
      }

      @Override
      protected BulkWrite<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, Void> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new BulkWrite<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <In, }{@link CommitRequest}{@code >}
   * which can extract the proto request from operations which perform a singular write like those
   * that can be performed via {@link DocumentReference#set(Object)}, {@link
   * DocumentReference#update(Map)} or {@link DocumentReference#delete()}.
   * <p/>
   * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
   * Write#batch(ManagedFirestoreSdkFutureFunction) batch()}.
   * <p/>
   *
   * @see FirestoreIO#sdk()
   * @see FirestoreSdk#write()
   * @see FirestoreSdk.Write#single(ManagedFirestoreSdkFutureFunction)
   * @see FirestoreSdk.SingleWrite.Builder
   * @see FirestoreV1.Write#batchWrite()
   * @see DocumentReference#set(Object)
   * @see DocumentReference#update(Map)
   * @see DocumentReference#delete()
   */
  public static final class SingleWrite<In>
      extends Transform<
      In,
      ApiFuture<WriteResult>,
      CommitRequest,
      CommitRequestHolder,
      com.google.firestore.v1.Write,
      ManagedFirestoreSdkFutureFunction<In, WriteResult>,
      SingleWrite<In>,
      SingleWrite.Builder<In>
      > {

    private SingleWrite(ManagedFirestoreSdkFutureFunction<In, WriteResult> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
      super(function, description, firestoreStatefulComponentFactory, CommitRequestHolder.class, UNWRAPPER_COMMIT);
    }

    @Override
    public Builder<In> toBuilder() {
      return new Builder<>(f, description, firestoreStatefulComponentFactory);
    }

    /**
     * A type safe builder for operations which perform a singular write like those that can be
     * performed via {@link DocumentReference#set(Object)}, {@link DocumentReference#update(Map)} or
     * {@link DocumentReference#delete()}.
     * <p/>
     * This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
     * {@link FirestoreIO#sdk()}{@code .}{@link FirestoreSdk#write() write()}{@code .}{@link
     * Write#single(ManagedFirestoreSdkFutureFunction) single()}.
     * <p/>
     *
     * @see FirestoreIO#sdk()
     * @see FirestoreSdk#write()
     * @see FirestoreSdk.Write#single(ManagedFirestoreSdkFutureFunction)
     * @see FirestoreSdk.SingleWrite
     * @see FirestoreV1.Write#batchWrite()
     * @see Firestore#batch()
     * @see com.google.cloud.firestore.WriteBatch
     * @see com.google.cloud.firestore.WriteBatch#commit()
     */
    public static final class Builder<In> extends Transform.Builder<
        In,
        ApiFuture<WriteResult>,
        CommitRequest,
        CommitRequestHolder,
        com.google.firestore.v1.Write,
        ManagedFirestoreSdkFutureFunction<In, WriteResult>,
        SingleWrite<In>,
        SingleWrite.Builder<In>
        > {

      private Builder(
          ManagedFirestoreSdkFutureFunction<In, WriteResult> function,
          @Nullable String description) {
        super(function, description);
      }

      private Builder(ManagedFirestoreSdkFutureFunction<In, WriteResult> function, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        super(function, description, firestoreStatefulComponentFactory);
      }

      @Override
      public SingleWrite<In> build() {
        return genericBuild();
      }

      @Override
      protected SingleWrite<In> buildSafe(ManagedFirestoreSdkFutureFunction<In, WriteResult> f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        return new SingleWrite<>(f, description, firestoreStatefulComponentFactory);
      }
    }
  }

  /**
   * Our base PTransform class for Firestore Sdk related functions.
   * <p/>
   * This class has several type parameters which bind together the various parts in play which are
   * necessary for extracting a request out from a {@link Firestore} operation.
   *
   * @param <In> The type of the previous stage of the pipeline, can be anything allowable by Beam
   * @param <Result> The type returned from the {@link Firestore} operation (a class from {@link
   * com.google.cloud.firestore}
   * @param <ProtoRequest> The type of the {@link com.google.cloud.firestore.v1} proto which is to
   * be sent as an RPC
   * @param <Holder> The intermediary Holder type which must correlate to the ProtoRequest, allowing
   * for a common superclass for all requests
   * @param <F> The type of the {@link ManagedFirestoreSdkFunction} which represents the function
   * which will be invoked for each element of the pipeline and whose request will be captured
   * @param <Self> The type of this transform used to bind this type and the corresponding type safe
   * {@link Bldr} together
   * @param <Bldr> The type of the type safe builder which is used to build and instance of {@link
   * Self}
   */
  private abstract static class Transform<
      In,
      Result,
      ProtoRequest extends Message,
      Holder extends FirestoreRequestHolder<ProtoRequest>,
      Out,
      F extends ManagedFirestoreSdkFunction<In, Result>,
      Self extends Transform<In, Result, ProtoRequest, Holder, Out, F, Self, Bldr>,
      Bldr extends Transform.Builder<In, Result, ProtoRequest, Holder, Out, F, Self, Bldr>
      >
      extends PTransform<PCollection<In>, PCollection<Out>>
      implements HasDisplayData {
    protected final F f;
    protected final @Nullable String description;
    protected final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    protected final Class<Holder> holderClass;
    protected final PTransform<PCollection<Holder>, PCollection<Out>> unwrapper;

    protected Transform(
        F f,
        @Nullable String description,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        Class<Holder> holderClass,
        PTransform<PCollection<Holder>, PCollection<Out>> unwrapper) {
      this.f = f;
      this.description = description;
      this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      this.holderClass = holderClass;
      this.unwrapper = unwrapper;
    }

    /**
     * Create a new {@link Bldr Builder} from the current instance.
     *
     * @return a new instance of a {@link Bldr Builder} initialized to the current state of this
     * instance
     */
    public abstract Bldr toBuilder();

    @Override
    public final PCollection<Out> expand(PCollection<In> input) {
      return input.apply(ParDo.of(getFn()))
          .apply(unwrapper);
    }

    @VisibleForTesting
    FirestoreSdkFn<In, Result, ProtoRequest, F, Holder> getFn() {
      return new FirestoreSdkFn<>(
          firestoreStatefulComponentFactory,
          description,
          f,
          holderClass
      );
    }

    @Override
    public void populateDisplayData(@NonNull DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(DisplayData.item("description", description).withLabel("Description"));
    }

    /**
     * Our base type safe builder for a {@link Transform}
     * <p/>
     * This class has several type parameters which bind together the various parts in play which
     * are necessary for extracting a request out from a {@link Firestore} operation.
     * <p/>
     * In addition to binding the various types together, this type safe builder provides an user
     * (and semver) friendly way to expose optional parameters to those users that wish to configure
     * them. Additionally, we are able to add and deprecate individual parameters as may be needed.
     *
     * @param <In> The type of the previous stage of the pipeline, can be anything allowable by
     * Beam
     * @param <Result> The type returned from the {@link Firestore} operation (a class from {@link
     * com.google.cloud.firestore})
     * @param <ProtoRequest> The type of the {@link com.google.cloud.firestore.v1} proto which is to
     * be sent as an RPC
     * @param <Holder> The intermediary Holder type which must correlate to the ProtoRequest,
     * allowing for a common superclass for all requests
     * @param <F> The type of the {@link ManagedFirestoreSdkFunction} which represents the function
     * which will be invoked for each element of the pipeline and whose request will be captured
     * @param <Trfm> The type of this transform used to bind this type and the corresponding type
     * safe {@link Bldr} together
     * @param <Bldr> The type of the type safe builder which is used to build and instance of {@link
     * Trfm}
     */
    private static abstract class Builder<
        In,
        Result,
        ProtoRequest extends Message,
        Holder extends FirestoreRequestHolder<ProtoRequest>,
        Out,
        F extends ManagedFirestoreSdkFunction<In, Result>,
        Trfm extends Transform<In, Result, ProtoRequest, Holder, Out, F, Trfm, Bldr>,
        Bldr extends Builder<In, Result, ProtoRequest, Holder, Out, F, Trfm, Bldr>
        > {
      protected F f;
      protected @Nullable String description;
      protected FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;

      protected Builder(F f, @Nullable String description) {
        this(f, description, FirestoreStatefulComponentFactory.INSTANCE);
      }

      protected Builder(F f, @Nullable String description, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory) {
        this.f = f;
        this.description = description;
        this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      }

      /**
       * Convenience method to take care of hiding the unchecked cast warning from the compiler.
       * This cast is safe because we are always an instance of {@link Bldr} as the only way to
       * get an instance of {@link Builder} is for it to conform to {@code Bldr}'s constraints.
       * @return Down cast this
       */
      @SuppressWarnings({"unchecked", "RedundantSuppression"})
      private Bldr self() {
        return (Bldr) this;
      }

      /**
       * Create a new instance of {@link Trfm Transform} from the current builder state.
       * @return a new instance of {@link Trfm Transform} from the current builder state.
       */
      public abstract Trfm build();

      /**
       * Provide a central location for the validation before ultimately constructing a
       * transformer.
       *
       * While this method looks to purely be duplication (given that each implementation of {@link
       * #build()} simply delegates to this method, the build method carries with it the concrete
       * class rather than the generic type information. Having the concrete class available to
       * users is advantageous to reduce the necessity of reading the complex type information and
       * instead presenting them with a concrete class name.
       *
       * Comparing the type of the builder at the use site of each method:
       * <table>
       *   <tr>
       *     <th>{@code build()}</th>
       *     <th>{@code genericBuild()}</th>
       *   </tr>
       *   <tr>
       *     <td><pre>{@code FirestoreSdk.BatchGetDocuments.Builder<In>}</pre></td>
       *     <td><pre>{@code FirestoreSdk.Transform.Builder<
       *              In,
       *              Result,
       *              ProtoRequest extends Message,
       *              Out extends FirestoreRequest.FirestoreRequestData<ProtoRequest>,
       *              F extends FirestoreSdk.ManagedFirestoreSdkFunction<In, Result>,
       *              Trfm extends FirestoreSdk.Transform<In, Result, ProtoRequest, Out, F, Trfm, Bldr>,
       *              Bldr extends FirestoreSdk.Transform.Builder<In, Result, ProtoRequest, Out, F, Trfm, Bldr>
       *          >}</pre></td>
       *   </tr>
       * </table>
       *
       * While this type information is important for our implementation, it is less important for
       * the users using our implementation.
       */
      protected final Trfm genericBuild() {
        return buildSafe(
            requireNonNull(f, "f must be non null"),
            description,
            requireNonNull(firestoreStatefulComponentFactory, "firestoreFactory must be non null")
        );
      }

      protected abstract Trfm buildSafe(
          F f,
          @Nullable String description,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory
      );

      /**
       * Specify a description for this Transform which will be included in the display data
       * of a pipeline.
       * <p/>
       * <i>NOTE</i> This method behaves as set, NOT copy with new value.
       * @param description The description to include in the display data.
       * @return This builder with the description value set.
       */
      public final Bldr withDescription(@Nullable String description) {
        this.description = description;
        return self();
      }
    }
  }

  /**
   * This class exists as a workaround to https://issues.apache.org/jira/browse/BEAM-7938 which
   * prevents us from being able to solely rely on a generic unwrap method defined on {@link
   * FirestoreRequestHolder}. So here we explicitly hold on to the classes which we then use in the
   * process of surfacing input and output type descriptors.
   */
  static final class Unwrapper<
      Data extends FirestoreRequestHolder<Request>,
      Request extends Message
      > extends PTransform<PCollection<Data>, PCollection<Request>> {
    private final Class<Data> dataClass;
    private final Class<Request> requestClass;

    public Unwrapper(Class<Data> dataClass, Class<Request> requestClass) {
      this.dataClass = dataClass;
      this.requestClass = requestClass;
    }

    @Override
    public PCollection<Request> expand(PCollection<Data> input) {
      return input.apply(MapElements.via(new Mapper()));
    }

    private final class Mapper extends SimpleFunction<Data, Request> {
      @Override
      public Request apply(Data input) {
        return input.getRequest();
      }

      @Override
      public TypeDescriptor<Data> getInputTypeDescriptor() {
        return TypeDescriptor.of(dataClass);
      }

      @Override
      public TypeDescriptor<Request> getOutputTypeDescriptor() {
        return TypeDescriptor.of(requestClass);
      }
    }
  }

  static final class CommitUnwrapper extends PTransform<PCollection<CommitRequestHolder>, PCollection<com.google.firestore.v1.Write>> {
    @Override
    public PCollection<com.google.firestore.v1.Write> expand(PCollection<CommitRequestHolder> input) {
      return input.apply(ParDo.of(new DoFn<CommitRequestHolder, com.google.firestore.v1.Write>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          CommitRequestHolder holder = context.element();
          holder.getRequest().getWritesList().forEach(context::output);
        }
      }));
    }
  }
  static final class BatchWriteUnwrapper extends PTransform<PCollection<BatchWriteRequestHolder>, PCollection<com.google.firestore.v1.Write>> {
    @Override
    public PCollection<com.google.firestore.v1.Write> expand(PCollection<BatchWriteRequestHolder> input) {
      return input.apply(ParDo.of(new DoFn<BatchWriteRequestHolder, com.google.firestore.v1.Write>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          BatchWriteRequestHolder holder = context.element();
          holder.getRequest().getWritesList().forEach(context::output);
        }
      }));
    }
  }

}
