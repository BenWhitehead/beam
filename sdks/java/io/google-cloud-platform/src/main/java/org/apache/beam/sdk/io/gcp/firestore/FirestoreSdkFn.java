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
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.api.gax.rpc.BidiStreamingCallable;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.BulkWriterOptions;
import com.google.cloud.firestore.CollectionGroup;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreBundle;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.FirestoreOptions.EmulatorCredentials;
import com.google.cloud.firestore.FirestoreRpcFactory;
import com.google.cloud.firestore.Transaction;
import com.google.cloud.firestore.Transaction.AsyncFunction;
import com.google.cloud.firestore.TransactionOptions;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.BeginTransactionRequest;
import com.google.firestore.v1.BeginTransactionResponse;
import com.google.firestore.v1.CommitRequest;
import com.google.firestore.v1.CommitResponse;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListenRequest;
import com.google.firestore.v1.ListenResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RollbackRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.rpc.Status;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchGetDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchWriteRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.CommitRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.FirestoreRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListCollectionIdsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.PartitionQueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.QueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFunction;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkIterableFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DoFn} which is the implementation that knows how to extract a {@link
 * com.google.cloud.firestore.v1} request proto from an operation performed via the model provided
 * by {@link Firestore} and associated classes.
 * <p/>
 * The complexity of this class exits to deal with the fact that in the {@link Firestore} sdk almost
 * all operations take the from of {@code Data -> AsyncRpcResult}. As such, there is no means of
 * accessing the proto request which is constructed under the hood.
 * <p/>
 * The Firestore SDK currently provides two primary "AsyncRpcResult" patterns we're concerned with
 * supporting.
 * <ol>
 *   <li>Lazy Iterable&lt;ModelType&gt;</li>
 *   <li>Future&lt;ModelType&gt;</li>
 * </ol>
 * (The Firestore SDK also uses callbacks in some situations, but we're not supporting those at this time).
 * <p/>
 * For example, imagine you want to list all the sub-collections for a document. In the Firestore V1 API
 * this operation is modeled via {@link ListCollectionIdsRequest} and {@link com.google.firestore.v1.ListCollectionIdsResponse}.
 * The in the Firestore SDK this is accessed via {@link DocumentReference#listCollections()} which
 * returns an {@link Iterable}{@code <}{@link CollectionReference}{@code >}. Under the hood the iterable
 * constructs the request proto when iteration begins, and then yields results from the response after
 * transforming the response into {@code CollectionReference}s.
 * <p/>
 * On top of this, every instance of a model object from {@link com.google.cloud.firestore} holds onto
 * a reference of the {@link Firestore} instance used to create it. Instances of {@link Firestore}
 * are not serializable (they maintain a reference to the underlying gRPC client used to send requests) and do
 * not have a clear lifecycle boundary to allow for a model to be "reattached" to a {@code Firestore} instance.
 * <p/>
 * To deal with the fact that the API and behavior of {@link Firestore} and associated model is not friendly
 * to serialization this class provides support to "extract" a request from those supported operations
 * we are able to intercept and guarantee stability of. (i.e. Transactions are not supported because
 * we can not guarantee the stability of them due to their multi-leg nature.)
 * <p/>
 * The approach taken by this Fn to extract a request is as follows:
 * <ol>
 *   <li>
 *     Have the user provide a function of type {@link F} which yields an "AsyncRpcResult" when
 *     provided with an instance of {@link Firestore} and {@link In}.
 *   </li>
 *   <li>
 *     Internally construct a custom instance of {@link Firestore} which has a custom implementation
 *     of {@link FirestoreRpc} which is an in process only captor of requests.
 *   </li>
 *   <li>
 *     Any request that is captured by the custom {@link FirestoreRpc} implementation will be wrapped
 *     in an {@link Out} before being emitted to the next stage of the pipeline.
 *   </li>
 * </ol>
 *
 * @param <In> The type of the previous stage of the pipeline, can be anything allowable by Beam
 * @param <Result> The type returned from the {@link Firestore} operation (a class from {@link
 * com.google.cloud.firestore})
 * @param <ProtoRequest> The type of the {@link com.google.cloud.firestore.v1} proto which is to be
 * sent as an RPC
 * @param <F> The type of the {@link ManagedFirestoreSdkFunction} which represents the function
 * which will be invoked for each element of the pipeline and whose request will be captured
 * @param <Out> The intermediary Holder type which must correlate to the ProtoRequest, allowing for a
 * common superclass for all requests
 */
final class FirestoreSdkFn<
    In,
    Result,
    ProtoRequest extends Message,
    F extends ManagedFirestoreSdkFunction<In, Result>,
    Out extends FirestoreRequestHolder<ProtoRequest>
    > extends NonWindowAwareDoFn<In, Out> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FirestoreSdkFn.class);

  /* Immutable Fields which are set at construction time and serialized with the function */
  /**
   * The factory used to construct an instance of {@link Firestore} for {@link #fs}
   */
  private final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
  /**
   * Optional description which can be provided and will be included when {@link
   * #populateDisplayData(DisplayData.Builder)}} is called
   */
  private final @Nullable String description;
  /**
   * User provided function which will be invoked for each element sent to this Fn
   */
  private final F function;
  /**
   * Class instance corresponding to {@link Out} which is used to filter down all possible requests
   * to only those which match.
   */
  private final Class<Out> outClass;

  /* transient state which is managed during the lifecycle of this Fn on the worker*/
  /**
   * Custom implementation of {@link FirestoreRpc} which captures any requests made when {@link #function} is invoked.
   */
  private transient CapturingFirestoreRpc<ProtoRequest, Out> capturingFirestoreRpc;
  /**
   * In process only configured instance of {@link Firestore} which is provided when {@link #function} is invoked.
   */
  private transient Firestore fs;
  /**
   * Custom implementation of {@link Firestore} which filters early which operations are permitted
   * to be invoked in {@link #function}.
   */
  private transient OperationFilteringFirestore operationFilteringFirestore;

  @SuppressWarnings("initialization.fields.uninitialized") // allow transient fields to be managed by component lifecycle
  FirestoreSdkFn(FirestoreStatefulComponentFactory firestoreStatefulComponentFactory, @Nullable String description, F function, Class<Out> outClass) {
    this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
    this.description = description;
    this.function = function;
    this.outClass = outClass;
  }

  /**
   * Prepares state of this Fn that spans more than a single bundle.
   */
  @Override
  public void setup() {
    capturingFirestoreRpc = new CapturingFirestoreRpc<>();
  }

  /**
   * Initialized this Fn to be able to process a bundle.
   * <p/>
   * Creates an instance of {@link Firestore} which will be used to invoke {@link #function}
   * <p/>
   * The provided {@link org.apache.beam.sdk.transforms.DoFn.StartBundleContext StartBundleContext}
   * is expected to provide {@link FirestoreIOOptions} and {@link org.apache.beam.sdk.extensions.gcp.options.GcpOptions
   * GcpOptions} if connecting to live Firestore (i.e. not an emulator).
   *
   * @param c Context containing options for Firestore connection
   */
  @Override
  public void startBundle(StartBundleContext c) {
    fs = firestoreStatefulComponentFactory.getFirestoreOptionsBuilder(c.getPipelineOptions())
        .setServiceRpcFactory((FirestoreRpcFactory) firestoreOptions -> capturingFirestoreRpc)
        .setCredentials(new EmulatorCredentials()) // suppress any potential environment probing for credentials
        .build()
        .getService();

    operationFilteringFirestore = new OperationFilteringFirestore(fs);
  }

  /**
   * Tear down the state of this Fn which was used to process a bundle.
   * <p/>
   * Closes and releases the {@link Firestore} instance created in {@link #startBundle(StartBundleContext)} startBundle}
   */
  @SuppressWarnings("nullness") // allow clearing transient fields
  @Override
  public void finishBundle() {
    operationFilteringFirestore = null;
    if (fs != null) {
      try {
        fs.close();
      } catch (Exception e) {
        // The firestore rpc instance closed by calling fs.close() is provided by us and doesn't
        // throw any exceptions in it's close method
      } finally {
        fs = null;
      }
    }
  }

  /**
   * Closes and releases any state of this Fn that spans more than a single bundle.
   */
  @SuppressWarnings("nullness") // allow clearing transient fields
  @Teardown
  public void teardown() {
    if (capturingFirestoreRpc != null) {
      try {
        capturingFirestoreRpc.close();
      } catch (Exception e) {
        // ignored
      } finally {
        capturingFirestoreRpc = null;
      }
    }
  }

  /**
   * For each non-null {@link ProcessContext#element() context.element()} invoke {@link #function}
   * with the element and our managed instance of {@link Firestore}.
   * <p/>
   * {@link ProcessContext#element() context.element()} must be non-null, otherwise a
   * NullPointerException will be thrown.
   *  @param context Context to source element from, and output any captured request to
   *
   */
  @Override
  public void processElement(ProcessContext context) {
    @SuppressWarnings("nullness") // for some reason requireNonNull thinks its parameter but be non-null...
    In element = requireNonNull(context.element(), "context.element() must be non null");
    capturingFirestoreRpc.reset();

    if (function instanceof ManagedFirestoreSdkFutureFunction<?, ?>) {
      //noinspection unchecked
      ManagedFirestoreSdkFutureFunction<In, Result> ff = (ManagedFirestoreSdkFutureFunction<In, Result>) function;
      ApiFuture<Result> future = ff.apply(operationFilteringFirestore, element);
      try {
        Result ignore = future.get();
      } catch (InterruptedException | ExecutionException ignore) {
        // All of the futures returned by our callables are "immediate" and won't throw these exceptions
      }
    } else if (function instanceof ManagedFirestoreSdkIterableFunction<?, ?>) {
      //noinspection unchecked
      ManagedFirestoreSdkIterableFunction<In, Result> ff = (ManagedFirestoreSdkIterableFunction<In, Result>) function;
      // We're only going to intercept the request for the first page, which is okay. We're only
      // interested in grabbing the request, the function that actually sends the request will take
      // care of pagination concerns.
      Iterable<Result> ignore = ff.apply(operationFilteringFirestore, element);
    } else {
      // Since java 8 lacks the concept of a sealed class/interface hierarchy we have to provide
      // a runtime check and an error thrown if someone happens to get into this state.
      throw new IllegalStateException(
          String.format("Unsupported implementation of %s. Expected %s or %s but received %s",
              ManagedFirestoreSdkFunction.class.getName(),
              ManagedFirestoreSdkFutureFunction.class.getName(),
              ManagedFirestoreSdkIterableFunction.class.getName(),
              function.getClass()
          )
      );
    }

    boolean outputAtLeastOne = capturingFirestoreRpc.output(outClass, context);
    if (!outputAtLeastOne) {
      LOGGER.warn("No output generated for element");
    }
    capturingFirestoreRpc.reset();
  }

  @Override
  public void populateDisplayData(@NonNull DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("description", description).withLabel("Description"));
  }

  /**
   * Custom implementation of {@link FirestoreRpc} which instead of sending a request to Firestore
   * will capture the request and wrap it in a corresponding {@link FirestoreRequestHolder}.
   * <p/>
   * This class provide a Callable for each operation which is supported, and should map roughly
   * 1-to-1 with {@link OperationFilteringFirestore}.
   *
   * @param <ProtoRequest> The type of the {@link com.google.cloud.firestore.v1} proto which is to
   * be sent as an RPC
   * @param <Out> The intermediary Holder type which must correlate to the ProtoRequest, allowing
   * for a common superclass for all requests
   * @see OperationFilteringFirestore
   */
  @VisibleForTesting
  static final class CapturingFirestoreRpc<
      ProtoRequest extends Message,
      Out extends FirestoreRequestHolder<ProtoRequest>
      > implements FirestoreRpc {

    private final CapturingUnaryCallable<
        CommitRequest,
        CommitResponse,
        CommitRequestHolder
        > commitCallable;
    private final CapturingUnaryCallable<
        BatchWriteRequest,
        BatchWriteResponse,
        BatchWriteRequestHolder
        > batchWriteCallable;

    private final CapturingUnaryCallable<
        ListCollectionIdsRequest,
        ListCollectionIdsPagedResponse,
        ListCollectionIdsRequestHolder
        > listCollectionIdsPagedCallable;
    private final CapturingUnaryCallable<
        ListDocumentsRequest,
        ListDocumentsPagedResponse,
        ListDocumentsRequestHolder
        > listDocumentsPagedCallable;
    private final CapturingUnaryCallable<
        PartitionQueryRequest,
        PartitionQueryPagedResponse,
        PartitionQueryRequestHolder
        > partitionQueryPagedCallable;
    private final CapturingStreamingCallable<
        BatchGetDocumentsRequest,
        BatchGetDocumentsResponse,
        BatchGetDocumentsRequestHolder
        > batchGetDocumentsCallable;
    private final CapturingStreamingCallable<
        RunQueryRequest,
        RunQueryResponse,
        QueryRequestHolder
        > runQueryCallable;

    private final ScheduledExecutorService scheduledExecutorService;

    CapturingFirestoreRpc() {
      commitCallable = new CapturingUnaryCallable<>(
          FirestoreRequest::create,
          CommitRequestHolder.class,
          request -> ApiFutures.immediateFuture(CommitResponse.getDefaultInstance())
      );
      batchWriteCallable = new CapturingUnaryCallable<>(
          FirestoreRequest::create,
          BatchWriteRequestHolder.class,
          new BatchWriteFunction()
      );
      
      listCollectionIdsPagedCallable = new CapturingUnaryCallable<>(
          FirestoreRequest::create,
          ListCollectionIdsRequestHolder.class,
          request -> ApiFutures.immediateFuture(null)
      );
      listDocumentsPagedCallable = new CapturingUnaryCallable<>(
          FirestoreRequest::create,
          ListDocumentsRequestHolder.class,
          request -> ApiFutures.immediateFuture(null)
      );
      partitionQueryPagedCallable = new CapturingUnaryCallable<>(
          FirestoreRequest::create,
          PartitionQueryRequestHolder.class,
          request -> ApiFutures.immediateFuture(null)
      );
      batchGetDocumentsCallable = new CapturingStreamingCallable<>(
          FirestoreRequest::create,
          BatchGetDocumentsRequestHolder.class
      );
      runQueryCallable = new CapturingStreamingCallable<>(
          FirestoreRequest::create,
          QueryRequestHolder.class
      );
      scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public UnaryCallable<CommitRequest, CommitResponse> commitCallable() {
      return commitCallable;
    }

    @Override
    public UnaryCallable<BatchWriteRequest, BatchWriteResponse> batchWriteCallable() {
      return batchWriteCallable;
    }

    @Override
    public UnaryCallable<BeginTransactionRequest, BeginTransactionResponse> beginTransactionCallable() {
      throw new IllegalStateException("beginTransactionCallable() not supported");
    }

    @Override
    public UnaryCallable<RollbackRequest, Empty> rollbackCallable() {
      throw new IllegalStateException("rollbackCallable() not supported");
    }

    @Override
    public UnaryCallable<ListCollectionIdsRequest, ListCollectionIdsPagedResponse> listCollectionIdsPagedCallable() {
      return listCollectionIdsPagedCallable;
    }

    @Override
    public UnaryCallable<PartitionQueryRequest, PartitionQueryPagedResponse> partitionQueryPagedCallable() {
      return partitionQueryPagedCallable;
    }

    @Override
    public UnaryCallable<ListDocumentsRequest, ListDocumentsPagedResponse> listDocumentsPagedCallable() {
      return listDocumentsPagedCallable;
    }

    @Override
    public ServerStreamingCallable<BatchGetDocumentsRequest, BatchGetDocumentsResponse> batchGetDocumentsCallable() {
      return batchGetDocumentsCallable;
    }

    @Override
    public ServerStreamingCallable<RunQueryRequest, RunQueryResponse> runQueryCallable() {
      return runQueryCallable;
    }

    @Override
    public BidiStreamingCallable<ListenRequest, ListenResponse> listenCallable() {
      throw new IllegalStateException("listenCallable() not supported");
    }

    @Override
    public ScheduledExecutorService getExecutor() {
      return scheduledExecutorService;
    }

    @Override
    public void close() {
      if (scheduledExecutorService != null) {
        scheduledExecutorService.shutdown();
      }
    }

    /**
     * Clear captured state of all callables
     */
    void reset() {
      newCaptorsStream()
          .forEach(CapturingCallable::reset);
    }

    /**
     * Filter all captured requests which are an instance of {@code clazz}, and output each one to
     * {@code c}.
     *
     * @param clazz Class type to filter captured requests to before outputting
     * @param c Context to output matching captured requests to
     * @return {@code true} if at least one value was output to c, {@code false} otherwise
     */
    boolean output(Class<Out> clazz, DoFn<?, Out>.ProcessContext c) {
      AtomicBoolean atLeastOneOutput = new AtomicBoolean(false);
      newCaptorsStream()
          .filter(captor -> captor.applicableTo(clazz))
          .map(captor -> {
            // The following cast is safe, as it is guarded by the filter above
            //noinspection unchecked
            return (CapturingCallable<?, Out>) captor;
          })
          .map((Function<CapturingCallable<?, Out>, List<Out>>) CapturingCallable::toHolder)
          .filter(l -> !l.isEmpty())
          .forEach(l -> {
            atLeastOneOutput.set(true);
            l.forEach(c::output);
          });
      return atLeastOneOutput.get();
    }

    /**
     * Return a new Stream representing all of the callables of this {@link
     * OperationFilteringFirestore}
     *
     * @return New stream of all callables
     */
    private Stream<CapturingCallable<?, ? extends FirestoreRequestHolder<?>>> newCaptorsStream() {
      return Stream.of(
          commitCallable,
          batchWriteCallable,
          listCollectionIdsPagedCallable,
          partitionQueryPagedCallable,
          listDocumentsPagedCallable,
          batchGetDocumentsCallable,
          runQueryCallable
      );
    }
  }

  /**
   * Interface representing a Callable who's request will be captured
   * <p/>
   * This type has to be an interface, because the types it is decorating are abstract classes.
   * @param <Request> Proto class which will be captured for an invocation of this callable
   * @param <Holder> Holder type the request will be wrapped in
   */
  private interface CapturingCallable<
      Request extends Message,
      Holder extends FirestoreRequestHolder<Request>
      > {

    /**
     * Clear captured state
     */
    void reset();

    /**
     * Wrap any captured requests in the corresponding Holder class and return as a List
     *
     * @return List of Holder wrapped instances of Request which were captured since the last {@link
     * #reset}
     */
    List<Holder> toHolder();

    /**
     * Get an instance of the Class representing the Holder type of this class
     *
     * @return Class representing the Holder type of this class
     */
    Class<Holder> getHolderClass();

    /**
     * Given a Class {@code clazz} determine if it is compatible with {@link Holder}s type
     *
     * @param clazz Class to test for compatibility
     * @return {@code true} if {@code clazz} is compatible with {@link Holder}s type, {@code false}
     * otherwise
     */
    default boolean applicableTo(Class<?> clazz) {
      Class<Holder> dataClass = getHolderClass();
      return dataClass.isAssignableFrom(clazz);
    }
  }

  /**
   * Decorator for a {@link UnaryCallable} where we capture any request that is provided to it
   * @param <Request> Proto class which will be captured for an invocation of this callable
   * @param <Response> Proto class representing the Response type of the UnaryCallable
   * @param <Holder> Holder type the request will be wrapped in
   */
  private static class CapturingUnaryCallable<
      Request extends Message,
      Response,
      Holder extends FirestoreRequestHolder<Request>
      >
      extends UnaryCallable<Request, Response>
      implements CapturingCallable<Request, Holder> {
    private final Function<Request, Holder> toData;
    private final Class<Holder> holderClass;
    private final Function<Request, ApiFuture<Response>> handler;

    private final List<Request> requests;

    private CapturingUnaryCallable(
        Function<Request, Holder> toData, Class<Holder> holderClass, Function<Request, ApiFuture<Response>> handler) {
      this.toData = toData;
      this.holderClass = holderClass;
      this.requests = new ArrayList<>();
      this.handler = handler;
    }

    @Override
    public ApiFuture<Response> futureCall(Request request, ApiCallContext context) {
      this.requests.add(request);
      return handler.apply(request);
    }

    @Override
    public void reset() {
      requests.clear();
    }

    @Override
    public List<Holder> toHolder() {
      return requests.isEmpty()
          ? Collections.emptyList()
          : requests.stream()
              .map(toData)
              .collect(Collectors.toList());
    }

    @Override
    public Class<Holder> getHolderClass() {
      return holderClass;
    }
  }

  /**
   * Decorator for a {@link ServerStreamingCallable} where we capture any request that is provided
   * to it
   *
   * @param <Request> Proto class which will be captured for an invocation of this callable
   * @param <Response> Proto class representing the Response type of the UnaryCallable
   * @param <Holder> Holder type the request will be wrapped in
   */
  private static final class CapturingStreamingCallable<
      Request extends Message,
      Response,
      Holder extends FirestoreRequestHolder<Request>
      >
      extends ServerStreamingCallable<Request, Response>
      implements CapturingCallable<Request, Holder> {
    private final Function<Request, Holder> toData;
    private final Class<Holder> holderClass;

    private final List<Request> requests;

    private CapturingStreamingCallable(Function<Request, Holder> toData, Class<Holder> holderClass) {
      this.toData = toData;
      this.holderClass = holderClass;
      this.requests = new ArrayList<>();
    }

    @Override
    public void call(Request request, ResponseObserver<Response> responseObserver, ApiCallContext context) {
      this.requests.add(request);
      responseObserver.onComplete();
    }

    @Override
    public void reset() {
      requests.clear();
    }

    @Override
    public List<Holder> toHolder() {
      return requests.isEmpty()
          ? Collections.emptyList()
          : requests.stream()
              .map(toData)
              .collect(Collectors.toList());
    }

    @Override
    public Class<Holder> getHolderClass() {
      return holderClass;
    }
  }

  /**
   * Our custom implementation of {@link Firestore} which filters operations which can be invoked on
   * it. If an operation is not supported an {@link UnsupportedOperationException} will be thrown.
   */
  @VisibleForTesting
  static final class OperationFilteringFirestore implements Firestore {
    private final Firestore fs;

    OperationFilteringFirestore(Firestore fs) {
      this.fs = fs;
    }

    @Override
    public FirestoreOptions getOptions() {
      return fs.getOptions();
    }

    @Override
    @Nonnull
    public BulkWriter bulkWriter() {
      // return bulkWriter(BulkWriterOptions.builder().setThrottlingEnabled(false).build());
      return fs.bulkWriter();
    }

    @Override
    @Nonnull
    public BulkWriter bulkWriter(BulkWriterOptions bulkWriterOptions) {
      return fs.bulkWriter(bulkWriterOptions);
    }

    @Nonnull
    @Override
    public FirestoreBundle.Builder bundleBuilder() {
      throw new UnsupportedOperationException("BundleBuilder not supported");
    }

    @Nonnull
    @Override
    public FirestoreBundle.Builder bundleBuilder(@Nonnull String s) {
      throw new UnsupportedOperationException("BundleBuilder not supported");
    }

    @Override
    @Nonnull
    public CollectionReference collection(@Nonnull String path) {
      return fs.collection(path);
    }

    @Override
    @Nonnull
    public DocumentReference document(@Nonnull String path) {
      return fs.document(path);
    }

    @Override
    @Nonnull
    public Iterable<CollectionReference> listCollections() {
      return fs.listCollections();
    }

    @Override
    public CollectionGroup collectionGroup(@Nonnull String collectionId) {
      return fs.collectionGroup(collectionId);
    }

    @Override
    @Nonnull
    public <T> ApiFuture<T> runTransaction(
        @Nonnull Transaction.Function<T> updateFunction) {
      throw new IllegalStateException("runTransaction(Transaction.Function<T>) is not supported");
    }

    @Override
    @Nonnull
    public <T> ApiFuture<T> runTransaction(
        @Nonnull Transaction.Function<T> updateFunction,
        @Nonnull TransactionOptions transactionOptions) {
      throw new IllegalStateException("runTransaction(Transaction.Function<T>, TransactionOptions) is not supported");
    }

    @Override
    @Nonnull
    public <T> ApiFuture<T> runAsyncTransaction(
        @Nonnull AsyncFunction<T> updateFunction) {
      throw new IllegalStateException("runAsyncTransaction(Transaction.Function<T>) is not supported");
    }

    @Override
    @Nonnull
    public <T> ApiFuture<T> runAsyncTransaction(
        @Nonnull AsyncFunction<T> updateFunction,
        @Nonnull TransactionOptions transactionOptions) {
      throw new IllegalStateException("runAsyncTransaction(Transaction.Function<T>, TransactionOptions) is not supported");
    }

    @Override
    @Nonnull
    public ApiFuture<List<DocumentSnapshot>> getAll(
        @Nonnull DocumentReference... documentReferences) {
      return fs.getAll(documentReferences);
    }

    @Override
    @Nonnull
    public ApiFuture<List<DocumentSnapshot>> getAll(
        @Nonnull DocumentReference[] documentReferences,
        @javax.annotation.Nullable FieldMask fieldMask) {
      return fs.getAll(documentReferences, fieldMask);
    }

    @Override
    public void getAll(
        @Nonnull DocumentReference[] documentReferences,
        @javax.annotation.Nullable FieldMask fieldMask,
        ApiStreamObserver<DocumentSnapshot> responseObserver) {
      throw new IllegalStateException(
          "getAll(DocumentReference[], FieldMask, ApiStreamObserver<DocumentSnapshot>) is not supported. Please use getAll(DocumentReference[], FieldMask) instead.");
    }

    @Override
    @Nonnull
    public WriteBatch batch() {
      return fs.batch();
    }

    @Override
    public void close() {

    }
  }

  private static final class BatchWriteFunction implements Function<BatchWriteRequest, ApiFuture<BatchWriteResponse>> {

    private static final int OK = io.grpc.Status.OK.getCode().value();
    private static final com.google.protobuf.Timestamp TIMESTAMP = com.google.protobuf.Timestamp.newBuilder()
        .setSeconds(Long.MIN_VALUE)
        .build();

    @Override
    public ApiFuture<BatchWriteResponse> apply(BatchWriteRequest request) {
      BatchWriteResponse.Builder builder = BatchWriteResponse.newBuilder();
      for (Write write : request.getWritesList()) {
        builder.addWriteResults(WriteResult.newBuilder().setUpdateTime(TIMESTAMP));
        builder.addStatus(Status.newBuilder().setCode(OK));
      }
      return ApiFutures.immediateFuture(builder.build());
    }
  }

}
