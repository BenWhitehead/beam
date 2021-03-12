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
package org.apache.beam.sdk.io.gcp.firestore.it;

import static org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.getBaseDocument;
import static org.junit.Assert.assertFalse;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.common.collect.ImmutableMap;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.Write;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Streams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

public final class FirestoreSdkIT extends BaseFirestoreIT {

  @Override
  protected PTransform<PCollection<List<String>>, PCollection<Write>> getWritePTransform(
      String testMethodName, String collectionId) {
    return FirestoreIO.sdk()
        .write()
        .<List<String>>batch((fs, documentIds) -> {
          // a batch can have at most 500 writes in it, ensure we keep within those bounds by
          // chunking up the documentIds provided to us
          Stream<List<String>> chunks = FirestoreTestingHelper.chunkUpDocIds(documentIds);
          List<ApiFuture<List<WriteResult>>> list =
              chunks.map(chunk ->
                  chunk.stream()
                      .map(val ->
                          getBaseDocument(fs, FirestoreSdkIT.class, testMethodName)
                              .collection(collectionId)
                              .document(val)
                              .set(ImmutableMap.of("foo", val))
                      )
                      .collect(Collectors.toList())
              )
                  .map(ApiFutures::allAsList)
                  .collect(Collectors.toList());

          return ApiFutures.transform(
              ApiFutures.allAsList(list),
              FirestoreTestingHelper.flattenListList(),
              MoreExecutors.directExecutor()
          );
        })
        .withDescription(String.format("/%s/doc{foo:?}", collectionId))
        .build();
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<ListCollectionIdsRequest>> getListCollectionIdsPTransform(
      String testMethodName) {
    return FirestoreIO.sdk().read()
        .<String>listCollections((fs, o) -> getBaseDocument(fs, FirestoreSdkIT.class, testMethodName).listCollections())
        .withDescription("/*")
        .build();
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<ListDocumentsRequest>> getListDocumentsPTransform(
      String testMethodName) {
    return FirestoreIO.sdk().read()
        .<String>listDocuments((fs, collectionId) -> getBaseDocument(fs, FirestoreSdkIT.class, testMethodName).collection(collectionId).listDocuments())
        .withDescription("/*")
        .build();
  }

  @Override
  protected PTransform<PCollection<List<String>>, PCollection<BatchGetDocumentsRequest>> getBatchGetDocumentsPTransform(
      String testMethodName, String collectionId) {
    return FirestoreIO.sdk().read()
        .<List<String>>getAll((fs, documentIds) -> fs.getAll(
            Streams.concat(Stream.of("404"), documentIds.stream())
                .map(id -> getBaseDocument(fs, FirestoreSdkIT.class, testMethodName).collection(collectionId).document(id))
                .toArray(DocumentReference[]::new)
            )
        )
        .withDescription("batchGet")
        .build();
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<RunQueryRequest>> getRunQueryPTransform(
      String testMethodName) {
    return FirestoreIO.sdk().read()
        .<String>query((fs, collectionId) -> getBaseDocument(fs, FirestoreSdkIT.class, testMethodName).collection(collectionId).whereEqualTo("foo", "bar").get())
        .withDescription("select * where foo = 'bar'")
        .build();
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<PartitionQueryRequest>> getPartitionQueryPTransform(String testMethodName) {
    return FirestoreIO.sdk().read()
        .<String>collectionGroupPartitionQuery(
            (fs, collectionGroupId) -> fs.collectionGroup(collectionGroupId).getPartitions(3))
        .withDescription("select * from ? | partition 3")
        .build()
        ;
  }

  @Test
  public void write_writeBatch() {
    // TestName above isn't Serializable, so we read the method name early so that we're able to
    // close over the String value. This isn't defined in the setup method because the closed over
    // reference will be FirestoreIT.methodName, and FirestoreIT intentionally isn't serializable.
    String methodName = testName.getMethodName();

    String collectionId = "col";

    PTransform<PCollection<List<String>>, PCollection<Write>> createWrite = FirestoreIO.sdk()
        .write()
        .<List<String>>batch((fs, documentIds) -> {

          // a batch can have at most 500 writes in it, ensure we keep within those bounds by
          // chunking up the documentIds provided to us
          Stream<List<String>> chunks = FirestoreTestingHelper.chunkUpDocIds(documentIds);

          List<ApiFuture<List<WriteResult>>> batchFutures = chunks
              .map(chunk -> {
                WriteBatch batch = fs.batch();
                chunk.forEach(docId -> {
                  DocumentReference ref = getBaseDocument(fs, FirestoreSdkIT.class, methodName)
                      .collection(collectionId)
                      .document(docId);
                  batch.set(ref, ImmutableMap.of("foo", docId));
                });
                return batch.commit();
              })
              .collect(Collectors.toList());

          return ApiFutures.transform(
              ApiFutures.allAsList(batchFutures),
              FirestoreTestingHelper.flattenListList(),
              MoreExecutors.directExecutor()
          );
        })
        .withDescription("/col/doc-?{foo:?}")
        .build();
    runWriteTest(createWrite, collectionId);
  }

  @Test
  public void write_bulkWriter() {
    // TestName above isn't Serializable, so we read the method name early so that we're able to
    // close over the String value. This isn't defined in the setup method because the closed over
    // reference will be FirestoreIT.methodName, and FirestoreIT intentionally isn't serializable.
    String methodName = testName.getMethodName();

    String collectionId = "col";

    PTransform<PCollection<List<String>>, PCollection<Write>> createWrite = FirestoreIO.sdk()
        .write()
        .<List<String>>bulkWriter((fs, documentIds) -> {

          // a batch can have at most 500 writes in it, ensure we keep within those bounds by
          // chunking up the documentIds provided to us
          Stream<List<String>> chunks = FirestoreTestingHelper.chunkUpDocIds(documentIds);

          List<ApiFuture<Void>> batchFutures = chunks
              .map(chunk -> {
                BulkWriter bulkWriter = fs.bulkWriter();
                chunk.forEach(docId -> {
                  DocumentReference ref = getBaseDocument(fs, FirestoreSdkIT.class, methodName)
                      .collection(collectionId)
                      .document(docId);
                  // we don't need to hold onto this future, as we will track completion via
                  // the bulkWriter.flush() below
                  ApiFuture<WriteResult> ignore = bulkWriter.set(ref, ImmutableMap.of("foo", docId));
                });
                return bulkWriter.flush();
              })
              .collect(Collectors.toList());

          ApiFuture<List<Void>> listApiFuture = ApiFutures.allAsList(batchFutures);
          return ApiFutures.transform(
              listApiFuture,
              (voids) -> null,
              MoreExecutors.directExecutor()
          );
        })
        .withDescription("/col/doc-?{foo:?}")
        .build();
    runWriteTest(createWrite, collectionId);
  }

  @Test
  public void write_deleteDocThatDoesNotExist() {
    // TestName above isn't Serializable, so we read the method name early so that we're able to
    // close over the String value. This isn't defined in the setup method because the closed over
    // reference will be FirestoreIT.methodName, and FirestoreIT intentionally isn't serializable.
    String methodName = testName.getMethodName();

    PTransform<PCollection<String>, PCollection<Write>> delete404 = FirestoreIO.sdk()
        .write()
        .<String>single((fs, s) ->
            getBaseDocument(fs, FirestoreSdkIT.class, methodName)
                .collection("col")
                .document("404")
                .delete()
        )
        .withDescription("/col/doc-?{foo:?}")
        .build();

    testPipeline.apply(Create.of(""))
        .apply(delete404)
        .apply(FirestoreIO.v1().write().batchWrite().withRpcQosOptions(rpcQosOptions).build());

    testPipeline.run(options);

    assertFalse(helper.getBaseDocument().listCollections().iterator().hasNext());
  }

}
