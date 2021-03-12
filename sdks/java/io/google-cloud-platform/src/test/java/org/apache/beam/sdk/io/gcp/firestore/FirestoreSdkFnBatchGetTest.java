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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.BatchGetDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnBatchGetTest extends
    BaseFirestoreSdkFnTest<String, BatchGetDocumentsRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    FirestoreSdkFn<
        String,
        ApiFuture<List<DocumentSnapshot>>,
        BatchGetDocumentsRequest,
        ManagedFirestoreSdkFutureFunction<String, List<DocumentSnapshot>>,
        BatchGetDocumentsRequestHolder
        > fn = FirestoreIO.sdk()
        .read()
        .getAll((Firestore fs, String collection) -> fs.getAll(
            fs.collection(collection).document("abcd"),
            fs.collection(collection).document("efgh")
        ))
        .withDescription("getAll")
        .build()
        .getFn();

    String collection = "col";
    when(processContext.element()).thenReturn(collection);

    ArgumentCaptor<BatchGetDocumentsRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(BatchGetDocumentsRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    BatchGetDocumentsRequestHolder requestData = requestCaptor.getValue();
    BatchGetDocumentsRequest request = requestData.getRequest();
    assertEquals("projects/testing-project/databases/(default)", request.getDatabase());

    HashSet<String> expectedDocReferences = newHashSet(
        String.format("projects/testing-project/databases/(default)/documents/%s/abcd",
            collection),
        String.format("projects/testing-project/databases/(default)/documents/%s/efgh",
            collection)
    );
    Set<String> docReferences = new HashSet<>(request.getDocumentsList());
    assertEquals(expectedDocReferences, docReferences);
  }

  @Override
  protected NonWindowAwareDoFn<String, BatchGetDocumentsRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(BatchGetDocumentsRequest.getDefaultInstance()),
        BatchGetDocumentsRequestHolder.class
    );
  }
}
