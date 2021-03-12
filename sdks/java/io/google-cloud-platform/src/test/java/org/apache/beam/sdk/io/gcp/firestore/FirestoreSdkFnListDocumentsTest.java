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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.ListDocumentsRequest;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkIterableFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnListDocumentsTest extends
    BaseFirestoreSdkFnTest<String, ListDocumentsRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    FirestoreSdkFn<
        String,
        Iterable<DocumentReference>,
        ListDocumentsRequest,
        ManagedFirestoreSdkIterableFunction<String, DocumentReference>,
        ListDocumentsRequestHolder
        > fn = FirestoreIO.sdk()
        .read()
        .listDocuments(
            (Firestore fs, String collection) -> fs.collection(collection).listDocuments())
        .withDescription("/base/*")
        .build()
        .getFn();

    when(processContext.element()).thenReturn("col");

    ArgumentCaptor<ListDocumentsRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(ListDocumentsRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    ListDocumentsRequestHolder requestData = requestCaptor.getValue();
    ListDocumentsRequest request = requestData.getRequest();
    assertEquals("projects/testing-project/databases/(default)/documents", request.getParent());
    assertEquals("col", request.getCollectionId());
  }

  @Override
  protected NonWindowAwareDoFn<String, ListDocumentsRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(ListDocumentsRequest.getDefaultInstance()),
        ListDocumentsRequestHolder.class
    );
  }
}
