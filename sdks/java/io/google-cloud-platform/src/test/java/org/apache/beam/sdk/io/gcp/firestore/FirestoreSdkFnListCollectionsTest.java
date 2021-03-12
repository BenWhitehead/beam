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

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.firestore.v1.ListCollectionIdsRequest;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListCollectionIdsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkIterableFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnListCollectionsTest extends
    BaseFirestoreSdkFnTest<String, ListCollectionIdsRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    FirestoreSdkFn<
        String,
        Iterable<CollectionReference>,
        ListCollectionIdsRequest,
        ManagedFirestoreSdkIterableFunction<String, CollectionReference>,
        ListCollectionIdsRequestHolder
        > fn = FirestoreIO.sdk()
        .read()
        .listCollections(
            (Firestore fs, String collection) -> fs.listCollections())
        .withDescription("/base/*")
        .build()
        .getFn();

    when(processContext.element()).thenReturn("col");

    ArgumentCaptor<ListCollectionIdsRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(ListCollectionIdsRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    ListCollectionIdsRequestHolder requestData = requestCaptor.getValue();
    ListCollectionIdsRequest request = requestData.getRequest();
    assertEquals("projects/testing-project/databases/(default)/documents", request.getParent());
  }

  @Override
  protected NonWindowAwareDoFn<String, ListCollectionIdsRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(ListCollectionIdsRequest.getDefaultInstance()),
        ListCollectionIdsRequestHolder.class
    );
  }
}
