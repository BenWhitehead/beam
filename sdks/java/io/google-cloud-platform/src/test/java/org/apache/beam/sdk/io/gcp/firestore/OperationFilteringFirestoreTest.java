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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiStreamObserver;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldMask;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Transaction.AsyncFunction;
import com.google.cloud.firestore.Transaction.Function;
import com.google.cloud.firestore.TransactionOptions;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdkFn.OperationFilteringFirestore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class OperationFilteringFirestoreTest {

  private static final Function<String> TX_FUNCTION = tx -> "asdf";
  private static final AsyncFunction<String> TX_ASYNC_FUNCTION = tx -> ApiFutures.immediateFuture("asdf");

  @Mock
  private Firestore fs;

  @Mock
  private DocumentReference doc1;
  @Mock
  private DocumentReference doc2;

  @Mock
  private ApiStreamObserver<DocumentSnapshot> streamObserver;

  @Test
  public void getOptions_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.getOptions();
    verify(fs, times(1)).getOptions();
  }

  @Test
  public void collection_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.collection("abc");
    verify(fs, times(1)).collection("abc");
  }

  @Test
  public void document_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.document("abc");
    verify(fs, times(1)).document("abc");
  }

  @Test
  public void listCollections_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.listCollections();
    verify(fs, times(1)).listCollections();
  }

  @Test
  public void collectionGroup_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.collectionGroup("abc");
    verify(fs, times(1)).collectionGroup("abc");
  }

  @SuppressWarnings("nullness")
  @Test(expected = IllegalStateException.class)
  public void runTransaction_IllegalStateException() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(null);
    ApiFuture<String> ignore = off.runTransaction(TX_FUNCTION);
  }

  @SuppressWarnings("nullness")
  @Test(expected = IllegalStateException.class)
  public void runTransaction_with_options_IllegalStateException() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(null);
    ApiFuture<String> ignore = off.runTransaction(TX_FUNCTION, TransactionOptions.createReadOnlyOptionsBuilder().build());
  }

  @SuppressWarnings("nullness")
  @Test(expected = IllegalStateException.class)
  public void runAsyncTransaction_IllegalStateException() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(null);
    ApiFuture<String> ignore = off.runAsyncTransaction(TX_ASYNC_FUNCTION);
  }

  @SuppressWarnings("nullness")
  @Test(expected = IllegalStateException.class)
  public void runAsyncTransaction_with_options_IllegalStateException() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(null);
    ApiFuture<String> ignore = off.runAsyncTransaction(TX_ASYNC_FUNCTION, TransactionOptions.createReadOnlyOptionsBuilder().build());
  }

  @Test
  public void getAll_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    ApiFuture<List<DocumentSnapshot>> ignore = off.getAll(doc1, doc2);
    verify(fs, times(1)).getAll(doc1, doc2);
  }

  @Test
  public void getAll_with_fieldMask_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    FieldMask mask = FieldMask.of("foo");
    DocumentReference[] documentReferences = {doc1, doc2};
    ApiFuture<List<DocumentSnapshot>> ignore = off
        .getAll(documentReferences, mask);
    verify(fs, times(1)).getAll(documentReferences, mask);
  }

  @Test(expected = IllegalStateException.class)
  public void getAll_withFieldMaskAndApiStreamObserver_IllegalStateException() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.getAll(new DocumentReference[]{doc1, doc2}, FieldMask.of("foo"), streamObserver);
  }

  @Test
  public void batch_delegates() {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.batch();
    verify(fs, times(1)).batch();
  }

  @Test
  public void close_doesNotDelegate() throws Exception {
    OperationFilteringFirestore off = new OperationFilteringFirestore(fs);
    off.close();
    verify(fs, never()).close();
  }

}
