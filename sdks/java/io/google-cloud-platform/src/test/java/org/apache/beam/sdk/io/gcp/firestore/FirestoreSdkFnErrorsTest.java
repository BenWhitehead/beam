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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.firestore.DocumentReference;
import com.google.firestore.v1.ListDocumentsRequest;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.ListDocumentsRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFunction;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings({
    "initialization.fields.uninitialized",  // mockito fields are initialized via the Mockito Runner
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public final class FirestoreSdkFnErrorsTest {

  @Mock
  protected DoFn<String, ListDocumentsRequestHolder>.ProcessContext processContext;

  @Test
  public void illegalStateExceptionWhenAttemptingToUseUnsupportedManagedFirestoreSdkFunction() {

    FirestoreStatefulComponentFactory factory = FirestoreStatefulComponentFactory.INSTANCE;
    ManagedFirestoreSdkFunction<String, Iterable<DocumentReference>> f = (firestore, s) -> {
      fail("Function should never actually be invoked");
      return Collections.emptyList();
    };

    when(processContext.element()).thenReturn("element");

    ArgumentCaptor<ListDocumentsRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(ListDocumentsRequestHolder.class);

    try {
      FirestoreSdkFn<
          String,
          Iterable<DocumentReference>,
          ListDocumentsRequest,
          ManagedFirestoreSdkFunction<String, Iterable<DocumentReference>>,
          ListDocumentsRequestHolder
          > fn = new FirestoreSdkFn<>(factory, "desc", f, ListDocumentsRequestHolder.class);
      fn.setup();
      fn.processElement(processContext);
      fail("Expected an IllegalStateException");
    } catch (IllegalStateException e) {
      String message = e.getMessage();
      assertNotNull(message);
      assertTrue(message.contains("Unsupported implementation"));
      assertTrue(message.contains(f.getClass().getName()));
    }

    verify(processContext, never()).output(requestCaptor.capture());

  }

}
