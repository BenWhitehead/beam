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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.FieldFilter;
import com.google.firestore.v1.StructuredQuery.FieldFilter.Operator;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.QueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnQueryTest extends
    BaseFirestoreSdkFnTest<String, QueryRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    FirestoreSdkFn<
        String,
        ApiFuture<QuerySnapshot>,
        RunQueryRequest,
        ManagedFirestoreSdkFutureFunction<String, QuerySnapshot>,
        QueryRequestHolder
        > fn = FirestoreIO.sdk()
        .read()
        .query((Firestore fs, String collection) -> fs.collection(collection).whereEqualTo("foo", "bar").get())
        .withDescription("where foo = 'bar'")
        .build()
        .getFn();

    when(processContext.element()).thenReturn("col");

    ArgumentCaptor<QueryRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(QueryRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    QueryRequestHolder requestData = requestCaptor.getValue();
    RunQueryRequest request = requestData.getRequest();
    StructuredQuery structuredQuery = request.getStructuredQuery();
    FieldFilter fieldFilter = structuredQuery.getWhere().getFieldFilter();
    assertEquals("foo", fieldFilter.getField().getFieldPath());
    assertEquals(Operator.EQUAL, fieldFilter.getOp());
    assertEquals("bar", fieldFilter.getValue().getStringValue());
  }

  @Override
  protected NonWindowAwareDoFn<String, QueryRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(RunQueryRequest.getDefaultInstance()),
        QueryRequestHolder.class
    );
  }
}
