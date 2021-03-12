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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionGroup;
import com.google.cloud.firestore.QueryPartition;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Order;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.PartitionQueryRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnPartitionQueryTest extends
    BaseFirestoreSdkFnTest<String, PartitionQueryRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    FirestoreSdkFn<
        String,
        ApiFuture<List<QueryPartition>>,
        PartitionQueryRequest,
        ManagedFirestoreSdkFutureFunction<String, List<QueryPartition>>,
        PartitionQueryRequestHolder
        > fn = FirestoreIO.sdk()
        .read()
        .<String>collectionGroupPartitionQuery((fs, collection) -> {
              CollectionGroup collectionGroup = fs.collectionGroup(collection);
              return collectionGroup.getPartitions(5);
            }
        )
        .withDescription("getAll")
        .build()
        .getFn();


    when(processContext.element()).thenReturn("col");

    ArgumentCaptor<PartitionQueryRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(PartitionQueryRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    PartitionQueryRequestHolder requestData = requestCaptor.getValue();
    PartitionQueryRequest request = requestData.getRequest();
    long partitionCount = request.getPartitionCount();
    // .getPartitions decrements the argument provided to it by one, because it automatically
    // adds another cursor with a open end
    assertEquals(4, partitionCount);
    assertEquals("projects/testing-project/databases/(default)/documents", request.getParent());
    StructuredQuery structuredQuery = request.getStructuredQuery();
    assertNotNull(structuredQuery);
    CollectionSelector from = structuredQuery.getFrom(0);
    assertEquals("col", from.getCollectionId());
    assertTrue(from.getAllDescendants());
    Order order = structuredQuery.getOrderBy(0);
    assertEquals("__name__", order.getField().getFieldPath());
    assertEquals(StructuredQuery.Direction.ASCENDING, order.getDirection());
  }

  @Override
  protected NonWindowAwareDoFn<String, PartitionQueryRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(PartitionQueryRequest.getDefaultInstance()),
        PartitionQueryRequestHolder.class
    );
  }
}
