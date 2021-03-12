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
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.common.collect.ImmutableMap;
import com.google.firestore.v1.CommitRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.CommitRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnBatchWriteTest extends
    BaseFirestoreSdkFnTest<Map<String, Map<String, Object>>, CommitRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    String projectId = "testing-project";
    FirestoreSdkFn<
        Map<String, Map<String, Object>>,
        ApiFuture<List<WriteResult>>,
        CommitRequest,
        ManagedFirestoreSdkFutureFunction<Map<String, Map<String, Object>>, List<WriteResult>>,
        CommitRequestHolder
        > fn = FirestoreIO.sdk()
        .write()
        .batch((Firestore fs, Map<String, Map<String, Object>> data) -> {
          WriteBatch batch = fs.batch();
          data.forEach((k, v) -> batch.set(fs.document(k), v));
          return batch.commit();
        })
        .withDescription("batch write map")
        .build()
        .getFn();

    String doc = "doc";
    ImmutableMap<String, Map<String, Object>> data = ImmutableMap.<String, Map<String, Object>>builder()
        .put(String.format("collection/%s-1", doc), ImmutableMap.of("foo", "bar"))
        .put(String.format("collection/%s-2", doc), ImmutableMap.of("foo", "bar"))
        .put(String.format("collection/%s-3", doc), ImmutableMap.of("foo", "bar"))
        .build();
    when(processContext.element()).thenReturn(data);

    ArgumentCaptor<CommitRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(CommitRequestHolder.class);
    runFunction(fn);
    verify(processContext, atLeastOnce()).output(requestCaptor.capture());

    CommitRequestHolder value = requestCaptor.getValue();
    CommitRequest request = value.getRequest();
    Set<String> docNames = request.getWritesList().stream()
        .map(w -> w.getUpdate().getName())
        .collect(Collectors.toSet());

    Set<String> expected = newHashSet(
        String.format("projects/%s/databases/(default)/documents/collection/%s-1", projectId, doc),
        String.format("projects/%s/databases/(default)/documents/collection/%s-2", projectId, doc),
        String.format("projects/%s/databases/(default)/documents/collection/%s-3", projectId, doc)
    );

    assertEquals(expected, docNames);
  }

  @Override
  protected NonWindowAwareDoFn<Map<String, Map<String, Object>>, CommitRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(CommitRequest.getDefaultInstance()),
        CommitRequestHolder.class
    );
  }
}
