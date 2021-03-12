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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.common.collect.ImmutableMap;
import com.google.firestore.v1.CommitRequest;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.NonWindowAwareDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreRequest.CommitRequestHolder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreSdk.ManagedFirestoreSdkFutureFunction;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public final class FirestoreSdkFnSingleWriteTest extends
    BaseFirestoreSdkFnTest<KV<String, Map<String, Object>>, CommitRequestHolder> {

  @Test
  public void endToEnd() throws Exception {
    String projectId = "testing-project";
    FirestoreSdkFn<
        KV<String, Map<String, Object>>,
        ApiFuture<WriteResult>,
        CommitRequest,
        ManagedFirestoreSdkFutureFunction<KV<String, Map<String, Object>>, WriteResult>,
        CommitRequestHolder
        > fn = FirestoreIO.sdk()
        .write()
        .single((Firestore fs, KV<String, Map<String, Object>> data) -> fs.document(data.getKey()).set(data.getValue()))
        .withDescription("batch write map")
        .build()
        .getFn();

    KV<String, Map<String, Object>> data = KV
        .of(String.format("collection/%s", "doc"), ImmutableMap.of("foo", "bar"));
    when(processContext.element()).thenReturn(data);

    ArgumentCaptor<CommitRequestHolder> requestCaptor = ArgumentCaptor
        .forClass(CommitRequestHolder.class);

    runFunction(fn);
    verify(processContext, times(1)).output(requestCaptor.capture());

    CommitRequestHolder value = requestCaptor.getValue();
    CommitRequest request = value.getRequest();
    Set<String> docNames = request.getWritesList().stream()
        .map(w -> w.getUpdate().getName())
        .collect(Collectors.toSet());

    Set<String> expected = newHashSet(
        String.format("projects/%s/databases/(default)/documents/collection/%s", projectId, "doc")
    );

    assertEquals(expected, docNames);
  }

  @Override
  protected NonWindowAwareDoFn<KV<String, Map<String, Object>>, CommitRequestHolder> getFn() {
    return new FirestoreSdkFn<>(
        FirestoreStatefulComponentFactory.INSTANCE,
        "desc",
        (s, o) -> FirestoreRequest.create(CommitRequest.getDefaultInstance()),
        CommitRequestHolder.class
    );
  }
}
