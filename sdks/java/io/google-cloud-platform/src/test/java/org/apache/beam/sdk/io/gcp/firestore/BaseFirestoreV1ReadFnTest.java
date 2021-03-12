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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.BaseFirestoreV1ReadFn;
import org.junit.Test;
import org.mockito.Mock;

@SuppressWarnings("initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
abstract class BaseFirestoreV1ReadFnTest<In, Out> extends BaseFirestoreV1FnTest<In, Out, BaseFirestoreV1ReadFn<In, Out>> {

  @Mock
  protected RpcQos.RpcReadAttempt attempt;

  @Override
  @Test
  public final void attemptsExhaustedForRetryableError() throws Exception {
    BaseFirestoreV1ReadFn<In, Out> fn = getFn(clock, ff, RPC_QOS_OPTIONS);
    V1RpcFnTestCtx ctx = newCtx();
    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(fn.getRpcAttemptContext())).thenReturn(attempt);
    ctx.mockRpcToCallable(rpc);

    when(attempt.awaitSafeToProceed(any())).thenReturn(true, true, true);
    ctx.whenCallableCall(any(), RETRYABLE_ERROR, RETRYABLE_ERROR, RETRYABLE_ERROR);
    doNothing().when(attempt).recordFailedRequest(any());
    doNothing().doNothing().doThrow(RETRYABLE_ERROR).when(attempt).checkCanRetry(RETRYABLE_ERROR);

    when(processContext.element()).thenReturn(ctx.getRequest());

    try {
      runFunction(fn);
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (ApiException e) {
      assertSame(RETRYABLE_ERROR, e);
    }

    verify(attempt, times(3)).awaitSafeToProceed(any());
    verify(attempt, times(3)).recordFailedRequest(any());
    verify(attempt, times(0)).recordStreamValue(any());
    verify(attempt, times(0)).recordSuccessfulRequest(any());
  }

  @Override
  @Test
  public final void noRequestIsSentIfNotSafeToProceed() throws Exception {
    BaseFirestoreV1ReadFn<In, Out> fn = getFn(clock, ff, RPC_QOS_OPTIONS);
    V1RpcFnTestCtx ctx = newCtx();
    when(ff.getFirestoreRpc(any())).thenReturn(rpc);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(fn.getRpcAttemptContext())).thenReturn(attempt);

    InterruptedException interruptedException = new InterruptedException();
    when(attempt.awaitSafeToProceed(any())).thenReturn(false).thenThrow(interruptedException);

    when(processContext.element()).thenReturn(ctx.getRequest());

    try {
      runFunction(fn);
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (InterruptedException e) {
      assertSame(interruptedException, e);
    }

    verify(rpc, times(1)).close();
    verifyNoMoreInteractions(rpc);
    ctx.verifyNoInteractionsWithCallable();
    verify(attempt, times(0)).recordFailedRequest(any());
    verify(attempt, times(0)).recordStreamValue(any());
    verify(attempt, times(0)).recordSuccessfulRequest(any());
  }

  @Test
  public abstract void resumeFromLastReadValue() throws Exception;

  protected abstract V1RpcFnTestCtx newCtx();

  @Override
  protected final BaseFirestoreV1ReadFn<In, Out> getFn() {
    return getFn(JodaClock.DEFAULT, FirestoreStatefulComponentFactory.INSTANCE, RPC_QOS_OPTIONS);
  }

  protected abstract BaseFirestoreV1ReadFn<In, Out> getFn(JodaClock clock, FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions);

  @Override
  protected void processElementsAndFinishBundle(BaseFirestoreV1ReadFn<In, Out> fn, int processElementCount) throws Exception {
    try {
      for (int i = 0; i < processElementCount; i++) {
        fn.processElement(processContext);
      }
    } finally {
      fn.finishBundle();
    }
  }

  /**
   * This class exists due to the fact that there is not a common parent interface in gax
   * for each of the types of Callables that are output upon code generation.
   */
  protected abstract class V1RpcFnTestCtx {

    protected V1RpcFnTestCtx() {
    }

    public abstract In getRequest();

    public abstract void mockRpcToCallable(FirestoreRpc rpc);

    public abstract void whenCallableCall(In in, Throwable... throwables);

    public abstract void verifyNoInteractionsWithCallable();
  }


}
