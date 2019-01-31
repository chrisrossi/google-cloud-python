# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud.ndb import _datastore_api
from google.cloud.ndb import _eventloop
from google.cloud.ndb import _runstate
from google.cloud.ndb import tasklets


def transaction(callback, retries=0, read_only=False):
    future = transaction_async(callback, retries=retries, read_only=read_only)
    return future.get_result()


def transaction_async(callback, retries=0, read_only=False):
    # Make sure current context has eventloop before copying
    _eventloop.get_event_loop()

    # Copy context so we can set transaction
    parent = _runstate.current()
    transaction_context = _runstate.State(parent.client)
    transaction_context.__dict__.update(parent.__dict__)
    _runstate.states.push(transaction_context)
    try:
        return _transaction(
            transaction_context, callback, retries=retries,
            read_only=read_only)
    finally:
        context = _runstate.states.pop()
        if (context is not transaction_context or
                _runstate.current() is not parent):
            # If we did this right, this won't ever happen
            raise RuntimeError("Corrupted context stack.")


@tasklets.tasklet
def _transaction(state, callback, retries=0, read_only=False):
    if retries:
        raise NotImplementedError("Retry is not implemented yet")

    # Keep transaction propagation simple: don't do it.
    if state.transaction:
        raise NotImplementedError(
            "Can't start a transaction during a transaction.")

    # Start the transaction
    transaction_id = yield _datastore_api.begin_transaction(read_only)
    state.transaction = transaction_id

    commit_batch = _datastore_api._get_commit_batch(
        {"transaction": transaction_id})

    try:
        # Run the callback
        result = callback()
        if isinstance(result, tasklets.Future):
            result = yield result

        # Commit the transaction
        yield commit_batch.commit()

    # Rollback if there is an error
    except:
        yield _datastore_api._datastore_rollback(transaction_id)
        raise

    # Clean up
    finally:
        state.transaction = None
        state.commit_callback = None

    return result
