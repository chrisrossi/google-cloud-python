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

"""Functions that interact with Datastore backend."""

import itertools
import logging

import grpc

from google.cloud import _helpers
from google.cloud import _http
from google.cloud.datastore_v1.proto import datastore_pb2
from google.cloud.datastore_v1.proto import datastore_pb2_grpc
from google.cloud.datastore_v1.proto import entity_pb2

from google.cloud.ndb import _eventloop
from google.cloud.ndb import _runstate
from google.cloud.ndb import tasklets

EVENTUAL = datastore_pb2.ReadOptions.EVENTUAL
EVENTUAL_CONSISTENCY = EVENTUAL  # Legacy NDB
_NOT_FOUND = object()
log = logging.getLogger(__name__)


def stub():
    """Get the stub for the `Google Datastore` API.

    Gets the stub from the current context, creating one if there isn't one
    already.

    Returns:
        :class:`~google.cloud.datastore_v1.proto.datastore_pb2_grpc.DatastoreStub`:
            The stub instance.
    """
    state = _runstate.current()

    if state.stub is None:
        client = state.client
        if client.secure:
            channel = _helpers.make_secure_channel(
                client._credentials, _http.DEFAULT_USER_AGENT, client.host
            )
        else:
            channel = grpc.insecure_channel(client.host)

        state.stub = datastore_pb2_grpc.DatastoreStub(channel)

    return state.stub


class RPC:
    """Indirection for debugging."""

    def __init__(self, name, request):
        log.debug("gRPC call: %s(%s)", name, request)
        self.name = name
        self.request = request

        api = stub()
        call = getattr(api, name)
        self.future = call.future(request)

    def result(self):
        return self.future.result()

    def exception(self):
        return self.future.exception()

    def add_done_callback(self, callback):
        return self.future.add_done_callback(callback)


def lookup(key, **options):
    """Look up a Datastore entity.

    Gets an entity from Datastore, asynchronously. Actually adds the request to
    a batch and fires off a Datastore Lookup call as soon as some code asks for
    the result of one of the batched requests.

    Args:
        key (~datastore.Key): The key for the entity to retrieve.
        options (Dict[str, Any]): The options for the request. For example,
            ``{"read_consistency": EVENTUAL}``.

    Returns:
        :class:`~tasklets.Future`: If not an exception, future's result will be
            either an entity protocol buffer or _NOT_FOUND.
    """
    _check_unsupported_options(options)

    batch = _get_batch(_LookupBatch, options)
    return batch.add(key)


def _get_batch(batch_cls, options):
    """Gets a data structure for storing batched calls to Datastore Lookup.

    The batch data structure is stored in the current run state. If there is
    not already a batch started, a new structure is created and an idle
    callback is added to the current event loop which will eventually perform
    the batch look up.

    Args:
        batch_cls (type): Class representing the kind of operation being
            batched.
        options (Dict[str, Any]): The options for the request. For example,
            ``{"read_consistency": EVENTUAL}``. Calls with different options
            will be placed in different batches.

    Returns:
        batch_cls: An instance of the batch class.
    """
    state = _runstate.current()
    batches = state.batches.get(batch_cls)
    if batches is None:
        state.batches[batch_cls] = batches = {}

    options_key = tuple(sorted(options.items()))
    batch = batches.get(options_key)
    if batch is not None:
        return batch

    def idle():
        batch = batches.pop(options_key)
        batch.idle_callback()

    batches[options_key] = batch = batch_cls(options)
    _eventloop.add_idle(idle)
    return batch


class _LookupBatch:
    """Batch for Lookup requests.

    Attributes:
        options (Dict[str, Any]): See Args.
        todo (Dict[bytes, List[tasklets.Future]]: Mapping of serialized key
            protocol buffers to dependent futures.

    Args:
        options (Dict[str, Any]): The options for the request. For example,
            ``{"read_consistency": EVENTUAL}``. Calls with different options
            will be placed in different batches.
    """

    def __init__(self, options):
        self.options = options
        self.todo = {}

    def add(self, key):
        """Add a key to the batch to look up.

        Args:
            key (datastore.Key): The key to look up.

        Returns:
            tasklets.Future: A future for the eventual result.
        """
        todo_key = key.to_protobuf().SerializeToString()
        future = tasklets.Future(purpose="Lookup({})".format(key))
        self.todo.setdefault(todo_key, []).append(future)
        return future

    def idle_callback(self):
        """Perform a Datastore Lookup on all batched Lookup requests."""
        keys = []
        for todo_key in self.todo.keys():
            key_pb = entity_pb2.Key()
            key_pb.ParseFromString(todo_key)
            keys.append(key_pb)

        read_options = _get_read_options(self.options)
        rpc = _datastore_lookup(keys, read_options)
        _eventloop.queue_rpc(rpc, self.lookup_callback)

    def lookup_callback(self, rpc):
        """Process the results of a call to Datastore Lookup.

        Each key in the batch will be in one of `found`, `missing`, or
        `deferred`. `found` keys have their futures' results set with the
        protocol buffers for their entities. `missing` keys have their futures'
        results with `_NOT_FOUND`, a sentinel value. `deferrred` keys are
        loaded into a new batch so they can be tried again.

        Args:
            rpc (grpc.Future): If not an exception, the result will be an
                instance of
                :class:`google.cloud.datastore_v1.datastore_pb.LookupResponse`
        """
        # If RPC has resulted in an exception, propagate that exception to all
        # waiting futures.
        exception = rpc.exception()
        if exception is not None:
            for future in itertools.chain(*self.todo.values()):
                future.set_exception(exception)
            return

        # Process results, which are divided into found, missing, and deferred
        results = rpc.result()

        # For all deferred keys, batch them up again with their original
        # futures
        if results.deferred:
            next_batch = _get_batch(type(self), self.options)
            for key in results.deferred:
                todo_key = key.SerializeToString()
                next_batch.todo.setdefault(todo_key, []).extend(
                    self.todo[todo_key]
                )

        # For all missing keys, set result to _NOT_FOUND and let callers decide
        # how to handle
        for result in results.missing:
            todo_key = result.entity.key.SerializeToString()
            for future in self.todo[todo_key]:
                future.set_result(_NOT_FOUND)

        # For all found entities, set the result on their corresponding futures
        for result in results.found:
            entity = result.entity
            todo_key = entity.key.SerializeToString()
            for future in self.todo[todo_key]:
                future.set_result(entity)


def _datastore_lookup(keys, read_options):
    """Issue a Lookup call to Datastore using gRPC.

    Args:
        keys (Iterable[entity_pb2.Key]): The entity keys to
            look up.
        read_options (Union[datastore_pb2.ReadOptions, NoneType]): Options for
            the request.

    Returns:
        :class:`grpc.Future`: Future object for eventual result of lookup.
    """
    client = _runstate.current().client
    request = datastore_pb2.LookupRequest(
        project_id=client.project,
        keys=[key for key in keys],
        read_options=read_options,
    )

    return RPC("Lookup", request)


def _get_read_options(options):
    """Get the read options for a request.

    Args:
        options (Dict[str, Any]): The options for the request. For example,
            ``{"read_consistency": EVENTUAL}``. May contain options unrelated
            to creating a :class:`datastore_pb2.ReadOptions` instance, which
            will be ignored.

    Returns:
        datastore_pb2.ReadOptions: The options instance for passing to the
            Datastore gRPC API.

    Raises:
        ValueError: When ``read_consistency`` is set to ``EVENTUAL`` and there
            is a transaction.
    """
    transaction = _get_transaction(options)

    read_consistency = options.get("read_consistency")
    if read_consistency is None:
        read_consistency = options.get("read_policy")  # Legacy NDB

    if transaction is not None and read_consistency is EVENTUAL:
        raise ValueError(
            "read_consistency must be EVENTUAL when in transaction"
        )

    return datastore_pb2.ReadOptions(
        read_consistency=read_consistency, transaction=transaction
    )


def _get_transaction(options):
    """Get the transaction for a request.

    If specified, this will return the transaction from ``options``. Otherwise,
    it will return the transaction for the current context.

    Args:
        options (Dict[str, Any]): The options for the request. Only
            ``transaction`` will have any bearing here.

    Returns:
        Union[bytes, NoneType]: The transaction identifier, or :data:`None`.
    """
    state = _runstate.current()
    return options.get("transaction", state.transaction)


def put(entity_pb, **options):
    """Store an entity in datastore.

    The entity can be a new entity to be saved for the first time or an
    existing entity that has been updated.

    Args:
        entity_pb (datastore_v1.types.Entity): The entity to be stored.
        options (Dict[str, Any]): Options for this request.

    Returns:
        tasklets.Future: Result will be completed datastore key
            (entity_pb2.Key) for the entity.
    """
    batch = _get_commit_batch(options)
    return batch.put(entity_pb)


def _get_commit_batch(options):
    """XXX"""
    # If not in a transaction we can batch in the normal way
    transaction = _get_transaction(options)
    if not transaction:
        _check_unsupported_options(options)
        return _get_batch(_CommitBatch, options)

    # Support for different options will be tricky if we're in a transaction,
    # since we can only do one commit, so any options that affect that gRPC
    # call would all need to be identical. For now, only "transaction" is
    # suppoorted if there is a transaction.
    options = options.copy()
    options.pop("transaction", None)
    for key in options:
        raise NotImplementedError("Passed bad option: {!r}".format(key))

    # Since we're in a transaction, we need to hang on to the batch until
    # commit time, so we need to store it separately from other batches.
    state = _runstate.current()
    batch = state.commit_batches.get(transaction)
    if batch is None:
        batch = _CommitBatch({"transaction": transaction})
        state.commit_batches[transaction] = batch

    return batch


class _CommitBatch:
    """Batch for tracking a set of mutations for a commit.

    Attributes:
        options (Dict[str, Any]): See Args.
        mutations (List[datastore_pb2.Mutation]): Sequence of mutation protocol
            buffers accumumlated for this batch.
        futures (List[tasklets.Future]): Sequence of futures for return results
            of the commit. The i-th element of ``futures`` corresponds to the
            i-th element of ``mutations``.
        transaction (bytes): The transaction id of the transaction for this
            commit, if in a transaction.
        allocating_ids (List[tasklets.Future]): Futures for any calls to
            AllocateIds that are fired off before commit.
        incomplete_mutations (List[datastore_pb2.Mutation]): List of mutations
            with keys which will need ids allocated. Incomplete keys will be
            allocated by an idle callback. Any keys still incomplete at commit
            time will be allocated by the call to Commit. Only used when in a
            transaction.
        incomplete_futures (List[tasklets.Future]): List of futures
            corresponding to keys in ``incomplete_mutations``. Futures will
            receive results of id allocation.

    Args:
        options (Dict[str, Any]): The options for the request. Calls with
            different options will be placed in different batches.
    """

    def __init__(self, options):
        self.options = options
        self.mutations = []
        self.futures = []
        self.transaction = _get_transaction(options)
        self.allocating_ids = []
        self.incomplete_mutations = []
        self.incomplete_futures = []

    def put(self, entity_pb):
        """Add an entity to batch to be stored.

        Args:
            entity_pb (datastore_v1.types.Entity): The entity to be stored.

        Returns:
            tasklets.Future: Result will be completed datastore key
                (entity_pb2.Key) for the entity.
        """
        future = tasklets.Future(purpose="put({})".format(entity_pb))
        self.futures.append(future)
        mutation = datastore_pb2.Mutation(upsert=entity_pb)
        self.mutations.append(mutation)

        # If we're in a transaction and have an incomplete key, add the
        # incomplete key to a batch for a call to AllocateIds
        if self.transaction:
            if not _complete(entity_pb.key):
                # If this is the first key in the batch, we also need to
                # schedule our idle handler to get called
                if not self.incomplete_mutations:
                    _eventloop.add_idle(self.idle_transaction_callback)

                self.incomplete_mutations.append(mutation)
                self.incomplete_futures.append(future)

            # Complete keys get passed back None
            else:
                future.set_result(None)

        return future

    def idle_callback(self):
        """Call Commit.

        Only used when not in a transaction.
        """
        rpc = _datastore_commit(self.mutations)
        _eventloop.queue_rpc(rpc, self.commit_callback)

    def idle_transaction_callback(self):
        """Call AllocateIds on any incomplete keys in the batch.

        Only used when in a transaction.
        """
        if not self.incomplete_mutations:
            # This will happen if `commit` is called first.
            return

        # Signal to a future commit that there is an id allocation in
        # progress and it should wait.
        allocating_ids = tasklets.Future(purpose="AllocateIds")
        self.allocating_ids.append(allocating_ids)

        mutations = self.incomplete_mutations
        futures = self.incomplete_futures

        def callback(rpc):
            self.allocate_ids_callback(rpc, mutations, futures)

            # Signal that we're done allocating these ids
            allocating_ids.set_result(None)

        keys = [mutation.upsert.key for mutation in mutations]
        rpc = _datastore_allocate_ids(keys)
        _eventloop.queue_rpc(rpc, callback)

        self.incomplete_mutations = []
        self.incomplete_futures = []

    def commit_callback(self, rpc):
        """Process the results of a commit request.

        For each mutation, set the result to the key handed back from
            Datastore. If a key wasn't allocated for the mutation, this will be
            :data:`None`.

        Args:
            rpc (grpc.Future): If not an exception, the result will be an
                instance of
                :class:`google.cloud.datastore_v1.datastore_pb2.CommitResponse`
        """
        # If RPC has resulted in an exception, propagate that exception to all
        # waiting futures.
        exception = rpc.exception()
        if exception is not None:
            for future in self.futures:
                if not future.done():
                    future.set_exception(exception)
            return

        # "The i-th mutation result corresponds to the i-th mutation in the
        # request."
        #
        # https://github.com/googleapis/googleapis/blob/master/google/datastore/v1/datastore.proto#L241
        response = rpc.result()
        results_futures = zip(response.mutation_results, self.futures)
        for mutation_result, future in results_futures:
            if future.done():
                continue

            # Datastore only sends a key if one is allocated for the
            # mutation. Confusingly, though, if a key isn't allocated, instead
            # of getting None, we get a key with an empty path.
            if mutation_result.key.path:
                key = mutation_result.key
            else:
                key = None
            future.set_result(key)

    def allocate_ids_callback(self, rpc, mutations, futures):
        """XXX"""
        # If RPC has resulted in an exception, propagate that exception to
        # all waiting futures.
        exception = rpc.exception()
        if exception is not None:
            for future in futures:
                future.set_exception(exception)
            return

        # Update mutations with complete keys
        response = rpc.result()
        for mutation, key, future in zip(mutations, response.keys, futures):
            mutation.upsert.key.CopyFrom(key)
            future.set_result(key)

    @tasklets.tasklet
    def commit(self):
        """Transactional commit."""
        if not self.mutations:
            return

        # Wait for any calls to AllocateIds that have been fired off so we
        # don't allocate ids again in the commit.
        for future in self.allocating_ids:
            if not future.done():
                yield future

        # Head off making any more AllocateId calls. Any remaining incomplete
        # keys will get ids as part of the Commit call.
        self.incomplete_mutations = []
        self.incomplete_futures = []

        future = tasklets.Future(purpose="Commit")

        def callback(rpc):
            self.commit_callback(rpc)

            exception = rpc.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(None)

        _eventloop.queue_rpc(
            _datastore_commit(self.mutations, transaction=self.transaction),
            callback)
        yield future


def _complete(key_pb):
    """Determines whether a key protocol buffer is complete.

    A new key may be left incomplete so that the id can be allocated by the
    database. A key is considered incomplete if the last element of the path
    has neither a ``name`` or an ``id``.

    Args:
        key_pb (entity_pb2.Key): The key to check.

    Returns:
        boolean: :data:`True` if key is incomplete, otherwise :data:`False`.
    """
    if key_pb.path:
        element = key_pb.path[-1]
        if element.id or element.name:
            return True

    return False


def _datastore_commit(mutations, transaction=None):
    """Call Commit on Datastore.

    Args:
        mutations (List[datastore_pb2.Mutation]): The changes to persist to
            Datastore.
        transaction (Union[bytes, NoneType]): The identifier for the
            transaction for this commit, or :data:`None` if no transaction is
            being used.

    Returns:
        grpc.Future: A future for
            :class:`google.cloud.datastore_v1.datastore_pb2.CommitResponse`
    """
    if transaction is None:
        mode = datastore_pb2.CommitRequest.NON_TRANSACTIONAL
    else:
        mode = datastore_pb2.CommitRequest.TRANSACTIONAL

    client = _runstate.current().client
    request = datastore_pb2.CommitRequest(
        project_id=client.project,
        mode=mode,
        mutations=mutations,
        transaction=transaction,
    )

    return RPC("Commit", request)


def _datastore_allocate_ids(keys):
    """Calls ``AllocateIds`` on Datastore.

    Args:
        keys (List[google.cloud.datastore_v1.entity_pb2.Key]): List of
            incomplete keys to allocate.

    Returns:
        grpc.Future: A future for
            :class:`google.cloud.datastore_v1.datastore_pb2.AllocateIdsResponse`
    """
    client = _runstate.current().client
    request = datastore_pb2.AllocateIdsRequest(
        project_id=client.project, keys=keys)

    return RPC("AllocateIds", request)


@tasklets.tasklet
def begin_transaction(read_only):
    """Start a new transction.

    Args:
        read_only (bool): Whether to start a read-only or read-write
            transaction.

    Returns:
        tasklets.Future: Result will be Transaction Id (bytes) of new
            transaction.
    """
    response = yield _datastore_begin_transaction(read_only)
    return response.transaction


def _datastore_begin_transaction(read_only):
    """Calls ``BeginTransaction`` on Datastore.

    Args:
        read_only (bool): Whether to start a read-only or read-write
            transaction.

    Returns:
        grpc.Future: A future for
            :class:`google.cloud.datastore_v1.datastore_pb2.BeginTransactionResponse`
    """
    client = _runstate.current().client
    if read_only:
        options = datastore_pb2.TransactionOptions(
            read_only=datastore_pb2.TransactionOptions.ReadOnly(),
        )
    else:
        options = datastore_pb2.TransactionOptions(
            read_write=datastore_pb2.TransactionOptions.ReadWrite(),
        )

    request = datastore_pb2.BeginTransactionRequest(
        project_id=client.project,
        transaction_options=options,
    )
    return RPC("BeginTransaction", request)


def _datastore_rollback(transaction):
    """Calls Rollback in Datastore.

    Args:
        transaction (bytes): Transaction id.

    Returns:
        grpc.Future: Future for
            :class:`google.cloud.datastore_v1.datastore_pb2.RollbackResponse`
    """
    client = _runstate.current().client
    request = datastore_pb2.RollbackRequest(
        project_id=client.project, transaction=transaction)

    return RPC("Rollback", request)


_OPTIONS_SUPPORTED = {"transaction", "read_consistency", "read_policy"}

_OPTIONS_NOT_IMPLEMENTED = {
    "deadline",
    "force_writes",
    "use_cache",
    "use_memcache",
    "use_datastore",
    "memcache_timeout",
    "max_memcache_items",
    "xg",
    "propagation",
    "retries",
}


def _check_unsupported_options(options):
    """Check to see if any passed options are not supported.

    options (Dict[str, Any]): The options for the request. For example,
        ``{"read_consistency": EVENTUAL}``.

    Raises: NotImplementedError if any options are not supported.
    """
    for key in options:
        if key in _OPTIONS_NOT_IMPLEMENTED:
            # option is used in Legacy NDB, but has not yet been implemented in
            # the rewrite, nor have we determined it won't be used, yet.
            raise NotImplementedError(
                "Support for option {!r} has not yet been implemented".format(
                    key
                )
            )

        elif key not in _OPTIONS_SUPPORTED:
            raise NotImplementedError("Passed bad option: {!r}".format(key))
