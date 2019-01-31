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

import logging
import pytest
import time

from unittest import mock

import grpc
import test_utils.system

from google.cloud.datastore_v1.proto import datastore_pb2_grpc
from google.cloud import datastore
from google.cloud import ndb
from google.cloud.ndb import _runstate


_NOT_PRESENT = object()
DatastoreStub = datastore_pb2_grpc.DatastoreStub
log = logging.getLogger("tests")


class LoggingDatastoreStub:
    """Intercept gRPC API calls for logging purposes."""

    def __init__(self, channel):
        self.stub = DatastoreStub(channel)

    def __getattr__(self, attr):
        value = getattr(self.stub, attr, _NOT_PRESENT)
        if not value:
            raise AttributeError(attr)

        if isinstance(value, grpc.UnaryUnaryMultiCallable):
            return ApiMethodProxy(attr, value)

        log.debug("got: {} {}".format(value, type(value)))
        return value


class ApiMethodProxy:
    """Intercept a single gRPC API call and log it."""

    def __init__(self, name, proxied):
        self.name = name
        self.proxied = proxied

    def __call__(self, *args, **kwargs):
        log.debug("Call: {} {!r} {!r}".format(self.name, args, kwargs))
        return self.proxied(*args, **kwargs)

    @property
    def future(self):
        return ApiMethodProxy(self.name + ".future", self.proxied.future)


@pytest.fixture
def ds_entity():
    keys = []
    client = datastore.Client()

    def make_entity(*key_args, **entity_kwargs):
        key = client.key(*key_args)
        assert client.get(key) is None
        entity = datastore.Entity(key=key)
        entity.update(entity_kwargs)
        client.put(entity)

        keys.append(key)
        return entity

    yield make_entity

    for key in keys:
        client.delete(key)


@pytest.fixture
def client_context():
    client = ndb.Client()
    with client.context():
        yield


@pytest.mark.usefixtures("client_context")
def test_retrieve_entity(ds_entity):
    entity_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity_id, foo=42, bar="none")

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    key = ndb.Key("SomeKind", entity_id)
    entity = key.get()
    assert isinstance(entity, SomeKind)
    assert entity.foo == 42
    assert entity.bar == "none"


@pytest.mark.usefixtures("client_context")
def test_retrieve_entity_not_found(ds_entity):
    entity_id = test_utils.system.unique_resource_id()

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    key = ndb.Key("SomeKind", entity_id)
    assert key.get() is None


@pytest.mark.usefixtures("client_context")
def test_nested_tasklet(ds_entity):
    entity_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity_id, foo=42, bar="none")

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    @ndb.tasklet
    def get_foo(key):
        entity = yield key.get_async()
        return entity.foo

    key = ndb.Key("SomeKind", entity_id)
    assert get_foo(key).result() == 42


@pytest.mark.usefixtures("client_context")
def test_retrieve_two_entities_in_parallel(ds_entity):
    entity1_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity1_id, foo=42, bar="none")
    entity2_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity2_id, foo=65, bar="naan")

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    key1 = ndb.Key("SomeKind", entity1_id)
    key2 = ndb.Key("SomeKind", entity2_id)

    @ndb.tasklet
    def get_two_entities():
        entity1, entity2 = yield key1.get_async(), key2.get_async()
        return entity1, entity2

    entity1, entity2 = get_two_entities().result()

    assert isinstance(entity1, SomeKind)
    assert entity1.foo == 42
    assert entity1.bar == "none"

    assert isinstance(entity2, SomeKind)
    assert entity2.foo == 65
    assert entity2.bar == "naan"


@pytest.mark.usefixtures("client_context")
def test_insert_entity():
    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    entity = SomeKind(foo=42, bar="none")
    key = entity.put()

    retrieved = key.get()
    assert retrieved.foo == 42
    assert retrieved.bar == "none"


@pytest.mark.usefixtures("client_context")
def test_update_entity(ds_entity):
    entity_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity_id, foo=42, bar="none")

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    key = ndb.Key("SomeKind", entity_id)
    entity = key.get()
    entity.foo = 56
    entity.bar = "high"
    assert entity.put() == key

    retrieved = key.get()
    assert retrieved.foo == 56
    assert retrieved.bar == "high"


@pytest.mark.usefixtures("client_context")
def test_insert_entity_in_transaction():
    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    def save_entity():
        entity = SomeKind(foo=42, bar="none")
        key = entity.put()
        return key

    key = ndb.transaction(save_entity)
    retrieved = key.get()
    assert retrieved.foo == 42
    assert retrieved.bar == "none"


@mock.patch("google.cloud.datastore_v1.proto.datastore_pb2_grpc.DatastoreStub",
            LoggingDatastoreStub)
@pytest.mark.usefixtures("client_context")
def test_update_datastore_entity_in_transaction(ds_entity):
    client = datastore.Client()

    # Create entity
    entity_id = test_utils.system.unique_resource_id()
    key = client.key("SomeKind", entity_id)
    assert client.get(key) is None
    entity = datastore.Entity(key=key)
    entity.update({"foo": 42, "bar": "none"})
    client.put(entity)

    with client.transaction():
        entity = client.get(key)
        entity.update({"foo": 56, "bar": "high"})
        client.put(entity)

    entity = client.get(key)
    assert entity["foo"] == 56
    assert entity["bar"] == "high"


@mock.patch("google.cloud.datastore_v1.proto.datastore_pb2_grpc.DatastoreStub",
            LoggingDatastoreStub)
@pytest.mark.usefixtures("client_context")
def test_update_entity_in_transaction(ds_entity):
    entity_id = test_utils.system.unique_resource_id()
    ds_entity("SomeKind", entity_id, foo=42, bar="none")
    time.sleep(2)

    class SomeKind(ndb.Model):
        foo = ndb.IntegerProperty()
        bar = ndb.StringProperty()

    def update_entity():
        key = ndb.Key("SomeKind", entity_id)
        entity = key.get()
        entity.foo = 56
        entity.bar = "high"
        assert entity.put() == key
        return key

    key = ndb.transaction(update_entity)
    retrieved = key.get()
    assert retrieved.foo == 56
    assert retrieved.bar == "high"


@pytest.mark.usefixtures("client_context")
def test_parallel_transactions():

    def task(delay):

        @ndb.tasklet
        def callback():
            transaction = _runstate.current().transaction
            yield ndb.sleep(delay)
            assert _runstate.current().transaction == transaction
            return transaction

        return callback

    future1 = ndb.transaction_async(task(0.1))
    future2 = ndb.transaction_async(task(0.06))
    ndb.wait_all((future1, future2))
    assert future1.get_result() != future2.get_result()
