# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# - Timothy Hnat <twhnat@memphis.edu>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import datetime
import uuid
from typing import List
from uuid import UUID

from datetime import datetime
from typing import Any
import sys
import json
import codecs
import gzip

class DataPoint:
    def __init__(self,
                 start_time: datetime = None,
                 end_time: datetime = None,
                 sample: Any = None):
        self._start_time = start_time
        self._end_time = end_time
        self._sample = sample

    @property
    def sample(self):
        return self._sample

    @sample.setter
    def sample(self, val):
        self._sample = val

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @classmethod
    def from_tuple(cls, start_time: datetime, sample: Any, end_time: datetime = None):
        return cls(start_time, end_time, sample)

    def __str__(self):
        return str(self.start_time) + " - " + str(self.sample)

    def __repr__(self):
        return 'DataPoint(' + ', '.join(map(str, [self.start_time, self.end_time, self.sample]))


class DataStream:
    def __init__(self,
                 identifier: UUID = None,
                 owner: UUID = None,
                 name: UUID = None,
                 data_descriptor = [],
                 execution_context = {},
                 annotations: List = [],
                 stream_type: str = "1",
                 start_time: datetime = None,
                 end_time: datetime = None,
                 data: List[DataPoint] = None
                 ):
        self._identifier = identifier
        self._owner = owner
        self._name = name
        self._data_descriptor = data_descriptor
        self._datastream_type = stream_type
        self._execution_context = execution_context
        self._annotations = annotations
        self._start_time = start_time
        self._end_time = end_time
        self._data = data


    @property
    def identifier(self):
        return self._identifier

    @property
    def owner(self):
        return self._owner

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def start_time(self):
        return self._start_time

    @start_time.setter
    def start_time(self, val):
        self._start_time = val

    @property
    def end_time(self):
        return self._end_time

    @end_time.setter
    def end_time(self, val):
        self._end_time = val

    @property
    def datastream_type(self):
        return self._datastream_type

    @property
    def data(self):
        return self._data

    def __str__(self):
        return str(self.identifier) + " - " + str(self.owner) + " - " + str(self.data)

    def __repr__(self):
        result = "Stream(" + ', '.join(map(str, [self.identifier,
                                                 self.owner,
                                                 self.name,
                                                 self.data_descriptor,
                                                 self.datastream_type,
                                                 self.execution_context,
                                                 self.annotations]))
        return result

def convert_sample(sample):
    return list([float(x.strip()) for x in sample.split(',')])


def line_parser(input):
    ts, offset, sample = input.split(',', 2)
    start_time = int(ts) / 1000.0
    offset = int(offset)
    return DataPoint(datetime.fromtimestamp(start_time), convert_sample(sample))


def load_datastream(filebase):
    metadata = {}
    with codecs.open(filebase + '.json', encoding='utf-8', errors='ignore') as f:
        metadata = json.loads(f.read())

    fp = gzip.open(filebase + '.gz')
    gzip_file_content = fp.read()
    fp.close()
    gzip_file_content = gzip_file_content.decode('utf-8')

    lines = gzip_file_content.splitlines()
    data = list(map(line_parser, lines))

    identifier = uuid.UUID(metadata['identifier'])
    owner = uuid.UUID(metadata['owner'])
    name = metadata['name']
    data_descriptor = metadata['data_descriptor']
    execution_context = metadata['execution_context']
    annotations = metadata['annotations']
    stream_type = "1"
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier,owner,name,
    data_descriptor,
    execution_context,
    annotations,
    stream_type,
    start_time,
    end_time,
    data)


def save_datastream(datastream):
    print(datastream)
    print(datastream.data)
    pass


def count(datastream):

    identifier = uuid.uuid1()
    name = datastream.name + '--COUNT'
    execution_context = {}
    annotations = {}
    data_descriptor = []


    data = [DataPoint(datastream.data[0].start_time, datastream.data[-1].start_time, len(datastream.data))]
    start_time = data[0].start_time
    end_time = data[-1].start_time

    return DataStream(identifier, datastream.owner, name, data_descriptor,
    execution_context,
    annotations,
    "1",
    start_time,
    end_time,
    data)

if __name__ == '__main__':

    datastream = load_datastream(sys.argv[1])
    number_entries = count(datastream)
    save_datastream(number_entries)
