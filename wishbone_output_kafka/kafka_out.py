#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  kafka_out.py
#
#  Copyright 2016 Jelle Smet <development@smetj.net>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

from gevent import monkey; monkey.patch_all()
from pykafka import KafkaClient
from gevent.event import Event
from wishbone import Actor
from wishbone.event import Bulk
from gevent import sleep
from wishbone.utils import ModuleConnectionMock


# root = logging.getLogger()
# root.setLevel(logging.DEBUG)

# ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# root.addHandler(ch)


class KafkaOut(Actor):

    """**Publish data to Kafka.**

    Publish data to Kafka.


    Parameters:

        - hosts(array)(["localhost:9092"])
           |  The server to submit data to.

        - client_id(str)("wishbone")
            |  The client_id to use when connecting.

        - topic(str)("")*
           |  The topic to use when submitting messages.

        - partition(str)(None)*
           |  The partition to submit <selection> to.

        - selection(str)("@data")
           |  The part of the event to submit.

        - timeout(int)(10)
            |  Timeout submitting messages


    Queues:

        - inbox
           |  A description of the queue

    """

    def __init__(self, actor_config, hosts=["localhost:9092"], client_id="wishbone", topic="wishbone", partition=None, selection="@data", timeout=10):
        Actor.__init__(self, actor_config)

        self.pool.createQueue("inbox")
        self.registerConsumer(self.consume, "inbox")
        self.setup_connection = Event()
        self.kafka = None
        self.producer = ModuleConnectionMock("Kafka connection not yet established.")
        self.client = ModuleConnectionMock("Kafka connection not yet established.")
        self.topic_producers = {}

    def preHook(self):

        self.setup_connection.set()
        self.sendToBackground(self.setupConnection)

    def consume(self, event):

        try:
            if isinstance(event, Bulk):
                for event in event.dumpFieldAsList(self.kwargs.selection):
                    self.__sendOne(event)
            else:
                self.__sendOne(event)
        except Exception:
            self.setup_connection.set()
            raise

    def __sendOne(self, event):

        data = str(event.get(self.kwargs.selection)).encode()
        try:
            self.getTopicProducer(self.kwargs.topic).produce(data)
        except Exception as err:
            self.setup_connection.set()
            raise Exception("Failed to send message to broker.  Reason: %s" % (err))

    def setupConnection(self):
        servers = ",".join(self.kwargs.hosts)
        while self.loop():
            self.setup_connection.wait()
            while self.loop():
                try:
                    self.client = KafkaClient(hosts=servers, use_greenlets=True)
                    for topic, o in self.topic_producers.items():
                        self.getTopicProducer(topic)
                    self.setup_connection.clear()
                    self.logging.info("Connected to broker.")
                    break
                except Exception as err:
                    self.logging.error("Failed to connect to broker.  Will retry in 1 second.  Reason: %s" % (err))
                    sleep(1)

    def getTopicProducer(self, topic):

        try:
            return self.topic_producers[topic]
        except KeyError:
            self.topic_producers[topic] = self.client.topics[topic].get_sync_producer()
            return self.topic_producers[topic]

