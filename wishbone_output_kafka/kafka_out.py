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
from gevent.event import Event
from kafka import KafkaProducer
from wishbone import Actor
from wishbone.event import Bulk
from gevent import sleep


class MockProducer(object):

    def __init__(self, logging):
        self.logging = logging

    def send(self, *args, **kwargs):

        self.logging.warning("Connection not initialized yet.")


class KafkaOut(Actor):

    """**Publish data to Kafka.**

    Publish data to Kafka.


    Parameters:

        - boostrap_servers(array)(["localhost:9092"])
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

    def __init__(self, actor_config, bootstrap_servers=["localhost:9092"], client_id="wishbone", topic="wishbone", partition=None, selection="@data", timeout=10):
        Actor.__init__(self, actor_config)

        self.pool.createQueue("inbox")
        self.registerConsumer(self.consume, "inbox")
        self.connecting = Event()
        self.kafka = None
        self.producer = None

    def preHook(self):

        self.connecting.set()
        self.sendToBackground(self.setupConnection)

    def consume(self, event):

        try:
            if isinstance(event, Bulk):
                self.__sendBulk(event)
            else:
                self.__sendOne(event)
        except Exception:
            self.connecting.set()
            raise

    def __sendOne(self, event):

        if self.producer is not None:
            data = str(event.get(self.kwargs.selection)).encode()
            reply = self.producer.send(self.kwargs.topic, data).get(self.kwargs.timeout)
            event.set(reply.partition, "@tmp.%s.partition" % (self.name))
            event.set(reply.offset, "@tmp.%s.offset" % (self.name))

    def __sendBulk(self, event):

        for item in event.dumpFieldAsList(self.kwargs.selection):
            data = str(event.get(self.kwargs.selection).encode())
            self.producer.send(self.kwargs.topic, data)
        self.producer.flush()

    def setupConnection(self):

        while self.loop():
            self.connecting.wait()
            while self.loop():
                try:
                    self.producer = KafkaProducer(bootstrap_servers=self.kwargs.bootstrap_servers)
                    self.logging.info("Connected to Kafka on %s:%s." % (list(self.producer._metadata.brokers())[0].host, list(self.producer._metadata.brokers())[0].port))
                    self.connecting.clear()
                    break
                except Exception as err:
                    self.logging.error("Failed to connect to Kafka.  Reason: %s" % (err))
                    sleep(1)
