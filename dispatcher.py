#!/usr/bin/env python3

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import pika
import yaml
from serial import Serial
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Reader = TypeVar("Reader")

STATUS_OK = "OK"
STATUS_ERR = "ERR"

Base = declarative_base()


class Log(Base):
    """Log every operation carried out by the dispatcher."""

    __tablename__ = "log"
    id = Column(Integer, primary_key=True)
    node = Column(String, nullable=False)
    status = Column(String, nullable=False)
    message = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class Dispatcher:
    """Class to dispatch commands to a device reader.

    Configuration is read from YAML file from PLACQS_CONFIG.
    The messages are read from rabbitmq and are dispatched to
    the appropiate reader. A postgresql session is created
    and passed to the reader to be able to store the information they need.

    The DB schemas are defined by the reader.

    Attributes:
      config: Dictionary containing the parsed YAML file.
      uptime: UTC timestamp when the reader was deployed.
      reader: Reader object to handle the devices.
    """

    def __init__(self, reader: Reader) -> None:
        try:
            config_path = os.environ["PLACQS_CONFIG"]
            with open(config_path, "r") as config_fd:
                self.config = yaml.safe_load(config_fd.read())
        except KeyError:
            print(f"PLACQS_CONFIG is not defined")
            sys.exit(1)

        except yaml.scanner.ScannerError as e:
            print(f"Yaml config file is malformed: {e}")
            sys.exit(1)

        self.uptime = datetime.utcnow()
        self.reader = reader

        # Configuration of postgres
        psql_conf = self.config["postgresql"]
        credentials = f"{psql_conf['username']}:{psql_conf['password']}"
        engine = create_engine(
            f"postgres://{credentials}@localhost:5432/{psql_conf['database']}"
        )

        Session = sessionmaker(engine)
        self.db_session = Session()
        Base.metadata.create_all(engine)

        # Configuration of rabbitmq
        rabbit_conf = self.config["rabbitmq"]
        self.node_name = rabbit_conf["node_name"]

        self.queue_commands_name = rabbit_conf["queue_commands"]

        credentials = pika.PlainCredentials(
            username=rabbit_conf["username"],
            password=rabbit_conf["password"],
        )
        self.con = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", 5672, "/", credentials)
        )
        self.channel = self.con.channel()
        self.channel.exchange_declare(
            exchange=self.queue_commands_name, exchange_type="topic", durable=False
        )
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_commands = result.method.queue
        self.channel.queue_bind(
            exchange=self.queue_commands_name,
            queue=self.queue_commands,
            routing_key=rabbit_conf["node_name"],
        )

    def log(self, level: str, message: str):
        """Log a message with a severity.

        Args:
            level: Severity of the message.
            message: Message to log.

        Returns:
            None
        """
        self.db_session.add(Log(node=self.node_name, status=level, message=message))
        self.db_session.commit()

    def handle_message(self, _ch, method_frame, _properties, body):
        """Handle a message received on rabbitmq

        Args:
            _ch: Rabbitmq channel.
            _method_frame: Rabbitmq message header.
            _properties: Properties of the message received.
            body: The message to be handled.

        Returns:
            None
        """
        try:
            message = json.loads(body.decode("utf8"))
        except Exception as e:
            self.log("WARNING", f"Could not decode message: {e}")
            return None

        try:
            method = getattr(self.reader, f'handle_{message["method"].lower()}', None)
            if method is None:
                self.log(
                    "WARNING",
                    f"Reader {self.node_name} does not implement method handle_{message['method']}",
                )
                return None
        except AttributeError:
            self.log("WARNING", f"Method is not defined")
            return None

        try:
            message["db_session"] = self.db_session
            res = method(message)
        except Exception as e:
            self.log("CRITICAL", f"python error - {e}")
            return None

        self.log(
            res["status"], f'{message["method"].upper()} - {method_frame.consumer_tag}'
        )

        if res.get("status", None) == STATUS_ERR:
            msg = f'{message["method"].upper()} - {res.get("message", "")} - {method_frame.consumer_tag}'
            self.log(res.get("level", "WARNING"), msg)

        # Just for debugging purposes
        if res.get("data", None):
            print(json.dumps(res["data"]))

        self.db_session.commit()

    def run(self):
        """Start listening and handling messages from rabbitmq.

        Args:
            None

        Returns:
            None
        """
        self.log("INFO", f"Reader added on {self.node_name}")
        self.channel.basic_consume(
            queue=self.queue_commands,
            on_message_callback=self.handle_message,
            auto_ack=True,
        )
        self.channel.start_consuming()
        self.con.close()
