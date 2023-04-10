import io
import os
import json
import logging

import cv2
import numpy as np
import torch
import time

import pika
from tenacity import retry, before_log, wait_fixed, \
    retry_if_not_exception_type, before_sleep_log


rabbit_user = os.getenv("RABBIT_USER")
rabbit_passwd = os.getenv("RABBIT_PASSWORD")

rabbit_url = f'amqp://{rabbit_user}:{rabbit_passwd}@10.14.244.26:5672/master'


class Yolo:

    def __init__(self):
        self.model = torch.hub.load("ultralytics/yolov5",
                                    "custom", path='best.pt',
                                    force_reload=True)

    def _predict(self, image):
        im = cv2.imread(image)
        outputs = self.model(im)
        buffer = outputs.ims[0]
        bio = io.BytesIO(buffer)
        bio.name = "image.jpeg"
        bio.seek(0)
        text = sorted(set(np.array(outputs)), reverse=True)
        return bio, text

    def __call__(self, image: str):
        answer = self._predict(image)
        return answer


class RabbitConnector:
    """
    Class processing input messages RabbitMW and them sending in output queue.

    """

    def __init__(
            self,
            url: str = None,
            model=None,
            input_queue: str = None,
            output_queue: str = None
    ):
        self.url = url
        self.model = model
        self.input_queue = input_queue
        self.output_queue = output_queue

    def _create_success_json(
            self,
            label: str = None,
            confidence: float = None
    ) -> json:
        """
        Create success json-file.
        """
        json_message = json.dumps(
            {
                "state": "success",
                "result": {
                    "submarker_id": self.submarker_ids[label],
                    "confidence": f"{round(confidence * 100, 2)}%",
                },
            },
            ensure_ascii=False,
        )

        return json_message

    def _create_error_json(
            self,
            exception: str = None
    ) -> json:
        """
        Create error json-file.
        """
        json_message = json.dumps(
            {
                "state": "error",
                "result": {
                    "Exception": exception
                },
            },
            ensure_ascii=False,
        )

        return json_message

    def _listen_queue(self, channel: pika):
        """
        Method listen queue
        """
        logging.info("Подключение к серверу RabbitMQ успешно выполнено.")
        count = -1
        delivery_mode = 2

        def callback(ch, method, properties, body):
            start_time = time.monotonic()
            nonlocal count, delivery_mode
            count += 1

            correlation_id = properties.correlation_id

            if self.output_queue is not None:
                reply_to = self.output_queue
            else:
                reply_to = properties.reply_to
            priority = properties.priority

            if priority is None:
                priority = 20

            if (reply_to is not None) and (correlation_id is not None):
                """
                Блок проверки входного сообщения.
                """
                try:
                    d = json.loads(body)
                    text = str(d["data"]["text"])
                    try:
                        label, confidence = self.model(text=text)
                    except Exception as e:
                        logging.error(
                            f"Ошибка при обработки сообщения. Причина {e}"
                        )
                        message = self._create_error_json(exception=e)
                    else:
                        logging.exception("Невалидный формат JSON файла.")
                        message = self._create_error_json(
                            exception="Невалидный формат JSON файла."
                        )
                except Exception as e:
                    logging.exception(e)
                    message = self._create_error_json(exception=str(e))

                """
                Отправка ответа в выходную очередь.
                """
                channel.basic_publish(
                    properties=pika.BasicProperties(
                        correlation_id=str(123),
                        priority=120,
                        delivery_mode=2
                    ),
                    body= (label, confidence),
                    exchange='',
                    routing_key="bot_output",
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                end_time = time.monotonic()
                logging.debug(
                    f"""
                    Сообщение обработано. Correlation_id: {correlation_id}. 
                    Обработка сообщения заняла {end_time - start_time} секунд.
                    """
                )
            else:
                logging.exception(
                    "Неверно указаны correlation_id и(или) reply_to."
                )

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.input_queue,
            on_message_callback=callback,
        )

        try:
            channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            pass

@retry(retry=retry_if_not_exception_type(
    (AttributeError, KeyboardInterrupt, TypeError)),
    wait=wait_fixed(5),
    before=before_log(logging, logging.INFO),
    before_sleep=before_sleep_log(logging, logging.INFO))
def rabbit_connection(self, ):
    """
    Connection to Rabbit MQ
        url (str): URL of the server Rabbit MQ
        mode (PipelineClsClustering): Object of the class model / pipeline
        input_queue (str): Name input queue RabbitMQ
    """

    try:
        url_pika = pika.URLParameters(self.url)
        params = pika.ConnectionParameters(
            heartbeat=600,
            blocked_connection_timeout=300,
            host=url_pika.host,
            port=url_pika.port,
            virtual_host=url_pika.virtual_host,
            credentials=url_pika.credentials
        )
        connection = pika.BlockingConnection(params)
        rabbit_channel = connection.channel()
        self._listen_queue(channel=rabbit_channel)

    except Exception as e:
        logging.critical(
            f"Не удалось подключиться к серверу RabbitMQ. Причина: {e}"
        )
