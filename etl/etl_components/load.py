import os
from pathlib import Path

import json
import logging
from typing import Any, Dict, List

import backoff
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError, TransportError


logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    """
    Loader для загрузки данных в ES с созданием индекса из JSON, если его нет.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        index_name: str = "movies",
        id_field: str = "id",
        mapping_file: str = os.path.join(
            str(Path(__file__).parent.parent / "utils" / "movies_index.json"),
        )
    ):
        self.host = host
        self.port = port
        self.index_name = index_name
        self.id_field = id_field
        self.es: Elasticsearch = None
        try:
            with open(mapping_file, "r") as f:
                self.index_config = json.load(f)
            logger.info(f"Загружена конфигурация индекса из {mapping_file}")
        except FileNotFoundError:
            raise ValueError(
                f"Файл с конфигурацией индекса не найден: {mapping_file}",
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка парсинга JSON в {mapping_file}: {e}")
        self.connect()
        self._ensure_index_exists()

    @backoff.on_exception(backoff.expo, ConnectionError, max_time=60)
    def connect(self):
        self.es = Elasticsearch(f"http://{self.host}:{self.port}")
        if not self.es.ping():
            raise ConnectionError("Не удалось подключиться к Elasticsearch")
        logger.info("Успешное подключение к Elasticsearch")

    def _ensure_index_exists(self):
        """Проверяет существование индекса и создаёт, если его нет."""

        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(
                index=self.index_name,
                body=self.index_config,
            )
            logger.info(
                f"Создан индекс {self.index_name} с конфигурацией из JSON",
            )
        else:
            logger.info(f"Индекс {self.index_name} уже существует")

    def _prepare_batch(self, docs: List[Dict[str, Any]]) -> List[Dict]:
        """
        Подготавливает документы для bulk-загрузки (добавляет _index и _id).
        """

        actions = []
        for doc_dict in docs:
            action = {"_index": self.index_name, "_source": doc_dict}
            doc_id = doc_dict.get(self.id_field)
            if doc_id:
                action["_id"] = str(doc_id)
            actions.append(action)
        return actions

    @backoff.on_exception(
        backoff.expo, (ConnectionError, TransportError), max_time=120,
    )
    def _bulk(self, actions: List[Dict]) -> tuple:
        success, errors = helpers.bulk(self.es, actions)
        return success, errors

    def load(self, docs: List[Dict[str, Any]]) -> Dict:
        if not docs:
            return {"success": True, "indexed": 0, "errors": []}
        actions = self._prepare_batch(docs)
        success, errors = self._bulk(actions)
        logger.info(f"Загружено {success} документов в {self.index_name}")
        return {"success": True, "indexed": success, "errors": errors or []}
