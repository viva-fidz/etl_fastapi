import logging
import uuid
from datetime import datetime
from typing import Any, Generator, List, Optional, Tuple

import backoff
import psycopg
from elasticsearch.exceptions import ConnectionError


logger = logging.getLogger(__name__)


class PostgresExtractor:
    """
    Класс для извлечения данных из PostgreSQL
    с поддержкой состояния и восстановления.
    """

    def __init__(
        self,
        connection_params: dict,
        state: Any,
        batch_size: int = 100,
    ):
        self.connection_params = connection_params
        self.state = state
        self.batch_size = batch_size
        self.state_key = "last_position"

    @backoff.on_exception(
        backoff.expo,
        (psycopg.OperationalError, psycopg.InterfaceError, ConnectionError),
        max_tries=10,
        on_backoff=lambda details: logger.warning(
            "Повторная попытка подключения к БД "
            f"#{details['tries']} через {details['wait']:.2f} сек."
        )
    )
    def _get_connection(self):
        try:
            conn = psycopg.connect(**self.connection_params)
            logger.info("Успешное подключение к БД")
            return conn
        except psycopg.Error as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise ConnectionError(f"Не удалось подключиться к БД: {e}")

    def _get_last_position(self) -> Optional[Tuple[datetime, uuid.UUID]]:
        pos = self.state.get_state(self.state_key)
        if pos:
            try:
                return (
                    datetime.fromisoformat(pos["last_modified"]),
                    uuid.UUID(pos["last_id"])
                )
            except (ValueError, TypeError) as e:
                logger.info(
                    f"Неверный формат позиции в состоянии: {pos} ({e})",
                )
                return None
        return None

    def _save_last_position(self, modified: datetime, id_: uuid.UUID):
        self.state.set_state(self.state_key, {
            "last_modified": modified.isoformat(),
            "last_id": str(id_)
        })
        logger.debug(f"Сохранена позиция: {modified.isoformat()}, {id_}")

    @backoff.on_exception(
        backoff.expo,
        (psycopg.OperationalError, psycopg.InterfaceError),
        max_tries=5,
        on_backoff=lambda details: logger.warning(
            "Повторная попытка выполнения запроса "
            f"#{details['tries']} через {details['wait']:.2f} сек."
        )
    )
    def get_film_works_batch(
            self,
            last_position: Optional[Tuple[datetime, uuid.UUID]] = None,
    ) -> List[Tuple]:
        """
        Получает одну пачку фильмов.
        Возвращает список кортежей с данными фильмов.
        """

        query = """
        SELECT
           fw.id,
           fw.title,
           fw.description,
           fw.imdb_rating,
           fw.type,
           fw.created,
           GREATEST(
               fw.modified,
               COALESCE(MAX(p.modified), fw.modified),
               COALESCE(MAX(g.modified), fw.modified)
           ) as modified,
           COALESCE (
               json_agg(
                   DISTINCT jsonb_build_object(
                       'person_role', pfw.role,
                       'person_id', p.id,
                       'person_name', p.full_name
                   )
               ) FILTER (WHERE p.id is not null),
               '[]'
           ) as persons,
           COALESCE(
               JSON_AGG(
                   DISTINCT jsonb_build_object(
                       'id', g.id,
                       'name', g.name
                   )
               ) FILTER (WHERE g.id IS NOT NULL),
               '[]'::json
           ) as genres
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        GROUP BY fw.id, fw.title, fw.description, fw.imdb_rating, fw.type, fw.created, fw.modified
        """

        params = []
        if last_position:
            last_modified, last_id = last_position
            query += """
            HAVING (
                GREATEST(
                    fw.modified,
                    COALESCE(MAX(p.modified), fw.modified),
                    COALESCE(MAX(g.modified), fw.modified)
                ) > %s OR
                (
                    GREATEST(
                        fw.modified,
                        COALESCE(MAX(p.modified), fw.modified),
                        COALESCE(MAX(g.modified), fw.modified)
                    ) = %s AND fw.id > %s
                )
            )
            """
            params.extend([last_modified, last_modified, last_id])

        query += """
        ORDER BY modified, fw.id
        LIMIT %s;
        """
        params.append(self.batch_size)

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            logger.info(f"Получено {len(results)} записей из БД")
            return results
        except psycopg.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def extract_all_batches(self) -> Generator[List[Tuple], None, None]:
        """
        Генератор, который возвращает пачки фильмов одну за другой.
        Автоматически сохраняет состояние
        и продолжает с последней обработанной записи.
        """

        last_position = self._get_last_position()
        if last_position:
            logger.info(f"Продолжаем обработку с позиции: {last_position}")
        else:
            logger.info("Начинаем обработку с самого начала")

        while True:
            try:
                batch = self.get_film_works_batch(last_position)
                if not batch:
                    logger.info("Больше нет записей для обработки")
                    break
                yield batch
                if batch:
                    max_modified = None
                    max_id = None
                    for row in batch:
                        row_modified = row[6]
                        row_id = row[0]
                        if row_modified and (
                            max_modified is None or
                            row_modified > max_modified or
                            (row_modified == max_modified and row_id > max_id)
                        ):
                            max_modified = row_modified
                            max_id = row_id

                    if max_modified and max_id:
                        last_position = (max_modified, max_id)
                        self._save_last_position(max_modified, max_id)
                        logger.info(
                            "Обработана пачка, последняя позиция:"
                            f" {max_modified}, {max_id}",
                        )

                if len(batch) < self.batch_size:
                    logger.info("Достигнут конец данных")
                    break

            except Exception as e:
                logger.error(f"Ошибка при извлечении данных: {e}")
                raise

    def extract_incremental(self) -> Generator[List[Tuple], None, None]:
        """
        Извлекает новые и обновленные записи с момента последнего запуска.
        Если состояние не найдено, извлекает все записи.
        """

        last_position = self._get_last_position()
        if not last_position:
            logger.info("Нет сохраненного состояния, извлекаем все записи")
        else:
            logger.info(f"Инкрементальная синхронизация с {last_position}")
        yield from self.extract_all_batches()
