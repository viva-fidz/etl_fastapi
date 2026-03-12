import sys, os
import json
import logging
from typing import Any, Dict, List, Tuple
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from es_models import FilmES, PersonName


logger = logging.getLogger(__name__)


class DataTransform:
    """Класс для преобразования данных из Postgres в Elasticsearch."""

    def transform_batch(self, batch: List[Tuple]) -> List[Dict[str, Any]]:
        documents: List[Dict[str, Any]] = []
        for row in batch:
            (
                film_id,
                title,
                description,
                rating,
                type_,
                created,
                modified,
                persons_raw,
                genres_list,
            ) = row

            persons = []
            if persons_raw and persons_raw != "[]":
                if isinstance(persons_raw, str):
                    persons_data = json.loads(persons_raw)
                else:
                    persons_data = persons_raw
                for p in persons_data:
                    if (
                        p.get("person_id") and
                        p.get("person_name") and
                        p.get("person_role")
                    ):
                        persons.append({
                            "id": str(p["person_id"]),
                            "name": p["person_name"],
                            "role": p["person_role"],
                        })

            actors = [PersonName(
                id=person["id"], name=person["name"],
            ) for person in persons if person["role"] == "actor"]

            directors = [PersonName(
                id=person["id"], name=person["name"],
            ) for person in persons if person["role"] == "director"]

            writers = [PersonName(
                id=person["id"], name=person["name"],
            ) for person in persons if person["role"] == "writer"]

            film_es = FilmES(
                id=str(film_id),
                imdb_rating=rating,
                genres=genres_list or [],
                title=title,
                description=description,
                directors_names=[d.name for d in directors],
                actors_names=[a.name for a in actors],
                writers_names=[w.name for w in writers],
                directors=directors,
                actors=actors,
                writers=writers,
            )

            documents.append(film_es.model_dump())
        return documents
