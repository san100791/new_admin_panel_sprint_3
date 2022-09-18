"""
SQL-запрос для выборки данных о фильмах
"""

query_filmfork_ids = """
    SELECT
        fw.id, 
        fw.title,
        fw.description,
        fw.rating,
        COALESCE (
            json_agg(
                DISTINCT jsonb_build_object (
                    'person_role', pfw.role,
                    'person_id', p.id,
                    'person_name', p.full_name
                )
            )
        ) AS persons,
        array_agg(DISTINCT g.name) AS genres,
        fw.updated_at 
    FROM content.film_work AS fw
    LEFT JOIN content.person_film_work AS pfw
    ON pfw.film_work_id = fw.id
    LEFT JOIN content.person AS p
    ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work AS gfw
    ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre AS g
    ON g.id = gfw.genre_id
    WHERE GREATEST(fw.updated_at, g.updated_at, p.updated_at) > %s
    GROUP BY fw.id
    ORDER BY fw.updated_at
"""