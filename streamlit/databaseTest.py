from sqlalchemy import create_engine

# Writable engine (no mode=ro / immutable)

engine = create_engine(
    "sqlite+pysqlite:////Users/rolandsrepetto/db_kg_exp/streamlit/netflixdb.sqlite"
)

with engine.begin() as conn:
    conn.exec_driver_sql("PRAGMA foreign_keys = ON;")

    # --- season migration ---
    conn.exec_driver_sql(
        """
    CREATE TABLE season_new (
        release_date date,
        season_number integer,
        created_date timestamp NOT NULL,
        id integer PRIMARY KEY,
        modified_date timestamp NOT NULL,
        runtime bigint,
        tv_show_id bigint,
        original_title varchar(255),
        title varchar(255) NOT NULL,
        FOREIGN KEY (tv_show_id) REFERENCES tv_show(id)
            ON DELETE SET NULL
            ON UPDATE NO ACTION
    );
    """
    )
    conn.exec_driver_sql(
        """
    INSERT INTO season_new (
        release_date, season_number, created_date, id, modified_date,
        runtime, tv_show_id, original_title, title
    )
    SELECT
        s.release_date, s.season_number, s.created_date, s.id, s.modified_date,
        s.runtime,
        CASE
            WHEN s.tv_show_id IS NULL THEN NULL
            WHEN EXISTS (SELECT 1 FROM tv_show t WHERE t.id = s.tv_show_id)
                 THEN s.tv_show_id
            ELSE NULL
        END,
        s.original_title, s.title
    FROM season s;
    """
    )
    conn.exec_driver_sql("DROP TABLE season;")
    conn.exec_driver_sql("ALTER TABLE season_new RENAME TO season;")
    conn.exec_driver_sql("CREATE INDEX idx_season_tv_show_id ON season(tv_show_id);")

    # --- view_summary migration ---
    conn.exec_driver_sql(
        """
    CREATE TABLE view_summary_new (
        cumulative_weeks_in_top10 integer,
        end_date date NOT NULL,
        hours_viewed integer NOT NULL,
        start_date date NOT NULL,
        view_rank integer,
        views integer,
        created_date timestamp NOT NULL,
        id integer PRIMARY KEY,
        modified_date timestamp NOT NULL,
        movie_id bigint,
        season_id bigint,
        duration varchar(20) NOT NULL CHECK (duration IN ('WEEKLY','SEMI_ANNUALLY')),
        FOREIGN KEY (movie_id) REFERENCES movie(id)
            ON DELETE SET NULL
            ON UPDATE NO ACTION,
        FOREIGN KEY (season_id) REFERENCES season(id)
            ON DELETE SET NULL
            ON UPDATE NO ACTION
    );
    """
    )
    conn.exec_driver_sql(
        """
    INSERT INTO view_summary_new (
        cumulative_weeks_in_top10, end_date, hours_viewed, start_date, view_rank,
        views, created_date, id, modified_date, movie_id, season_id, duration
    )
    SELECT
        v.cumulative_weeks_in_top10, v.end_date, v.hours_viewed, v.start_date, v.view_rank,
        v.views, v.created_date, v.id, v.modified_date,
        CASE
            WHEN v.movie_id IS NULL THEN NULL
            WHEN EXISTS (SELECT 1 FROM movie m WHERE m.id = v.movie_id)
                 THEN v.movie_id
            ELSE NULL
        END,
        CASE
            WHEN v.season_id IS NULL THEN NULL
            WHEN EXISTS (SELECT 1 FROM season s WHERE s.id = v.season_id)
                 THEN v.season_id
            ELSE NULL
        END,
        v.duration
    FROM view_summary v;
    """
    )
    conn.exec_driver_sql("DROP TABLE view_summary;")
    conn.exec_driver_sql("ALTER TABLE view_summary_new RENAME TO view_summary;")
    conn.exec_driver_sql(
        "CREATE INDEX idx_view_summary_movie_id  ON view_summary(movie_id);"
    )
    conn.exec_driver_sql(
        "CREATE INDEX idx_view_summary_season_id ON view_summary(season_id);"
    )
