"""Tests for the migration runner, using small throwaway migration files."""

import psycopg
import pytest

from pipeline.migrate import apply_migrations, find_migrations


def write(directory, name, sql):
    (directory / name).write_text(sql, encoding="utf-8")


def table_exists(conn, qualified_name):
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified_name,)).fetchone()[0]


def recorded(conn):
    return [
        row[0]
        for row in conn.execute("SELECT filename FROM ops.schema_migrations ORDER BY filename")
    ]


def test_find_migrations_sorts_by_timestamp(tmp_path):
    write(tmp_path, "20261126120500_second.sql", "")
    write(tmp_path, "20261126120000_first.sql", "")

    names = [p.name for p in find_migrations(tmp_path)]

    assert names == ["20261126120000_first.sql", "20261126120500_second.sql"]


@pytest.mark.parametrize("bad_name", ["0002_bronze.sql", "20261126_bronze.sql", "20261126120000-Bronze.sql"])
def test_find_migrations_rejects_names_without_timestamp(tmp_path, bad_name):
    write(tmp_path, bad_name, "")

    with pytest.raises(ValueError, match=bad_name):
        find_migrations(tmp_path)


def test_applies_files_in_timestamp_order_and_records_them(empty_db, tmp_path):
    # The second file depends on the first, so the order matters.
    write(tmp_path, "20261126120100_add_column.sql", "ALTER TABLE demo.items ADD COLUMN name TEXT;")
    write(
        tmp_path,
        "20261126120000_create_table.sql",
        "CREATE SCHEMA demo; CREATE TABLE demo.items (id INT PRIMARY KEY);",
    )

    applied = apply_migrations(empty_db, tmp_path)

    assert applied == ["20261126120000_create_table.sql", "20261126120100_add_column.sql"]
    assert recorded(empty_db) == applied
    empty_db.execute("INSERT INTO demo.items (id, name) VALUES (1, 'ok')")


def test_running_twice_applies_nothing_the_second_time(empty_db, tmp_path):
    write(tmp_path, "20261126120000_create_table.sql", "CREATE TABLE items (id INT);")

    apply_migrations(empty_db, tmp_path)
    second_run = apply_migrations(empty_db, tmp_path)

    assert second_run == []
    assert recorded(empty_db) == ["20261126120000_create_table.sql"]


def test_only_new_files_are_applied(empty_db, tmp_path):
    write(tmp_path, "20261126120000_first.sql", "CREATE TABLE first (id INT);")
    apply_migrations(empty_db, tmp_path)

    write(tmp_path, "20261126130000_second.sql", "CREATE TABLE second (id INT);")
    applied = apply_migrations(empty_db, tmp_path)

    assert applied == ["20261126130000_second.sql"]


def test_failed_migration_is_rolled_back_and_not_recorded(empty_db, tmp_path):
    write(tmp_path, "20261126120000_good.sql", "CREATE TABLE good (id INT);")
    write(
        tmp_path,
        "20261126120100_broken.sql",
        "CREATE TABLE half_done (id INT); SELECT * FROM no_such_table;",
    )

    with pytest.raises(psycopg.errors.UndefinedTable):
        apply_migrations(empty_db, tmp_path)

    assert recorded(empty_db) == ["20261126120000_good.sql"]
    assert table_exists(empty_db, "good")
    assert not table_exists(empty_db, "half_done")
