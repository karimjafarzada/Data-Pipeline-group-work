"""Checks that the test database fixture itself works."""


def test_empty_db_is_a_fresh_database(empty_db):
    name = empty_db.execute("SELECT current_database()").fetchone()[0]
    tables = empty_db.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchone()[0]

    assert name.startswith("pipeline_test_")
    assert tables == 0
