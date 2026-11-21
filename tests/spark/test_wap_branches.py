"""Iceberg behaviour the write-audit-publish job relies on, on an isolated file catalog."""


def test_branch_write_is_invisible_until_fast_forward(spark):
    t = "test.db.wap"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} (id INT, v STRING) USING iceberg TBLPROPERTIES ('write.wap.enabled'='true')")
    spark.sql(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b')")
    spark.sql(f"ALTER TABLE {t} CREATE BRANCH audit")
    spark.conf.set("spark.wap.branch", "audit")
    spark.sql(f"INSERT INTO {t} VALUES (3, 'c')")
    spark.conf.unset("spark.wap.branch")
    assert spark.sql(f"SELECT count(*) FROM {t}").first()[0] == 2
    assert spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF 'audit'").first()[0] == 3
    spark.sql(f"CALL test.system.fast_forward('db.wap', 'main', 'audit')")
    assert spark.sql(f"SELECT count(*) FROM {t}").first()[0] == 3


def test_tag_reads_old_schema_after_drop_and_readd(spark):
    t = "test.db.evo"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} (id INT, flag BOOLEAN) USING iceberg")
    spark.sql(f"INSERT INTO {t} VALUES (1, true), (2, false)")
    spark.sql(f"ALTER TABLE {t} CREATE TAG v1")
    spark.sql(f"ALTER TABLE {t} DROP COLUMN flag")
    spark.sql(f"ALTER TABLE {t} ADD COLUMN flag BOOLEAN")
    # columns are tracked by id: the re-added column does not resurrect old values
    assert spark.sql(f"SELECT count(*) FROM {t} WHERE flag IS NOT NULL").first()[0] == 0
    assert spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF 'v1' WHERE flag").first()[0] == 1


def test_rollback_restores_deleted_rows(spark):
    t = "test.db.rb"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(f"CREATE TABLE {t} (id INT) USING iceberg")
    spark.sql(f"INSERT INTO {t} VALUES (1), (2), (3)")
    good = spark.sql(f"SELECT snapshot_id FROM {t}.refs WHERE name = 'main'").first()[0]
    spark.sql(f"DELETE FROM {t} WHERE id > 1")
    spark.sql(f"CALL test.system.rollback_to_snapshot('db.rb', {good})")
    assert spark.sql(f"SELECT count(*) FROM {t}").first()[0] == 3
