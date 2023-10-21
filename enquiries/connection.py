from django.conf import settings

import cx_Oracle

if settings.ORACLE_PATH != "":
    try:
        print(settings.ORACLE_PATH)
        cx_Oracle.init_oracle_client(lib_dir=settings.ORACLE_PATH)
    except Exception as e:
        print(e)


def connect():
    # dsn_tns = cx_Oracle.makedsn(
    #     settings.ORACLE_HOST, settings.ORACLE_PORT, service_name=settings.ORACLE_DB
    # )
    # connection = cx_Oracle.connect(
    #     user=settings.ORACLE_USER, password=settings.ORACLE_PASSWORD, dsn=dsn_tns,
    # )

    dsn = f"""(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={settings.ORACLE_HOST})(PORT={settings.ORACLE_PORT}))
    (CONNECT_DATA=(SERVICE_NAME={settings.ORACLE_DB})))"""
    conn = cx_Oracle.connect(
        user=settings.ORACLE_USER,
        password=settings.ORACLE_PASSWORD,
        dsn=dsn,
        encoding="UTF-8",
    )

    return conn
