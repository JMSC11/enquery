import traceback

from django.conf import settings

from rest_framework import serializers

from mas_con_menos.bridge_mcm.enquiries import connection

CDGEM = settings.MCM_CDGEM


def auth(codigo, clave):
    """Petición para obtener los detalles del usuario"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            "SELECT DISTINCT CDGCO,"
            " (SELECT NOMBRE FROM CO WHERE CDGEM = PE.CDGEM AND CODIGO = PE.CDGCO) NOMBRE, "
            "  NOMBREC(PE.CDGEM, PE.CODIGO, 'G', 'N', PE.NOMBRE1, PE.NOMBRE2, PE.PRIMAPE, PE.SEGAPE)"
            " AS EJECUTIVO, PE.PUESTO, PE.ACTIVO, PE.BLOQUEO, UT.CDGTUS PERFIL, UT.MODULO "
            " FROM PE, UT "
            f" WHERE PE.CDGEM = '{CDGEM}' AND PE.CODIGO = '{codigo}'"
            " AND PE.CODIGO = UT.CDGPE "
            # " AND PE.CLAVE = CODIFICA('{clave}')"
        )
        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        details = {}

        for row in list:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            details = {
                "CDGCO": items[0],
                "NOMBRE": items[1],
                "EJECUTIVO": items[2],
                "PUESTO": items[3],
                "ACTIVO": items[4],
                "BLOQUEO": items[5],
                "PERFIL": items[6],
                "codigo": codigo,
            }

        cursor.close()

        return details

    except Exception as e:
        print(e)
        print(traceback.format_exc())
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()


def user_detail(codigo):
    """Petición para obtener los detalles del usuario"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            "SELECT CDGCO, CO.NOMBRE, NOMBREC(PE.CDGEM, PE.CODIGO, 'G', 'N', NOMBRE1, NOMBRE2, PRIMAPE, SEGAPE)"
            " AS EJECUTIVO, PUESTO, ACTIVO, BLOQUEO"
            " FROM PE,CO WHERE PE.CDGEM = CO.CDGEM AND PE.CDGCO = CO.CODIGO AND "
            f" PE.CDGEM = '{CDGEM}' AND PE.CODIGO = '{codigo}'"
            # "AND PE.CLAVE = CODIFICA('{clave}')"
        )
        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        details = {}

        for row in list:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            details = {
                "CDGCO": items[0],
                "NOMBRE": items[1],
                "EJECUTIVO": items[2],
                "PUESTO": items[3],
                "ACTIVO": items[4],
                "BLOQUEO": items[5],
                "codigo": codigo,
            }

        cursor.close()

        return details

    except Exception as e:
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()
