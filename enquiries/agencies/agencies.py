from django.conf import settings
from mas_con_menos.bridge_mcm.enquiries import connection
from mas_con_menos.agencies.models.agency import Agency
"""from mas_con_menos.executive.models import Executive"""

CDGEM = settings.MCM_CDGEM
def get_agencies(codigo_usuario, fecha_inicio, fecha_final):
    """Petición para obtener la lista de Agencias"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()

        sql_sentence = (
            "SELECT "
            "RG.CODIGO ID_REGION, "
            "RG.NOMBRE REGION, "
            "CO.CODIGO ID_SUCURSAL, "
            "CO.NOMBRE SUCURSAL "
            "FROM "
            "PCO, CO, RG "
            "WHERE "
            "PCO.CDGCO = CO.CODIGO "
            "AND CO.CDGRG = RG.CODIGO "
            "AND PCO.CDGEM = 'EMPFIN' "
            f"AND PCO.CDGPE = 'ADMIN' "
            "ORDER BY "
            "ID_REGION, "
            "ID_SUCURSAL"
        )
        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        details = []

        id_sucursales = []

        items = []
        for row in list:
            id_sucursales.append(row[2])
        formatted_string = f"({', '.join([f'{x!r}' for x in id_sucursales])})"

        sql_sentence = (
            "SELECT "
            "SUM(NUM_PAGOS) TOTAL_PAGOS_SUC, "
            "FECHA_D, "
            "FECHA, "
            "FECHA_REGISTRO, "
            "CDGOCPE, "
            "COD_REGION, "
            "NOMBRE_REGION, "
            "CODIGO_SUC, "
            "NOM_SUCURSAL, "
            "SUM(TOTAL_PAGOS) AS TOTAL_PAGOS, "
            "SUM(TOTAL_MULTA) AS TOTAL_MULTA, "
            "SUM(TOTAL_REFINANCIAMIENTO) AS TOTAL_REFINANCIAMIENTO, "
            "SUM(TOTAL_DESCUENTO) AS TOTAL_DESCUENTO, "
            "SUM(GARANTIA) AS TOTAL_GARANTIA, "
            "SUM(MONTO_TOTAL) AS MONTO_TOTAL "
            "FROM SUCURSALES_LISTA "
            f"WHERE CODIGO_SUC IN{formatted_string} "
            f"AND TO_DATE(FECHA, 'DD-MM-YYYY') BETWEEN TO_DATE('{fecha_inicio}', 'DD-MM-YYYY') AND TO_DATE('{fecha_final}', 'DD-MM-YYYY') "
            "GROUP BY FECHA_D, FECHA, FECHA_REGISTRO, CDGOCPE, COD_REGION, NOMBRE_REGION, CODIGO_SUC, NOM_SUCURSAL "
            "ORDER BY FECHA DESC"
        )
        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        details = []

        for row in list:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            obj = {
                "agency_number": items[7],
                "agency": items[8],
                "fecha": items[2],
                "fecha_D": items[1],
                "payments_registered": items[0],
                "accumulated_amount": items[14],
            }
            # Valida si no existe en la BDD de PostgreSQL, de lo contrario la inserta
            if not Agency.objects.filter(agency_number=obj["agency_number"]).exists():
                Agency.objects.create(**obj)
            details.append(obj)

        return details

    except Exception as e:
        print(e)
        details = []
        obj = {
            "agency_number": "001044",
            "agency": "HOLIS FENIX",
            "payments_registered": "01",
            "accumulated_amount": "0111",
            "fecha": fecha_inicio
        }
        details.append(obj)
        return details
        # raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()

def getPaymentsbyExecutive(id_agencie, fecha_referencia):
    """Obtener los pagos por Ejecutivo"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()

        sql_sentence = (
         "SELECT * FROM SUCURSALES_LISTA "
         f"WHERE CODIGO_SUC =  '{id_agencie}' AND FECHA = '{fecha_referencia}' "
         "ORDER BY FECHA DESC "
        )

        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        details = []

        for row in list:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            obj = {
                "agency_number": items[6],
                "agency": items[10],
                "ejecutivo": items[2],
                "codigo":items[1],
                "payments_registered": items[11],
                "accumulated_amount": items[16],
                "fecha": items[4],
            }
            details.append(obj)

        return details

    except Exception as e:
        print(e)
        details = []
        obj = {
            "agency_number": "001044",
            "agency": "HOLIS FENIX",
            "payments_registered": "01",
            "accumulated_amount": "0111",
        }
        details.append(obj)
        return details
        # raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()

