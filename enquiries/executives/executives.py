from django.conf import settings

from mas_con_menos.bridge_mcm.enquiries import connection

"""from mas_con_menos.executive.models import Executive"""

CDGEM = settings.MCM_CDGEM


def get_executives(codigo_usuario, fecha_inicial, fecha_final):
    """Petición para obtener la lista de ejecutivos"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            " SELECT NOMBREC(NULL,NULL,'I','N',PE.NOMBRE1,PE.NOMBRE2,PE.PRIMAPE,PE.SEGAPE) AS EJECUTIVO"
            " ,CO.NOMBRE AS SUCURSAL"
            " ,COUNT(*) AS NUM_PAGOS"
            " , SUM(CANTIDAD) MONTO"
            " ,PE.CODIGO"
            " FROM MP, PRN,PE,CO"
            " WHERE MP.CDGEM = PRN.CDGEM"
            " AND MP.CDGCLNS = PRN.CDGNS"
            " AND MP.CICLO = PRN.CICLO"
            " AND PRN.CDGEM = PE.CDGEM"
            " AND PRN.CDGOCPE = PE.CODIGO"
            " AND PRN.CDGEM = CO.CDGEM"
            " AND PRN.CDGCO = CO.CODIGO"
            f" AND MP.CDGEM = '{CDGEM}'"
            f" AND MP.FREALDEP BETWEEN TO_DATE('{fecha_inicial}','dd/mm/yyyy') "
            f" AND TO_DATE('{fecha_final}','dd/mm/yyyy')"
            " AND MP.TIPO = 'PD'"
            f" AND PRN.CDGCO IN (SELECT CDGCO FROM PCO WHERE CDGPE = '{codigo_usuario}')"
            " GROUP BY NOMBREC(NULL,NULL,'I','N',PE.NOMBRE1,PE.NOMBRE2,PE.PRIMAPE,PE.SEGAPE), CO.NOMBRE,PE.CODIGO"
            " ORDER BY 2,1"
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
                "EJECUTIVO": items[0],
                "SUCURSAL": items[1],
                "NUM_PAGOS": items[2],
                "MONTO": items[3],
                "CODIGO": items[4],
            }
            details.append(obj)

        return details

    except Exception as e:
        print(e)
        details = []
        obj = {
            "CDGNS": "001044",
            "NOMBRE": "HOLIS FENIX",
            "CICLO": "01",
            "REF_PAGO": "P001",
            "REF_GL": "0111",
        }
        details.append(obj)
        return details
        # raise serializers.ValidationError(f"Error en la conexión {e}")

    finally:
        if db_connection is not None:
            db_connection.close()
