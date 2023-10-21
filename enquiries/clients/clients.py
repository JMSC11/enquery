from django.conf import settings

from mas_con_menos.bridge_mcm.enquiries import connection
from mas_con_menos.clients.models import Client

CDGEM = settings.MCM_CDGEM


def get_clients(codigo_ejecutivo):
    """Petición para obtener la lista de clientes"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            " SELECT PRN.CDGNS, NS.NOMBRE, PRN.CICLO"
            " , 'P' || PRN.CDGNS || PRN.CDGTPC || FN_DV('P' || PRN.CDGNS || PRN.CDGTPC) REF_PAGO"
            " , '0' || PRN.CDGNS || PRN.CDGTPC || FN_DV('0' || PRN.CDGNS || PRN.CDGTPC) REF_GL"
            " FROM PRN,NS"
            " WHERE PRN.CDGEM = NS.CDGEM"
            " AND PRN.CDGNS = NS.CODIGO"
            f" AND PRN.CDGEM = '{CDGEM}'"
            " AND PRN.SITUACION = 'E'"
            f" AND PRN.CDGOCPE = '{codigo_ejecutivo}'"
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
                "CDGNS": items[0],
                "NOMBRE": items[1],
                "CICLO": items[2],
                "REF_PAGO": items[3],
                "REF_GL": items[4],
            }
            # Valida si no existe en la BDD de PostgreSQL, de lo contrario la inserta
            if not Client.objects.filter(CDGNS=obj["CDGNS"]).exists():
                Client.objects.create(**obj)
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
