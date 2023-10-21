import datetime as datetime
import logging
import traceback

from django.conf import settings

from rest_framework import serializers

from mas_con_menos.bridge_mcm.enquiries import connection
from mas_con_menos.clients.models import Client
from mas_con_menos.records.models import Record, RecordOrigen

LOG = logging.getLogger("enquiries.records.records")
CDGEM = settings.MCM_CDGEM


def get_and_update_records_enquiry(cdgns, ciclo):
    """Petición para obtener los registros de un cliente"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            "SELECT FECHA, CICLO, MONTO, "
            "CASE WHEN TIPO = 'P' THEN 'PAGO' "
            "     WHEN TIPO = 'M' THEN 'MULTA' "
            "     WHEN TIPO = 'R' THEN 'REFINANCIAMIENTO' "
            "     WHEN TIPO = 'D' THEN 'DESCUENTO' "
            "     WHEN TIPO = 'G' THEN 'GARANTIA' "
            "     ELSE 'NO IDENTIFICADO' "
            "END TIPO, FIDENTIFICAPP REGISTRO "
            "FROM CORTECAJA_PAGOSDIA "
            f"WHERE CDGEM = '{CDGEM}' "
            "AND TIPO IN ('M', 'R', 'D', 'G', 'P') "
            f"AND CDGNS = '{cdgns}' "
            f"AND CICLO = '{ciclo}' "
            "AND ESTATUS = 'A'"
        )
        LOG.info("get_and_update_records_enquiry")
        LOG.info(sql_sentence)
        cursor.execute(sql_sentence)

        list_records = cursor.fetchall()
        for row in list_records:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)

            if type(items[4]) == datetime.datetime:
                fecha = items[0].strftime("%d/%m/%Y")
                fecha_registro_ddmmyyyy_hhmmss = items[4].strftime("%d/%m/%Y %H:%M:%S")
                fecha_timestamp = items[4].strftime("%d%m%Y%H%M%S")
            elif items[4] == "":
                fecha = items[0].strftime("%d/%m/%Y")
                fecha_registro_ddmmyyyy_hhmmss = items[0].strftime("%d/%m/%Y %H:%M:%S")
                fecha_timestamp = items[0].strftime("%d%m%Y%H%M%S")

            else:
                raise serializers.ValidationError("REGISTRO: Tipo desconocido datetime")

            obj = {
                "FECHA": fecha,
                "CICLO": items[1],
                "MONTO": items[2],
                "TIPO": items[3],
                "REGISTRO": fecha_registro_ddmmyyyy_hhmmss,
                "TIMESTAMP": fecha_timestamp,
            }

            cliente = Client.objects.filter(CDGNS=cdgns).first()
            obj["CLIENTE"] = cliente

            if not Record.objects.filter(
                CLIENTE=obj["CLIENTE"],
                CICLO=obj["CICLO"],
                MONTO=obj["MONTO"],
                TIPO=obj["TIPO"],
                TIMESTAMP=obj["TIMESTAMP"],
            ).exists():
                LOG.info(
                    "Buscando registro con los valores: CLIENTE, CICLO, MONTO, TIPO, REGISTRO"
                )
                LOG.info(obj)
                obj["ORIGEN"] = RecordOrigen.ORACLE
                Record.objects.create(**obj)
        cursor.close()
        # return (
        #     Record.objects.filter(CLIENTE__CDGNS=client_id, CICLO=cycle)
        #     .all()
        #     .order_by("id")
        # )
        return (
            Record.objects.filter(CLIENTE__CDGNS=cdgns, CICLO=ciclo, is_active=True)
            .all()
            .order_by("-REGISTRO")
        )

    except Exception as e:
        LOG.error(str(e))
        raise serializers.ValidationError(f"Error en la conexión {e}")

    finally:
        if db_connection is not None:
            db_connection.close()


def get_filtered_records(cdgns, ciclo, fecha_inicial, fecha_final):
    """Petición para obtener los registros de un cliente"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            " SELECT FREALDEP AS FECHA,CICLO,CANTIDAD AS MONTO,'PAGO' AS TIPO, FFACTURA REGISTRO"
            " FROM MP "
            f" WHERE CDGEM = '{CDGEM}' "
            f" AND CDGCLNS = '{cdgns}' "
            f" AND CICLO = '{ciclo}' "
            " AND TIPO = 'PD' "
            f" AND FREALDEP BETWEEN TO_DATE('{fecha_inicial}','dd/mm/yyyy') AND TO_DATE('{fecha_final}','dd/mm/yyyy')"
            " UNION"
            " SELECT FECHA,CICLO,MONTO"
            " ,CASE WHEN TIPO = 'P' THEN 'PAGO'"
            "       WHEN TIPO = 'M' THEN 'MULTA'"
            "       WHEN TIPO = 'R' THEN 'REFINANCIAMIENTO'"
            "       WHEN TIPO = 'D' THEN 'DESCUENTO'"
            "       WHEN TIPO = 'G' THEN 'GARANTIA'"
            "       ELSE 'NO IDENTIFICADO'"
            "       END TIPO, FREGISTRO REGISTRO      "
            " FROM PAGOSDIA "
            f" WHERE CDGEM = '{CDGEM}'"
            " AND TIPO IN ('M','R','D','G')"
            f" AND CDGNS = '{cdgns}'"
            f" AND CICLO = '{ciclo}'"
            f" AND FECHA BETWEEN TO_DATE('{fecha_inicial}','dd/mm/yyyy') AND TO_DATE('{fecha_final}','dd/mm/yyyy')"
        )
        cursor.execute(sql_sentence)

        list_items = cursor.fetchall()

        details = []

        for row in list_items:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            fecha = items[0]
            registro = items[4]

            obj = {
                "FECHA": fecha.strftime("%d/%m/%YY"),
                "CICLO": items[1],
                "MONTO": items[2],
                "TIPO": items[3],
                "REGISTRO": items[4],
            }
            if obj["REGISTRO"] is None:
                obj["REGISTRO"] = registro.strftime("%d/%m/%Y %H:%M:%S")
            # Valida si no existe en la BDD de PostgreSQL, de lo contrario la inserta
            cliente = Client.objects.filter(CDGNS=cdgns).first()
            obj["CLIENTE"] = cliente
            record = Record.objects.filter(
                CLIENTE=obj["CLIENTE"],
                CICLO=obj["CICLO"],
                MONTO=obj["MONTO"],
                TIPO=obj["TIPO"],
                REGISTRO=obj["REGISTRO"],
            ).first()
            details.append(record)
        cursor.close()
        # return (
        #     Record.objects.filter(CLIENTE__CDGNS=client_id, CICLO=cycle)
        #     .all()
        #     .order_by("id")
        # )
        return details

    except Exception as e:
        print(e)
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()


def get_cdgns_records(cdgns, ciclo):
    """Petición para obtener los detalles del cliente"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            " SELECT CDGNS, CICLO"
            " ,round(round((round(decode(nvl(PRN.periodicidad,''), 'S', nvl(PRN.cantentre,0) +"
            " (nvl(PRN.tasa,0) * nvl(PRN.plazo,0) * nvl(PRN.cantentre,0))/(4 * 100),"
            " 'Q', nvl(PRN.cantentre,0) + (nvl(PRN.tasa,0) * nvl(PRN.plazo,0) * nvl(PRN.cantentre,0) * 15)/(30 * 100),"
            " 'C', nvl(PRN.cantentre,0) + (nvl(PRN.tasa,0) * nvl(PRN.plazo,0) * nvl(PRN.cantentre,0))/(2 * 100),"
            " 'M', nvl(PRN.cantentre,0) + (nvl(PRN.tasa,0) * nvl(PRN.plazo,0) * nvl(PRN.cantentre,0))/(100),"
            " '', ''),0)) / PRN.PLAZO,2),2) PAGO_SEMANAL"
            " ,SALDOTOTALPRN(PrN.CdgEm, PrN.CdgNS, PrN.Ciclo, PrN.CantEntre, PrN.Tasa,"
            " PrN.Plazo, PrN.Periodicidad, PrN.CdgMCI, PrN.Inicio, PrN.DiaJunta"
            " ,PrN.MULTPER, PrN.PeriGrCap, PrN.PeriGrInt, PrN.DesfasePago, PrN.CdgTI, PrN.ModoApliReca"
            " ,'01/SEP/2021') AS SALDOTOTAL"
            " FROM PRN"
            f" WHERE CDGEM = '{CDGEM}'"
            f" AND CDGNS = '{cdgns}'"
            f" AND CICLO = '{ciclo}'"
        )

        LOG.info("get_cdgns_records: ")
        LOG.info(str(sql_sentence))

        cursor.execute(sql_sentence)

        list = cursor.fetchall()

        obj = {}

        if list == []:
            return obj

        else:
            item = list[0]

            obj = {
                "CDGNS": item[0],
                "CICLO": item[1],
                "PAGO_SEMANAL": item[2],
                "SALDOTOTAL": item[3],
                "LATITUD": 19.608942,
                "LONGITUD": -99.241021,
            }

            cursor.close()

            return obj

    except Exception as e:
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()


def new_payment_record_v1(ref, amount, fecha_de_pago_oracle, user, time_stamp):
    """v1: Sólo se agregarán PAGOS y GARANTÍAS en este método"""

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        cursor.callproc("dbms_output.enable")
        sql_sentence = (
            " DECLARE"
            " PFECHAPAGO DATE;"
            " PREFERENCIA VARCHAR2(100);"
            " PMONTO NUMBER;"
            " PEMPRESA VARCHAR2(6);"
            " PCTABANCARIA VARCHAR2(2);"
            " PUSUARIO VARCHAR2(6);"
            " PIDENTIFICADOR VARCHAR2(200);"
            " PPERIODO NUMBER;"
            " POPERACION VARCHAR2(200);"
            " RES VARCHAR2(200);"
            " PMONTOCANCEL NUMBER;"
            " PRENEXCEL NUMBER;"
            " PRENGLON NUMBER;"
            " PNOPAGOS NUMBER;"
            " PIDIMPORTACION NUMBER;"
            " PVALIDACION NUMBER;"
            " PMON VARCHAR2(200);"
            " BEGIN"
            f" PFECHAPAGO := {fecha_de_pago_oracle};"
            f" PREFERENCIA := '{ref}';"
            f" PMONTO := {amount};"
            " PEMPRESA := 'EMPFIN';"
            " PCTABANCARIA := '02';"
            f" PUSUARIO := '{user}';"
            f" PIDENTIFICADOR := '{time_stamp}';"
            " PPERIODO := 1;"
            " POPERACION := 'I';"
            " RES := NULL;"
            " PMONTOCANCEL := 0;"
            " PRENEXCEL := NULL;"
            " PRENGLON := NULL;"
            " PNOPAGOS := NULL;"
            " PIDIMPORTACION := NULL;"
            " PVALIDACION := NULL;"
            " PMON := 'MN';      "
            " ESIACOM.PKG_IMPORTAPAGOSOF.SPIMPORTAPAGOSOF ( "
            " PFECHAPAGO, PREFERENCIA, PMONTO, PEMPRESA, PCTABANCARIA, PUSUARIO, PIDENTIFICADOR, "
            " PPERIODO, POPERACION, RES, PMONTOCANCEL, PRENEXCEL, PRENGLON, PNOPAGOS, PIDIMPORTACION, "
            " PVALIDACION, PMON );"
            " DBMS_OUTPUT.PUT_LINE('' || RES);"
            " COMMIT;"
            " END;"
        )
        LOG.info("new_payment_record_v1: ")
        LOG.info(str(sql_sentence))

        # Requerimos un formato de fecha del tipo 2022-08-11 ("%Y-$m-%d")
        cursor.execute(sql_sentence)

        chunk_size = 100

        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)

        data = []
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                data.append(line)
            if num_lines < chunk_size:
                break

        LOG.info("new_payment_record_v1__data: ")
        LOG.info(str(data))

        return data

    except Exception as e:
        traceback.print_exc()
        raise serializers.ValidationError(f"Error en la conexión {e}")

    finally:
        if db_connection is not None:
            db_connection.close()


def new_payment_record_v2(
    cdgns,
    ciclo,
    secuencia,
    nombre_cli,
    cdgco,
    ejecutivo,
    usuario,
    monto,
    tipo,
    movimiento,
    time_stamp
):
    """Petición para registro de descuentos, refinanciamiento, multa"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        cursor.callproc("dbms_output.enable")
        sql_sentence = (
            " DECLARE"
            " PRMCDGEM VARCHAR2(6);"
            " PRMFECHA DATE;"
            " PRMFECHAAUX DATE;"
            " PRMCDGNS VARCHAR2(6);"
            " PRMCICLO VARCHAR2(2);"
            " PRMSECUENCIA NUMBER(22);"
            " PRMNOMBRE VARCHAR2(200);"
            " PRMCDGOCPE VARCHAR2(6);"
            " PRMEJECUTIVO VARCHAR2(200);"
            " PRMCDGPE VARCHAR2(6);"
            " PRMMONTO NUMBER(22);"
            " PRMTIPOMOV VARCHAR2(1);"
            " PRMTIPO NUMBER(22);"
            " VMENSAJE VARCHAR2(200);"
            " PRMIDENTIFICAAPP VARCHAR2(200);"
            " BEGIN"
            " PRMCDGEM := 'EMPFIN';"
            " PRMFECHA := TRUNC(SYSDATE);"
            " PRMFECHAAUX := NULL;"
            f" PRMCDGNS := '{cdgns}';"
            f" PRMCICLO := '{ciclo}';"
            f" PRMSECUENCIA := '{secuencia}';"
            f" PRMNOMBRE := '{nombre_cli}';"
            f" PRMCDGOCPE := '{cdgco}';"
            f" PRMEJECUTIVO := '{ejecutivo}';"
            f" PRMCDGPE := '{usuario}';"
            f" PRMMONTO := '{monto}';"
            f" PRMTIPOMOV := '{tipo}';"
            f" PRMTIPO := '{movimiento}';"  # validar aqui si es 1, 2 o 3
            " VMENSAJE := NULL;"
            f" PRMIDENTIFICAAPP := '{time_stamp}';"
            " ESIACOM.SPACCIONPAGODIA ( PRMCDGEM, PRMFECHA, PRMFECHAAUX, PRMCDGNS, PRMCICLO, PRMSECUENCIA"
            ",PRMNOMBRE, PRMCDGOCPE, PRMEJECUTIVO, PRMCDGPE, PRMMONTO, PRMTIPOMOV, PRMTIPO, VMENSAJE"
            ",PRMIDENTIFICAAPP );"
            " DBMS_OUTPUT.PUT_LINE('' || VMENSAJE);"
            " COMMIT;"
            " END;"
        )
        LOG.info("new_payment_record_v2: ")
        LOG.info(str(sql_sentence))

        cursor.execute(sql_sentence)

        chunk_size = 100

        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)
        data = []
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                data = [line]
            if num_lines < chunk_size:
                break

        return data

    except Exception as e:
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()


def edit_payment_record_v2(
    cdgns,
    ciclo,
    secuencia,
    nombre_cli,
    cdgco,
    ejecutivo,
    usuario,
    monto,
    tipo,
    movimiento,
):
    """Petición para edicion de descuentos, refinanciamiento, multa"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        cursor.callproc("dbms_output.enable")
        sql_sentence = (
            " DECLARE"
            " PRMCDGEM VARCHAR2(6);"
            " PRMFECHA DATE;"
            " PRMFECHAAUX DATE;"
            " PRMCDGNS VARCHAR2(6);"
            " PRMCICLO VARCHAR2(2);"
            " PRMSECUENCIA NUMBER(22);"
            " PRMNOMBRE VARCHAR2(200);"
            " PRMCDGOCPE VARCHAR2(6);"
            " PRMEJECUTIVO VARCHAR2(200);"
            " PRMCDGPE VARCHAR2(6);"
            " PRMMONTO NUMBER(22);"
            " PRMTIPOMOV VARCHAR2(1);"
            " PRMTIPO NUMBER(22);"
            " VMENSAJE VARCHAR2(200);"
            " BEGIN"
            " PRMCDGEM := 'EMPFIN';"
            " PRMFECHA := TRUNC(SYSDATE);"
            " PRMFECHAAUX := NULL;"
            f" PRMCDGNS := '{cdgns}';"
            f" PRMCICLO := '{ciclo}';"
            f" PRMSECUENCIA := '{secuencia}';"
            f" PRMNOMBRE := '{nombre_cli}';"
            f" PRMCDGOCPE := '{cdgco}';"
            f" PRMEJECUTIVO := '{ejecutivo}';"
            f" PRMCDGPE := '{usuario}';"
            f" PRMMONTO := '{monto}';"
            f" PRMTIPOMOV := '{tipo}';"
            f" PRMTIPO := '{movimiento}';"  # validar aqui si es 1, 2 o 3
            " VMENSAJE := NULL;"
            " ESIACOM.SPACCIONPAGODIA ( PRMCDGEM, PRMFECHA, PRMFECHAAUX, PRMCDGNS, PRMCICLO, PRMSECUENCIA"
            " ,PRMNOMBRE, PRMCDGOCPE, PRMEJECUTIVO, PRMCDGPE, PRMMONTO, PRMTIPOMOV, PRMTIPO, VMENSAJE );"
            " DBMS_OUTPUT.PUT_LINE('' || VMENSAJE);"
            " COMMIT;"
            " END;"
        )
        cursor.execute(sql_sentence)

        chunk_size = 100

        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)
        data = []
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                data = [line]
            if num_lines < chunk_size:
                break

        return data

    except Exception as e:
        raise serializers.ValidationError(f"Error en la conexión {e}")

    finally:
        if db_connection is not None:
            db_connection.close()


def delete_payment_record_v2(
    cdgns,
    ciclo,
    secuencia,
    nombre_cli,
    cdgco,
    ejecutivo,
    usuario,
    monto,
    tipo,
    movimiento,
):
    """Petición para eliminar descuentos, refinanciamiento, multa"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        cursor.callproc("dbms_output.enable")
        sql_sentence = (
            " DECLARE"
            " PRMCDGEM VARCHAR2(6);"
            " PRMFECHA DATE;"
            " PRMFECHAAUX DATE;"
            " PRMCDGNS VARCHAR2(6);"
            " PRMCICLO VARCHAR2(2);"
            " PRMSECUENCIA NUMBER(22);"
            " PRMNOMBRE VARCHAR2(200);"
            " PRMCDGOCPE VARCHAR2(6);"
            " PRMEJECUTIVO VARCHAR2(200);"
            " PRMCDGPE VARCHAR2(6);"
            " PRMMONTO NUMBER(22);"
            " PRMTIPOMOV VARCHAR2(1);"
            " PRMTIPO NUMBER(22);"
            " VMENSAJE VARCHAR2(200);"
            " BEGIN"
            " PRMCDGEM := 'EMPFIN';"
            " PRMFECHA := NULL;"
            " PRMFECHAAUX := TRUNC(SYSDATE);"
            f" PRMCDGNS := '{cdgns}';"
            f" PRMCICLO := '{ciclo}';"
            f" PRMSECUENCIA := '{secuencia}';"  # aumentar en uno para borrar
            f" PRMNOMBRE := '{nombre_cli}';"
            f" PRMCDGOCPE := '{cdgco}';"
            f" PRMEJECUTIVO := '{ejecutivo}';"
            f" PRMCDGPE := '{usuario}';"
            f" PRMMONTO := '{monto}';"
            f" PRMTIPOMOV := '{tipo}';"
            f" PRMTIPO := '{movimiento}';"  # validar aqui si es 1, 2 o 3, mismo sp
            " VMENSAJE := NULL;"
            " ESIACOM.SPACCIONPAGODIA ( PRMCDGEM, PRMFECHA, PRMFECHAAUX, PRMCDGNS, PRMCICLO, PRMSECUENCIA"
            " ,PRMNOMBRE, PRMCDGOCPE, PRMEJECUTIVO, PRMCDGPE, PRMMONTO, PRMTIPOMOV, PRMTIPO, VMENSAJE );"
            " DBMS_OUTPUT.PUT_LINE('' || VMENSAJE);"
            " COMMIT;"
            " END;"
        )
        cursor.execute(sql_sentence)

        chunk_size = 100

        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)
        data = []
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                data = [line]
            if num_lines < chunk_size:
                break

        return data

    except Exception as e:
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()


def get_all_records(fechaInicio, fechaFin, codigo):
    """Petición para obtener los montos de las graficas"""
    db_connection = None

    try:
        db_connection = connection.connect()
        cursor = db_connection.cursor()
        sql_sentence = (
            " SELECT CORTECAJA_PAGOSDIA.FECHA, SUM (MONTO) AS MONTO, CO.NOMBRE  AS SUCURSAL, COUNT(MONTO)  AS CANT_PAGOS, CORTECAJA_PAGOSDIA.EJECUTIVO "
            " FROM CORTECAJA_PAGOSDIA, PRN"
            " INNER JOIN CO ON CO.CODIGO = PRN.CDGCO"
            " WHERE CORTECAJA_PAGOSDIA.CDGEM = PRN.CDGEM"
            " AND CORTECAJA_PAGOSDIA.CDGNS = PRN.CDGNS"
            " AND CORTECAJA_PAGOSDIA.CICLO = PRN.CICLO"
            " AND CORTECAJA_PAGOSDIA.CDGEM = 'EMPFIN'"
            f" AND CORTECAJA_PAGOSDIA.FECHA BETWEEN '{fechaInicio}' AND '{fechaFin}'"
            "AND (CORTECAJA_PAGOSDIA.TIPO = 'P' OR CORTECAJA_PAGOSDIA.TIPO = 'M')"
            " AND CORTECAJA_PAGOSDIA.ESTATUS = 'A'"
            " AND CO.CODIGO = PRN.CDGCO"
            f" AND PRN.CDGCO IN (SELECT CDGCO FROM PCO WHERE CDGPE = '{codigo}')"
            " GROUP BY CORTECAJA_PAGOSDIA.FECHA, PRN.CDGCO, CO.NOMBRE, CORTECAJA_PAGOSDIA.EJECUTIVO "
            " ORDER BY CORTECAJA_PAGOSDIA.FECHA"
        )
        cursor.execute(sql_sentence)
        list_items = cursor.fetchall()
        arreglo = []
        for row in list_items:
            items = []
            for item in row:
                if item is None:
                    items.append("")
                else:
                    items.append(item)
            fecha = items[0]
            obj = {
                "FECHA": fecha.strftime("%d/%m/%Y"),
                "MONTO": items[1],
                "SUCURSAL": items[2],
                "CANT_PAGOS": items[3]
            }
            arreglo.append(obj)
        cursor.close()
        return arreglo
    except Exception as e:
        print(e)
        raise serializers.ValidationError(f"Error en la conexión {e}")
    finally:
        if db_connection is not None:
            db_connection.close()
