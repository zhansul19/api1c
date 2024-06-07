from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import clickhouse_connect
import psycopg2

# Initialize FastAPI app
app = FastAPI()

# Connect to ClickHouse
client = clickhouse_connect.get_client(user='nifitest',
                                       password='nifitest',
                                       host='192.168.122.45',
                                       port=8123)


DATABASE_CONFIG = {
        'database': 'postgres',
        'user': 'photo_user',
        'password': 'TgYhUj123!@#',
        'host': '192.168.122.6',
        'port': '5432'
}


def connect_to_db():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        return conn
    except Exception as e:
        print("Unable to connect to the database:", e)
        return None


@app.get("/get_photo/{iin}")
async def read_photo(iin: str):
    conn = connect_to_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT photo FROM import_fl.photo WHERE iin = %s and document_type_id='2'", (iin,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return StreamingResponse(iter([bytes(row[0])]),media_type='application/octet-stream')
        else:
            raise HTTPException(status_code=404, detail="No photo found for the given iin")
    else:
        raise HTTPException(status_code=500, detail="Failed to connect to the database")


@app.get("/get_data/{iin}")
async def get_data(iin: str):
    if len(iin) != 12:
        raise HTTPException(status_code=404, detail="IIN Length should be 12")

    query = ("""
        SELECT
            fpn.IIN AS IIN,
            fpn.FIRSTNAME AS FIRSTNAME,
            fpn.SECONDNAME AS SECONDNAME,
            fpn.SURNAME AS SURNAME,
            IF(fpn.SEX_ID = '1', 'Мужчина', IF(fpn.SEX_ID = '2', 'Женщина', 'Unknown')) AS SEX,
            fpn.BIRTH_DATE AS BIRTH_DATE,
            n.RU_NAME AS NATIONALITY,
            MAX(dd.DOCUMENT_NUMBER) AS DOCUMENT_NUMBER,
            dc.RU_NAME AS COUNTRY_RU_NAME,
            dc.KZ_NAME AS COUNTRY_KZ_NAME
        FROM
            ser.fl_person_new AS fpn
        INNER JOIN
            db_fl_ul_dpar.damp_document AS dd ON fpn.IIN = dd.IIN
        INNER JOIN
            db_fl_ul_dpar.DIC_COUNTRY AS dc ON fpn.CITIZENSHIP_ID = CAST(dc.ID AS String)
        INNER JOIN
            db_fl_ul_dpar.nationality AS n ON fpn.NATIONALTY_ID = CAST(n.ID AS String) AND fpn.SEX_ID = n.SEX
        WHERE
            fpn.IIN = %(iin)s
            AND dd.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'
        GROUP BY
            fpn.IIN,
            fpn.FIRSTNAME,
            fpn.SECONDNAME,
            fpn.SURNAME,
            fpn.BIRTH_DATE,
            dc.RU_NAME,
            dc.KZ_NAME,
            fpn.SEX_ID,
            n.RU_NAME,
            fpn.CITIZENSHIP_ID
    """)

    result = client.query(query, parameters={'iin': iin})
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return result.named_results()


@app.get("/hello")
async def get_data2():
    return "hello world"
