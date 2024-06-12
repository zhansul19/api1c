from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import clickhouse_connect
import psycopg2
from PIL import Image
import io
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

@app.get("/get_photo_png/{iin}")
async def read_photo(iin: str):
    conn = connect_to_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT photo FROM import_fl.photo WHERE iin = %s and document_type_id='2'", (iin,))
        row = cursor.fetchone()
        conn.close()
        if row:
            # Convert bytea data to PNG image
            bytea_data = row[0]
            image = Image.open(io.BytesIO(bytea_data))

            # Convert the image to PNG format and return it
            with io.BytesIO() as output:
                image.save(output, format="PNG")
                return StreamingResponse(iter([output.getvalue()]), media_type='image/png')

    else:
        raise HTTPException(status_code=500, detail="Failed to connect to the database")


@app.get("/get_data/{iin}")
async def get_data(iin: str):
    if len(iin) != 12:
        raise HTTPException(status_code=404, detail="IIN Length should be 12")

    query = ("""
        select dd.IIN IIN,
        dd.FIRSTNAME_ FIRSTNAME,
        dd.SURNAME_ SURNAME,
        dd.SECONDNAME_ SECONDNAME,            
		IF(dd.SEX_ID = '1', 'Мужчина', IF(dd.SEX_ID = '2', 'Женщина', 'Unknown')) AS SEX,
		dd.BIRTH_DATE_ BIRTH_DATE,
		dd.DOCUMENT_NUMBER DOCUMENT_NUMBER,
		n.RU_NAME AS NATIONALITY,
		n.KZ_NAME AS NATIONALITY_KZ,
		dc.RU_NAME AS COUNTRY_RU_NAME,
		dc.KZ_NAME AS COUNTRY_KZ_NAME,
		ra.`Адрес на русском` as ADDRESS
from db_fl_ul_dpar.damp_document as dd 
INNER JOIN db_fl_ul_dpar.DIC_COUNTRY AS dc ON dd.CITIZENSHIP_ID = CAST(dc.ID AS String)
INNER JOIN db_fl_ul_dpar.nationality AS n ON dd.NATIONALTY_ID = CAST(n.ID AS String) AND dd.SEX_ID = n.SEX
INNER JOIN db_fl_ul_dpar.reg_address AS ra ON dd.IIN = ra.`ИИН/БИН` 

WHERE
      dd.IIN = %(iin)s AND 
     dd.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'  and
     dd.DOCUMENT_BEGIN_DATE =(SELECT MAX(d2.DOCUMENT_BEGIN_DATE) from  db_fl_ul_dpar.damp_document d2 where d2.IIN=%(iin)s AND d2.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'  )
GROUP BY
            dd.IIN,
            dd.FIRSTNAME_,
            dd.SECONDNAME_,
            dd.SURNAME_,
            dd.BIRTH_DATE_,
            dc.RU_NAME,
            dc.KZ_NAME,
            dd.SEX_ID,
            n.RU_NAME,
            n.KZ_NAME,
            dd.CITIZENSHIP_ID,
            dd.DOCUMENT_NUMBER,
            ra.`Адрес на русском`,
            dd.DOCUMENT_BEGIN_DATE
    """)

    result = client.query(query, parameters={'iin': iin})
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return result.named_results()


@app.get("/hello")
async def get_data2():
    return "hello world"
