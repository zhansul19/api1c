from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import clickhouse_connect
import psycopg2
from PIL import Image
import io
import httpx
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
async def read_photo(iin: str):
    conn = connect_to_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("""
        select 
         fp."IIN", 
         fp."FIRSTNAME",
         fp."SURNAME",
         fp."SECONDNAME",
         case 
            when fp."SEX_ID"='1' then 'Мужчина'
            when fp."SEX_ID"='2' then 'Женщина'
         end as SEX,
         fp."BIRTH_DATE",
         
         dc."RU_NAME" as "BIRTH_COUNTRY_RU",
         dc."KZ_NAME"as "BIRTH_COUNTRY_KZ",
          
         
         dd."RU_NAME" as "BIRTH_DISTRICT_NAME_RU",
         dd."KZ_NAME" as "BIRTH_DISTRICT_NAME_KZ",
         dr."RU_NAME" as "BIRTH_REGION_NAME_RU",
         dr."KZ_NAME" as "BIRTH_REGION_NAME_KZ",
        
        fp."BIRTH_CITY",
        dc2."RU_NAME" as "CITIZENSHIP",
         dn."RU_NAME" as "NATIONALITY_RU",
         dn."KZ_NAME" as "NATIONALITY_KZ",
         
         ddtn."RU_NAME" as "DOCUMENT_TYPE",
         fd."DOCUMENT_NUMBER",
         fd."DOCUMENT_BEGIN_DATE",
         fd."DOCUMENT_END_DATE",
         ddo."RU_NAME" as "ISSUE_ORGANIZATION"
        from imp_kfm_fl.fl_person fp 
        left outer join imp_kfm_fl.fl_document fd  on fp."ID"  = fd."PERSON_ID" 
        left join "dictionary".d_country dc on fp."BIRTH_COUNTRY_ID" = cast(dc."ID" as text )
        left join "dictionary".d_country dc2 on fp."CITIZENSHIP_ID" = cast(dc2."ID" as text )

        left join "dictionary".d_nationality_new dn on fp."NATIONALTY_ID" =cast(dn."ID"  as text )
        left join "dictionary".d_districts dd on fp."BIRTH_DISTRICTS_ID"  =cast(dd."ID"  as text )
        left join "dictionary".d_region dr on fp."BIRTH_REGION_ID"  =cast(dr."ID"  as text )
        left join "dictionary".d_document_type_new ddtn on fd."DOCUMENT_TYPE_ID" =cast(ddtn."ID"  as text )
        left join "dictionary".d_doc_organization ddo on fd."ISSUE_ORGANIZATION_ID" =cast(ddo."ID"  as text )
        
        
        where fp."IIN" like %s
        AND ddtn."ID" = 2 
        order by fd."DOCUMENT_BEGIN_DATE" desc
""", (iin,))
        row = cursor.fetchone()
        conn.close()


        query2 = """SELECT 
            s.iin AS IIN,
            s.study_info AS STUDY,
            s2.school_info AS SCHOOL,
            ra.`Адрес на русском` AS ADDRESS,
          nb.phonenumber_ AS PHONE_NUMBER

        FROM (
            SELECT 
                iin,
                groupArray(tuple(study_code, study_name, start_date, end_date)) AS study_info
            FROM 
                db_fl_ul_dpar.study
            GROUP BY 
                iin
        ) AS s
        LEFT JOIN (
            SELECT 
                iin,
                groupArray(tuple(school_code, school_name, start_date, end_date)) AS school_info
            FROM 
                db_fl_ul_dpar.school
            GROUP BY 
                iin
        ) AS s2 ON s.iin = s2.iin
        LEFT JOIN 
            db_fl_ul_dpar.reg_address AS ra ON s.iin = ra.`ИИН/БИН` 
        LEFT JOIN
           db_fl_ul_dpar.numb AS nb ON s.iin = nb.iin_
           
                WHERE s.iin = %(iin)s 
                """
        result = client.query(query2, parameters={'iin': iin})
        if row:
            data = {
                "IIN": row[0],
                "FIRSTNAME": row[1],
                "SURNAME": row[2],
                "SECONDNAME": row[3],
                "SEX": row[4],
                "BIRTH_DATE": row[5],
                "BIRTH_COUNTRY_RU": row[6],
                "BIRTH_COUNTRY_KZ": row[7],
                "BIRTH_DISTRICT_NAME_RU": row[8],
                "BIRTH_DISTRICT_NAME_KZ": row[9],
                "BIRTH_REGION_NAME_RU": row[10],
                "BIRTH_REGION_NAME_KZ": row[11],
                 "BIRTH_CITY": row[12],
                 "CITIZENSHIP": row[13],
                "NATIONALITY_RU": row[14],
                "NATIONALITY_KZ": row[15],
                "DOCUMENT_TYPE": row[16],
                "DOCUMENT_NUMBER": row[17],
                "DOCUMENT_BEGIN_DATE": row[18],
                "DOCUMENT_END_DATE": row[19],
                "ISSUE_ORGANIZATION": row[20],

                 "data": result.named_results()
            }

            return data

    else:
        raise HTTPException(status_code=500, detail="Failed to connect to the database")

# @app.get("/get_data/{iin}")
# async def get_data(iin: str):
#     if len(iin) != 12:
#         raise HTTPException(status_code=404, detail="IIN Length should be 12")
#
#     query = ("""
#          SELECT
#             dd.IIN AS IIN,
#             dd.FIRSTNAME_ AS FIRSTNAME,
#             dd.SURNAME_ AS SURNAME,
#             dd.SECONDNAME_ AS SECONDNAME,
#             IF(dd.SEX_ID = '1', 'Мужчина', IF(dd.SEX_ID = '2', 'Женщина', 'Unknown')) AS SEX,
#             dd.BIRTH_DATE_ AS BIRTH_DATE,
#             dd2.RU_NAME AS BIRTH_DISTRICT_RU_NAME,
#             dd2.KZ_NAME AS BIRTH_DISTRICT_KZ_NAME,
#             dr.RU_NAME AS BIRTH_REGION_RU_NAME,
#             dr.KZ_NAME AS BIRTH_REGION_KZ_NAME,
#             dd.BIRTH_CITY AS BIRTH_CITY,
#
#             dd.DOCUMENT_NUMBER AS DOCUMENT_NUMBER,
#             dd.ISSUE_ORGANIZATION_ID,
#             dd.DOCUMENT_BEGIN_DATE,
#             dd.DOCUMENT_END_DATE,
#             n.RU_NAME AS NATIONALITY,
#             n.KZ_NAME AS NATIONALITY_KZ,
#             dc.RU_NAME AS COUNTRY_RU_NAME,
#             dc.KZ_NAME AS COUNTRY_KZ_NAME,
#             ra.`Адрес на русском` AS ADDRESS,
#             nb.phonenumber_ AS PHONE_NUMBER
#
#         FROM
#             db_fl_ul_dpar.damp_document AS dd
#         LEFT JOIN
#             db_fl_ul_dpar.DIC_COUNTRY AS dc ON dd.CITIZENSHIP_ID = CAST(dc.ID AS String)
#         LEFT JOIN
#             db_fl_ul_dpar.nationality AS n ON dd.NATIONALTY_ID = CAST(n.ID AS String) AND dd.SEX_ID = n.SEX
#         LEFT JOIN
#             db_fl_ul_dpar.reg_address AS ra ON dd.IIN = ra.`ИИН/БИН`
#         LEFT JOIN
#             db_fl_ul_dpar.numb AS nb ON dd.IIN = nb.iin_
#         LEFT JOIN
#             db_fl_ul_dpar.DIC_DISTRICTS AS dd2 ON dd.BIRTH_DISTRICTS_ID = CAST(dd2.ID AS String)
#         LEFT JOIN
#             db_fl_ul_dpar.DIC_REGION AS dr ON dd.BIRTH_REGION_ID = CAST(dr.ID AS String)
#         WHERE
#             dd.IIN = %(iin)s
#             AND dd.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'
#             AND dd.DOCUMENT_BEGIN_DATE = (
#                 SELECT MAX(d2.DOCUMENT_BEGIN_DATE)
#                 FROM db_fl_ul_dpar.damp_document d2
#                 WHERE d2.IIN = %(iin)s
#                 AND d2.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'
#             )
#         GROUP BY
#             dd.IIN,
#             dd.FIRSTNAME_,
#             dd.SECONDNAME_,
#             dd.SURNAME_,
#             dd.BIRTH_DATE_,
#             dd.BIRTH_CITY,
#             dd.BIRTH_REGION_NAME,
#             dd.BIRTH_DISTRICT_NAME,
#             dd2.RU_NAME,
#             dd2.KZ_NAME,
#             dr.RU_NAME,
#             dr.KZ_NAME,
#             dc.RU_NAME,
#             dc.KZ_NAME,
#             dd.SEX_ID,
#             n.RU_NAME,
#             n.KZ_NAME,
#             dd.CITIZENSHIP_ID,
#             dd.DOCUMENT_NUMBER,
#             dd.ISSUE_ORGANIZATION_ID,
#             dd.DOCUMENT_BEGIN_DATE,
#             dd.DOCUMENT_END_DATE,
#             ra.`Адрес на русском`,
#             dd.DOCUMENT_BEGIN_DATE,
#             nb.phonenumber_
#     """)
#     query2 = """SELECT
#             s.iin AS IIN,
#             s.study_info AS STUDY,
#             s2.school_info AS SCHOOL
#         FROM (
#             SELECT
#                 iin,
#                 groupArray(tuple(study_code, study_name, start_date, end_date)) AS study_info
#             FROM
#                 db_fl_ul_dpar.study
#             GROUP BY
#                 iin
#         ) AS s
#         LEFT JOIN (
#             SELECT
#                 iin,
#                 groupArray(tuple(school_code, school_name, start_date, end_date)) AS school_info
#             FROM
#                 db_fl_ul_dpar.school
#             GROUP BY
#                 iin
#         ) AS s2 ON s.iin = s2.iin
#
#         WHERE s.iin = %(iin)s
#         """
#     result = client.query(query, parameters={'iin': iin})
#     result2 = client.query(query2, parameters={'iin': iin})
#
#     combined = {
#         "data": result.named_results(),
#         "study": result2.named_results()
#     }
#     return combined


@app.get("/get_relatives/{iin}")
async def get_relatives(iin: str):
    if len(iin) != 12:
        raise HTTPException(status_code=404, detail="IIN Length should be 12")

    query = ("""
        SELECT 
            fr.iin,
            CASE
                WHEN MAX(LENGTH(fr.marriage_divorce_date)) > 1 THEN 'Divorced'
                WHEN MAX(LENGTH(fr.marriage_reg_date)) > 1 THEN 'Married'
                ELSE 'Single'
            END AS STATUS,
            groupArray(tuple(fr.parent_iin, fr.parent_fio, fr.parent_birth_date, fr.relative_type)) AS relatives
        FROM 
            ser.fl_relatives fr 
        WHERE 
            fr.iin = %(iin)s
        GROUP BY 
            fr.iin
    """)

    result = client.query(query, parameters={'iin': iin})
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="Data not found")
    return result.named_results()


@app.get("/hello")
async def get_data2():
    return "hello world"
