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
        # 'user': 'postgres',
        # 'password': 'P9Kbb+A%D#',
        'host': '192.168.122.6',
        'port': '5432'
}
# h8MuxLk~1zKGZ

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
async def read_data(iin: str):
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

        client = clickhouse_connect.get_client(user='nifitest',
                                               password='nifitest',
                                               host='192.168.122.45',
                                               port=8123)

        query2 = """
        SELECT 
            s.iin AS IIN,
            s.study_info AS STUDY,
            s2.school_info AS SCHOOL,
            ra.`Адрес на русском` AS ADDRESS,
            nb.phonenumber_ AS PHONE_NUMBER
        FROM (
            SELECT 
                iin,
                groupArray(tuple(study_code AS study_code, study_name AS study_name, start_date AS start_date, end_date AS end_date)) AS study_info
            FROM 
                db_fl_ul_dpar.study
            GROUP BY 
                iin
        ) AS s
        LEFT JOIN (
            SELECT 
                iin,
                groupArray(tuple(school_code AS school_code, school_name AS school_name, start_date AS start_date, end_date AS end_date)) AS school_info
            FROM 
                db_fl_ul_dpar.school
            GROUP BY 
                iin
        ) AS s2 ON s.iin = s2.iin
        LEFT JOIN 
            db_fl_ul_dpar.reg_address AS ra ON s.iin = ra.`ИИН/БИН`
        LEFT JOIN
            db_fl_ul_dpar.numb AS nb ON s.iin = nb.iin_
        WHERE 
            s.iin = %(iin)s 
        """
        result = client.query(query2, parameters={'iin': iin})

        data = result.named_results()

        if row and data:
            # Process the results from ClickHouse
            processed_data = []
            for row1 in data:
                study_info = [
                    {
                        'study_bin': study[0],
                        'study_name': study[1],
                        'start_date': study[2],
                        'end_date': study[3]
                    }
                    for study in row1['STUDY']
                ]
                school_info = [
                    {
                        'school_bin': school[0],
                        'school_name': school[1],
                        'start_date': school[2],
                        'end_date': school[3]
                    }
                    for school in row1['SCHOOL']
                ]
                row1['STUDY'] = study_info
                row1['SCHOOL'] = school_info
                processed_data.append(row1)

            # Combine PostgreSQL and ClickHouse results
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
                "data": processed_data
            }

            return data
        else:
            return {"detail": "No data found for the provided IIN."}

    else:
        raise HTTPException(status_code=500, detail="Failed to connect to the database")



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
            groupArray(tuple(fr.parent_iin, fr.parent_fio, fr.parent_birth_date, fr.relative_type,nb.phonenumber_)) AS relatives
        FROM 
            ser.fl_relatives fr 
        LEFT JOIN db_fl_ul_dpar.numb AS nb ON fr.parent_iin = nb.iin_
        
        WHERE 
            fr.iin = %(iin)s
        GROUP BY 
            fr.iin
    """)

    try:
        result = client.query(query, parameters={'iin': iin})
        data = result.named_results()

        if not data:
            raise HTTPException(status_code=404, detail="No data found")

        # Process the results
        for row in data:
            relatives = [
                {
                    'IIN': relative[0],
                    'FIO': relative[1],
                    'BIRTH_DATE': relative[2],
                    'RELATIVE_TYPE': relative[3],
                    'PHONE_NUMBER': relative[4]
                }
                for relative in row['relatives']
            ]

            data2 = {
                "IIN": row['iin'],
                "STATUS": row['STATUS'],
                "RELATIVES": relatives
            }

        return data2

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/hello")
async def get_data2():
    return "hello world"
