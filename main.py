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


@app.get("/get_data2/{iin}")
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
            nb.iin_ AS IIN,
            s.study_info AS STUDY,
            s2.school_info AS SCHOOL,
            ra.`Адрес на русском` AS ADDRESS,
            nb.phonenumber_ AS PHONE_NUMBER
        FROM db_fl_ul_dpar.numb AS nb 
        left join(
            SELECT 
                iin,
                groupArray(tuple(study_code AS study_code, study_name AS study_name, start_date AS start_date, end_date AS end_date)) AS study_info
            FROM 
                db_fl_ul_dpar.study
            GROUP BY 
                iin
        ) AS s ON s.iin = nb.iin_
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
            db_fl_ul_dpar.reg_address AS ra ON nb.iin_ = ra.`ИИН/БИН`
            
        WHERE 
            nb.iin_ = %(iin)s 
        SETTINGS join_algorithm = 'parallel_hash'
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


@app.get("/get_data/{iin}")
async def read_data(iin: str):
    try:
        client = clickhouse_connect.get_client(
            user='nifitest',
            password='nifitest',
            host='192.168.122.45',
            port=8123
        )

        query1 = """
        SELECT 
            dd.IIN IIN,
            dd.FIRSTNAME FIRSTNAME,
            dd.SURNAME SURNAME,
            dd.SECONDNAME SECONDNAME,
            CASE 
                WHEN dd.SEX_ID = '1' THEN 'Мужчина'
                WHEN dd.SEX_ID = '2' THEN 'Женщина'
            END AS SEX,
            dd.BIRTH_DATE BIRTH_DATE,
            dc.RU_NAME AS BIRTH_COUNTRY_RU,
            dc.KZ_NAME AS BIRTH_COUNTRY_KZ,
            dd2.RU_NAME AS BIRTH_DISTRICT_NAME_RU,
            dd2.KZ_NAME AS BIRTH_DISTRICT_NAME_KZ,
            dru.RU_NAME AS BIRTH_REGION_NAME_RU,
            dru.KZ_NAME AS BIRTH_REGION_NAME_KZ,
            dd.BIRTH_CITY BIRTH_CITY,
            dc2.RU_NAME AS CITIZENSHIP,
            n.RU_NAME AS NATIONALITY_RU,
            n.KZ_NAME AS NATIONALITY_KZ,
            dd.DOCUMENT_TYPE_ID DOCUMENT_TYPE,
            dd.DOCUMENT_NUMBER DOCUMENT_NUMBER,
            dd.DOCUMENT_BEGIN_DATE DOCUMENT_BEGIN_DATE,
            dd.DOCUMENT_END_DATE DOCUMENT_END_DATE,
            dd.ISSUE_ORGANIZATION_ID ISSUE_ORGANIZATION
        FROM db_fl_ul_dpar.damp_document dd 
        INNER JOIN db_fl_ul_dpar.DIC_COUNTRY dc ON dd.BIRTH_COUNTRY_ID = toString(dc.ID)
        INNER JOIN db_fl_ul_dpar.DIC_DISTRICTS dd2 ON dd.BIRTH_DISTRICTS_ID = toString(dd2.ID)
        INNER JOIN db_fl_ul_dpar.DIC_REGION_uniq dru ON dd.BIRTH_REGION_ID = toString(dru.ID)
        INNER JOIN db_fl_ul_dpar.DIC_COUNTRY dc2 ON dd.CITIZENSHIP_ID = toString(dc2.ID)
        INNER JOIN db_fl_ul_dpar.nationality n ON dd.NATIONALTY_ID = toString(n.ID) AND dd.SEX_ID = n.SEX
        WHERE dd.IIN = %(iin)s 
        AND dd.DOCUMENT_TYPE_ID = 'УДОСТОВЕРЕНИЕ РК'
        ORDER BY dd.DOCUMENT_BEGIN_DATE DESC
        LIMIT 1
        """

        query2 = """
        SELECT 
            nb.iin_ AS IIN,
            s.study_info AS STUDY,
            s2.school_info AS SCHOOL,
            ra.`Адрес на русском` AS ADDRESS,
            nb.phonenumber_ AS PHONE_NUMBER
        FROM db_fl_ul_dpar.numb AS nb 
        LEFT JOIN (
            SELECT 
                iin,
                groupArray(tuple(study_code, study_name, start_date, end_date)) AS study_info
            FROM db_fl_ul_dpar.study
            GROUP BY iin
        ) AS s ON s.iin = nb.iin_
        LEFT JOIN (
            SELECT 
                iin,
                groupArray(tuple(school_code, school_name, start_date, end_date)) AS school_info
            FROM db_fl_ul_dpar.school
            GROUP BY iin
        ) AS s2 ON s.iin = s2.iin
        LEFT JOIN db_fl_ul_dpar.reg_address AS ra ON nb.iin_ = ra.`ИИН/БИН`
        WHERE nb.iin_ = %(iin)s 
        SETTINGS join_algorithm = 'parallel_hash'
        """

        result1 = client.query(query1, parameters={'iin': iin})
        row = list(result1.named_results())  # Convert generator to list

        result2 = client.query(query2, parameters={'iin': iin})
        data2 = list(result2.named_results())  # Convert generator to list

        if row and data2 :
            processed_data = []
            for row1 in data2:
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

            data = {
                "IIN": row[0]['IIN'],
                "FIRSTNAME": row[0]['FIRSTNAME'],
                "SURNAME": row[0]['SURNAME'],
                "SECONDNAME": row[0]['SECONDNAME'],
                "SEX": row[0]['SEX'],
                "BIRTH_DATE": row[0]['BIRTH_DATE'],
                "BIRTH_COUNTRY_RU": row[0]['BIRTH_COUNTRY_RU'],
                "BIRTH_COUNTRY_KZ": row[0]['BIRTH_COUNTRY_KZ'],
                "BIRTH_DISTRICT_NAME_RU": row[0]['BIRTH_DISTRICT_NAME_RU'],
                "BIRTH_DISTRICT_NAME_KZ": row[0]['BIRTH_DISTRICT_NAME_KZ'],
                "BIRTH_REGION_NAME_RU": row[0]['BIRTH_REGION_NAME_RU'],
                "BIRTH_REGION_NAME_KZ": row[0]['BIRTH_REGION_NAME_KZ'],
                "BIRTH_CITY": row[0]['BIRTH_CITY'],
                "CITIZENSHIP": row[0]['CITIZENSHIP'],
                "NATIONALITY_RU": row[0]['NATIONALITY_RU'],
                "NATIONALITY_KZ": row[0]['NATIONALITY_KZ'],
                "DOCUMENT_TYPE": row[0]['DOCUMENT_TYPE'],
                "DOCUMENT_NUMBER": row[0]['DOCUMENT_NUMBER'],
                "DOCUMENT_BEGIN_DATE": row[0]['DOCUMENT_BEGIN_DATE'],
                "DOCUMENT_END_DATE": row[0]['DOCUMENT_END_DATE'],
                "ISSUE_ORGANIZATION": row[0]['ISSUE_ORGANIZATION'],
                "data": processed_data
            }

            return data
        else:
            raise HTTPException(status_code=404, detail="No data found for the provided IIN.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

#new get relatives
# @app.get("/get_relatives/{iin}")
# async def get_relatives(iin: str):
#     if len(iin) != 12:
#         raise HTTPException(status_code=404, detail="IIN Length should be 12")
#
#     query = ("""
#         SELECT
#             fr.iin,
#
#             CASE
#                 WHEN MAX(LENGTH(fr.marriage_divorce_date)) > 1 THEN 'Divorced'
#                 WHEN MAX(LENGTH(fr.marriage_reg_date)) > 1 THEN 'Married'
#                 ELSE 'Single'
#             END AS STATUS,
#             groupArray(tuple(fr.parent_iin, fr.parent_fio, fr.parent_birth_date, fr.relative_type,nb.phonenumber_
#             , ra.`Адрес на русском`,
#             dc.RU_NAME AS COUNTRY_RU_NAME,
#             dd2.RU_NAME AS BIRTH_DISTRICT_RU_NAME,
#             dr1.RU_NAME AS BIRTH_REGION_RU_NAME,
#             fp.BIRTH_CITY
#             )) AS relatives
#         FROM
#             ser.fl_relatives fr
#         LEFT JOIN ser.fl_person AS fp ON fr.parent_iin = fp.IIN
#         LEFT JOIN db_fl_ul_dpar.numb AS nb ON fr.parent_iin = nb.iin_
#         LEFT JOIN db_fl_ul_dpar.reg_address AS ra ON fr.parent_iin = ra.`ИИН/БИН`
#         LEFT JOIN db_fl_ul_dpar.DIC_DISTRICTS AS dd2 ON fp.BIRTH_DISTRICTS_ID = CAST(dd2.ID AS String)
#         LEFT JOIN db_fl_ul_dpar.DIC_REGION AS dr1 ON fp.BIRTH_REGION_ID = CAST(dr1.ID AS String)
#         LEFT JOIN db_fl_ul_dpar.DIC_COUNTRY AS dc ON fp.CITIZENSHIP_ID = CAST(dc.ID AS String)
#         WHERE
#             fr.iin = %(iin)s
#         GROUP BY
#             fr.iin
#         SETTINGS join_algorithm = 'parallel_hash'
#     """)
#
#     result = client.query(query, parameters={'iin': iin})
#     if not result.result_rows:
#         raise HTTPException(status_code=404, detail="Data not found")
#     return result.named_results()


@app.get("/hello")
async def get_data2():
    return "hello world"

#
# @app.get("/get_relatives2/{iin}")
# async def get_relatives(iin: str):
#     if len(iin) != 12:
#         raise HTTPException(status_code=404, detail="IIN Length should be 12")
#
#     query = ("""
#         SELECT
#             fr.iin,
#
#             CASE
#                 WHEN MAX(LENGTH(fr.marriage_divorce_date)) > 1 THEN 'Divorced'
#                 WHEN MAX(LENGTH(fr.marriage_reg_date)) > 1 THEN 'Married'
#                 ELSE 'Single'
#             END AS STATUS,
#             groupArray(tuple(fr.parent_iin, fr.parent_fio, fr.parent_birth_date, fr.relative_type,nb.phonenumber_
#             , ra.`Адрес на русском`,
#             dc.RU_NAME AS COUNTRY_RU_NAME,
#             dd2.RU_NAME AS BIRTH_DISTRICT_RU_NAME,
#             dr1.RU_NAME AS BIRTH_REGION_RU_NAME,
#             fp.BIRTH_CITY
#             )) AS relatives
#         FROM
#             ser.fl_relatives fr
#         LEFT JOIN ser.fl_person 				AS fp  ON fr.parent_iin 		= fp.IIN
#         LEFT JOIN db_fl_ul_dpar.numb 			AS nb  ON fr.parent_iin 		= nb.iin_
#         LEFT JOIN db_fl_ul_dpar.reg_address 	AS ra  ON fr.parent_iin 		= ra.`ИИН/БИН`
#         LEFT JOIN db_fl_ul_dpar.DIC_DISTRICTS 	AS dd2 ON fp.BIRTH_DISTRICTS_ID = CAST(dd2.ID AS String)
#         LEFT JOIN db_fl_ul_dpar.DIC_REGION_uniq AS dr1 ON fp.BIRTH_REGION_ID 	= CAST(dr1.ID AS String)
#         LEFT JOIN db_fl_ul_dpar.DIC_COUNTRY 	AS dc  ON fp.CITIZENSHIP_ID 	= CAST(dc.ID AS String)
#         WHERE
#             fr.iin = %(iin)s
#         GROUP BY
#             fr.iin
#         SETTINGS join_algorithm = 'parallel_hash'
#     """)
#
#     try:
#         result = client.query(query, parameters={'iin': iin})
#         data = result.named_results()
#
#         if not data:
#             raise HTTPException(status_code=404, detail="No data found")
#
#         # Process the results
#         processed_data = []
#         for row in data:
#             relatives = [
#                 {
#                     'IIN': relative[0],
#                     'FIO': relative[1],
#                     'BIRTH_DATE': relative[2],
#                     'RELATIVE_TYPE': relative[3],
#                     'PHONE_NUMBER': relative[4],
#                     'REG_ADDRESS': relative[5],
#                     'COUNTRY': relative[6],
#                     'DISTRICT': relative[7],
#                     'REGION': relative[8],
#                     'CITY': relative[9],
#                 }
#                 for relative in row['relatives']
#             ]
#
#             data2 = {
#                 "IIN": row['fr.iin'],
#                 "STATUS": row['STATUS'],
#                 "RELATIVES": relatives
#             }
#             processed_data.append(data2)
#
#         return processed_data
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
