from fastapi import FastAPI, HTTPException
import requests
from fastapi.responses import StreamingResponse
import clickhouse_connect
import psycopg2
import io
# Initialize FastAPI app
app = FastAPI()

# Connect to ClickHouse
client = clickhouse_connect.get_client(user='nifitest',
                                       password='nifitest',
                                       host='192.168.122.45',
                                       port=8123)


DATABASE_CONFIG2 = {
        'database': 'etanu',
        'user': 'readonly_user',
        'password': '1q2w3e$R%T',
        'host': '192.168.122.110',
        'port': '5432'
}


def connect_to_db():
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG2)
        return conn
    except Exception as e:
        print("Unable to connect to the database:", e)
        return None


@app.get("/get_photo_png/{iin}")
async def read_photo(iin: str):
    conn = connect_to_db()
    if conn:
        cursor = conn.cursor()

        # Ensure IIN is dynamically handled in the query
        cursor.execute("""SELECT photo
                               FROM public.metadata_gallery mg
                               inner join metadata_person mp on mg."personId_id" = mp.id
                               where mp.iin = %s limit 1""", (iin,))
        row = cursor.fetchone()
        conn.close()

        if row is None:
            raise HTTPException(status_code=404, detail="Photo not found for the provided IIN")

        # Construct the full URL to access the image
        url = "https://192.168.122.110:9000/photos/" + row[0]
        try:
            response = requests.get(url, verify=False)
            if response.status_code == 200 and 'image' in response.headers['Content-Type']:
                return StreamingResponse(io.BytesIO(response.content), media_type='image/png')
            else:
                raise HTTPException(status_code=400, detail="Invalid response or content type")
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=500, detail=f"Error fetching the image: {str(e)}")
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
            replace(dd.BIRTH_DATE,'/','-')as BIRTH_DATE,
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

@app.get("/hello")
async def get_data2():
    return "hello world"
