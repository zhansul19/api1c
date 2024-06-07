from fastapi import FastAPI, HTTPException
import clickhouse_connect

# Initialize FastAPI app
app = FastAPI()

# Connect to ClickHouse
client = clickhouse_connect.get_client(user='nifitest',
                                       password='nifitest',
                                       host='192.168.122.45',
                                       port=8123)


# Define FastAPI endpoint
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


@app.get("/get_data2/{iin}")
async def get_data2(iin: str):
    return iin
  # query = (f'SELECT fpn.IIN,fpn.FIRSTNAME,fpn.SECONDNAME,nn.ID,fpn.SURNAME,fpn.SEX_ID,fpn.CITIZENSHIP_ID, MAX(dd.DOCUMENT_NUMBER) as DOCUMENT_NUMBER '
    #          f'FROM ser.fl_person_new as fpn '
    #          f'inner join db_fl_ul_dpar.damp_document as dd on fpn.IIN = dd.IIN '
    #          f'inner join db_fl_ul_dpar.nationality_MENS as nn on fpn.NATIONALTY_ID = CAST(nn.ID as String) '
    #          f'WHERE fpn.IIN like \'{iin}\' and dd.DOCUMENT_TYPE_ID like \'УДОСТОВЕРЕНИЕ РК\' '
    #          f'GROUP BY fpn.IIN, fpn.FIRSTNAME, fpn.SECONDNAME, fpn.SURNAME, fpn.SEX_ID, fpn.CITIZENSHIP_ID, nn.ID',)
