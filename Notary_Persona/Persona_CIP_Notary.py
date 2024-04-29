import snowflake_connection as db_conn
import pandas as pd


def notary_data():
    notary_cursor = db_conn.conn.cursor()

    notary_sql = notary_cursor.execute("""select concat(app.first_name, ' ', app.last_name) as RNAME,
           to_char (app.DATE_OF_BIRTH, 'mm-dd-yyyy')as RDOB,
    concat(hh.address, ' ', hh.city, ' ', hh.state, ' ', hh.zip_code)as RADD,
    ana.LOAN_APPLICATION_ID as LID,
    ana.APPLICANT_ID as AID,
    to_char(date_trunc('DAY',ana.scheduled_session_start_time),'DD-MM-YYYY')as date,
    concat('https://app.withpersona.com/dashboard/inquiries/',ana.PERSONA_INQUIRY_ID) as PER_LINK,
    concat('https://s3.console.aws.amazon.com/s3/buckets/aven-prod-remote-notarization-recordings?region=us-west-2&prefix=',
        ana.SESSION_RECORDING_KEY) as AWS_LINK,ana.SESSION_RECORDING_KEY as KEY,
    ana.session_type as RON_RIN,
    la.lien_recording_status as RSTATUS,
    la.status as la_status,
    app.TYPE,
    n.name as NNAME,
    la.TYPE as APPLICATION_TYPE
    from applicant_notary_assignment ana
    left join notary n
    on ana.notary_id = n.id
    left join applicant app
    on app.id = ana.applicant_id
    left join loan_application la
    on la.id = app.loan_application_id
    left join fraud_flag ff
    on ff.loan_application_id = la.id
    left join HOME hh
    on la.ID = hh.LOAN_APPLICATION_ID
    where
      (ana.scheduled_session_start_time >= '2024-04-23':: date
      AND ana.scheduled_session_start_time < '2024-04-24':: date)
      and la_status LIKE 'approved'
      and flow_status LIKE 'sessionComplete'
      
      order by ana.scheduled_session_start_time desc, ana.ID asc""")

    notary_approved_df = pd.DataFrame(notary_sql.fetchall(), columns=[desc[0] for desc in notary_sql.description])
    # print(notary_approved_df.head())
    notary_cursor.close()

    return notary_approved_df



def persona_data():
    notary_approved_df = notary_data()
    notary_approved_LID = tuple(notary_approved_df["AID"].tolist())
    persona_cursor = db_conn.conn.cursor()
    # persona last updated report generated
    persona_sql_for_approved = "SELECT distinct LOAN_APPLICATION_ID as LID," \
                               " p.APPLICANT_ID as AID, " \
                               "CAST(try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['issue-date']['value'] as DATE )as ISS,"\
                               "CAST(try_parse_json(RESPONSE_JSON)['included'][0]['attributes']['expiration-date'] as DATE)as exp," \
                               "try_parse_json(RESPONSE_JSON)['data']['relationships']['verifications']['data'][3]['type'] as Gov," \
                               "try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['selected-id-class-1']['value'] as ID_TYPE,"\
                               "Case when try_parse_json(RESPONSE_JSON)['data']['attributes']['status'] = 'completed' then 'Completed'"\
                                "when try_parse_json(RESPONSE_JSON)['data']['attributes']['status'] = 'created' then 'Created'"\
                                "when try_parse_json(RESPONSE_JSON)['data']['attributes']['status'] = 'needs_review' then 'Needs review'"\
                                "when try_parse_json(RESPONSE_JSON)['data']['attributes']['status'] = 'approved' then 'Approved'"\
                                " else  'No status' end as PER_STATUS," \
                               " concat(try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['name-first']['value'], ' '," \
                               " try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['name-last']['value']) as PNAME, " \
                               "to_char(date_trunc('DAY',try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['birthdate']['value']::DATE),'mm-dd-yyyy' )as PDOB ," \
                               " concat(try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['address-street-1']['value'] ,' ',  " \
                               "try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['address-city']['value'], ' ', " \
                               "try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['address-subdivision']['value'], ' '," \
                               " try_parse_json(RESPONSE_JSON)['data']['attributes']['fields']['address-postal-code']['value'] )  as PADD  " \
                               " FROM PERSONA_DATA_REPORT p " \
                               "join ( select  q.APPLICANT_ID, max(q.UPDATED_AT) as lst" \
                               " from PERSONA_DATA_REPORT q group by APPLICANT_ID  )subquery" \
                               "  on p.APPLICANT_ID = subquery.APPLICANT_ID" \
                               " and p.UPDATED_AT = subquery.lst" \
                               " and p.APPLICANT_ID IN "
    print(persona_sql_for_approved)
    # "where try_parse_json(RESPONSE_JSON)['data']['attributes']['status'] in  ('completed')" \
    # "  and RESPONSE_JSON is not null and APPLICANT_ID IN "

    query1 = persona_sql_for_approved + str(notary_approved_LID)
    print(query1)
    persona_sql = persona_cursor.execute(query1)
    persona_result = persona_sql.fetchall()
    persona_df = pd.DataFrame(persona_result, columns=[desc[0] for desc in persona_sql.description])
    persona_cursor.close()
    return persona_df

def autoclear():
    autoclear_cursor = db_conn.conn.cursor()
    notary_approved_df = notary_data()
    notary_approved_LID = tuple(notary_approved_df["LID"].tolist())
    autoclear_sql = "SELECT DISTINCT loan_application_id as LID, " \
                    "operation_endpoint AS autoclear_status " \
                    "FROM operation_log " \
                    "WHERE operation_endpoint ILIKE '%autoclear%' " \
                    "AND action ILIKE 'clear_loan_application_dl_address_mismatch' " \
                    "AND loan_application_id IN"
    query1 = autoclear_sql + str(notary_approved_LID)
    autoclear_sql=autoclear_cursor.execute(query1)
    autoclear_result = autoclear_sql.fetchall()
    autoclear_df = pd.DataFrame(autoclear_result, columns=[desc[0] for desc in autoclear_sql.description])
    autoclear_cursor.close()
    return autoclear_df


class output_report:


    def __init__(self):
        self.final_df = None
        self.pre_final_df = notary_data()
        self.persona_data_df = persona_data()
        self.autoclear_data_df = autoclear()

    def output_gene(self):
        self.pre_final_df = self.pre_final_df.merge(
            self.persona_data_df[["AID", "PNAME", "PDOB", "PADD", "PER_STATUS",'ISS','EXP','GOV','ID_TYPE']], on='AID', left_index=False, right_index=False)
        print(self.pre_final_df)
        # print(self.pre_final_df.columns)
        self.prefinal1_df = self.pre_final_df.merge(
            self.autoclear_data_df[['LID','AUTOCLEAR_STATUS']], on='LID',how='left')
        print(self.prefinal1_df)
        self.final_df = self.prefinal1_df[['LID', 'AID', 'RNAME', 'PNAME', 'RDOB', 'PDOB', 'RADD',
                                           'PADD','APPLICATION_TYPE', 'TYPE', 'DATE','ISS','EXP','GOV','ID_TYPE', 'PER_LINK',
                                           'AWS_LINK', 'RON_RIN', 'LA_STATUS',
                                           'PER_STATUS','NNAME','AUTOCLEAR_STATUS']]
        # df['Address'] = df['Address'].str.rsplit('-', n=1).str[0]
        self.final_df['PADD'] = self.final_df['PADD'].str.rsplit("-", n=1).str[0]
        self.final_df.to_csv("notary_data.csv",index=False)
        print(len(self.final_df))

obj = output_report()
obj.output_gene()
# db_conn.conn.close()