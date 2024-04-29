import io
import os
import PyPDF2
import pandas as pd
from pdfminer.high_level import extract_text
import snowflake_connection as db_conn


# Extract text from pdf
def extract_lines_with_keywords(pdf_path, keywords):
    extracted_lines = []
    reason=[]
    extracted_text = extract_text(pdf_path)
    lines = extracted_text.split('\n')

    # count=0
    # for i in range(len(lines)-1):
    #     if "SUMMARY OF SOFT CREDIT INQUIRY" in lines[i] :
    #         count=count+1
    #         break
    # if count>0:
    #     for i in range(len(lines) - 1):
    #         if "SUMMARY OF SOFT CREDIT INQUIRY" in lines[i]:
    #             if "21@10aca187647fb20cee4ffc1908544d6441bd5035" in lines[i + 2]:
    #                 extracted_lines.append(lines[i + 6])
    #             elif "23@6409402da38cd6fa59967d6777de2540094d097f" in lines[i + 2]:
    #                 extracted_lines.append(lines[i + 6])
    #             else:
    #                 extracted_lines.append(lines[i + 2])
    # else:
    #     extracted_lines.append(None)

    count=0
    for i in range(len(lines)-1):
        if "Your credit score:" in lines[i] :
            count=count+1
            break
    if count>0:
        for i in range(len(lines) - 1):
            if "Your credit score:" in lines[i]:
                extracted_lines.append(lines[i])
    else:
        extracted_lines.append(None)


    for line in lines:
        for keyword in keywords:
            if keyword.lower() in line.lower():
                extracted_lines.append(line)
                break

    # a = pdf_loan_id(pdf_path)
    # extracted_lines.insert(0, a)

    flag = False
    for line in lines:
        if "the following reason(s):" in line:
            flag = True
            continue
        elif "Our credit decision was" in line:
            flag = False
            break

        elif "If you have any questions" in line:
            flag = False
            break

        if flag == True:
            reason.append(line)

    denial_reason = ' | '.join(reason)
    extracted_lines.append(denial_reason)

    Parsed_json = {"Score": extracted_lines[0], "Name": extracted_lines[1], "Address": extracted_lines[2], "Denial_reasons" : extracted_lines[3]}

    return Parsed_json

# Open and process each pdf
def process_pdfs(pdf_folder, keywords):
    series_list = []
    for filename in os.listdir(pdf_folder):
        if filename.endswith('.pdf'):
            pdf_path = os.path.join(pdf_folder, filename)
            extracted_lines = extract_lines_with_keywords(pdf_path, keywords)
            series_list.append(extracted_lines)
    return series_list

# def pdf_loan_id(pdf_path):
#     file_name = os.path.basename(pdf_path)
#     a = file_name.split('_')[0]
#     print(a)
#     return a

pdf_folder = ' '
keywords = ['Applicant\'s Name:', 'Property Address:']
extracted_data = process_pdfs(pdf_folder, keywords)


def strr(x):
    if x is not None :
        x = x.split(':')[1]
        return x

df['Name']=df['Name'].apply(lambda x: strr(x))
df['Score']=df['Score'].apply(lambda x: strr(x))
df['Address']=df['Address'].apply(lambda x: strr(x))


# Set up connection parameters


cursor = db_conn.cursor()

loan_ids = [816731,816264,816022,816034,816527]

where_clause = "doc_key IN ({})".format(",".join(map(str, loan_ids)))

def AAN_query():
    query = " SELECT * from ( select t1.LOAN_APPLICATION_ID, t1.applicant_id, t1.VANTAGE_SCORE, t1.FICO_SCORE, CONCAT(t2.first_name, ' ', t2.last_name) AS full_name, t2.phone_number AS phone_number , t2.email AS Email , CONCAT(h.address,' ',h.CITY,' ',h.STATE, h.ZIP_CODE) AS Address,t2.type, d.document_key as doc_key  FROM EXPERIAN_DATA_REPORT as t1 join applicant as t2 on t1.applicant_id=t2.id left join home as h on h.loan_application_id=t1.loan_application_id left join document_ack as d on d.applicant_id=t2.id WHERE t2.type='primary' AND t1.VANTAGE_SCORE IS NOT NULL AND d.document_name like 'AdverseActionNotice') WHERE {}".format(where_clause)
    cursor.execute(query)
    results = cursor.fetchall()
    df2= pd.DataFrame(results, columns=['LID','DOC_KEY','APPLICANT_ID','VANTAGE_RETOOL','FICO_RETOOL','NAME_RETOOL','PHONE_RETOOL','EMAIL_RETOOL','ADDRESS_RETOOL','TYPE'])

merged_df = pd.merge(df, df2, on=['DOC_KEY'])
merged_df.to_csv('merged_aws.csv')
print(merged_df)
cursor.close()
db_conn.conn.close()




