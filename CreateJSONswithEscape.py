import json
import boto3
import mysql.connector
from datetime import datetime

import csv

client = boto3.client('secretsmanager')
response = client.get_secret_value(SecretId='mpos')
secret_dict = json.loads(response['SecretString'])

# setup connections
db = mysql.connector.connect(
    # host="vf-cm-dev-nora-mpos-cluster-db-1.cveuijr4mjrq.us-east-1.rds.amazonaws.com",
    # port="3306",
    # user="george",
    # passwd="GEORGE@123",
    # database="orderhub"
    host=secret_dict['host'],
    port=secret_dict['port'],
    user=secret_dict['username'],
    passwd=secret_dict['password'],
    database="dbname"

)

# Create cursor
mycursor = db.cursor()

# Create command to extract data, this will create the data in a tuple
# Extract TNF data to be processed in jMeter
mycursor.execute("""select store, workstation_number, sequence_number, source_txn 
                    from audit_transmission 
                    where store between 400001 and 409999 
                        and protocol != 'DB' 
                        and date(transmission_ts) between '2022-10-01' and '2022-10-02'""")

with open('data_load.csv', 'w') as out_file:
    # First write the header
    csv_write = csv.writer(out_file, delimiter=',')
    fieldnames = ['json']
    auth_write = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=',')
    auth_write.writeheader()

    # We could use the enumerate function which will return 2 values the index and the value
    for index, item in enumerate(mycursor):
        # create file name
        new_file = str(item[0]) + '-' + str(item[1]) + '-' + str(item[2]) + '.json'
        file_name = str(item[0]) + '-' + str(item[1]) + '-' + str(item[2])
        f_name = {'json': file_name}
        auth_write.writerow(f_name)
        # print(str(item[3]))


        # Open that file to create the json
        with open(new_file, 'w') as wf:
            # wf.write(str(item[3]) + '\n')
            # json.dump(item[3])
            print(json.dumps(item[3], separators=(',' ':')), file=wf)
            # print(str(item[3]))




