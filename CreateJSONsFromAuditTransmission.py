import json

import mysql.connector
from datetime import datetime

import csv

# setup connections
db = mysql.connector.connect(
    host="vf-cm-dev-nora-mpos-cluster-db-1.cveuijr4mjrq.us-east-1.rds.amazonaws.com",
    port="3306",
    user="george",
    passwd="GEORGE@123",
    database="orderhub"
)

# Create cursor
mycursor = db.cursor()

# Create command to extract data, this will create the data in a tuple
# mycursor.execute("""select store, workstation_number, sequence_number, source_txn from audit_transmission
# 	where store > 400000 and to_system = 'ESB' and workstation_number <> 0
#     and order_hub_eligible = 1 and date(transmission_ts) between '2022-10-01' and '2022-11-03'""")

# Extract TNF data to be processed in jMeter
mycursor.execute("""select store, workstation_number, sequence_number, source_txn 
                    from audit_transmission 
                    where store between 400001 and 409999 
                        and protocol != 'DB' 
                        and date(transmission_ts) between '2022-10-01' and '2022-11-03'""")

# # To extract TBL data for two weeks for UAT Test
# mycursor.execute("""select store, workstation_number, sequence_number, source_txn
#                     from audit_transmission
#                     where store between 200001 and 200999
#                         and to_system = 'ESB'
#                         and date(transmission_ts) between '2022-10-01' and '2022-11-03'""")


with open('data_load.csv', 'w', newline="") as out_file:
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


        # Open that file to create the json
        with open(new_file, 'w') as wf:
            wf.write(item[3] + '\n')



