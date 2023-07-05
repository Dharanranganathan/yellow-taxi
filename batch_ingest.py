import csv
import happybase
import time

batch_size = 1000
host = "0.0.0.0"
file_path = "yellow_tripdata_2017-03.csv"
namespace = "tripdata"
row_count = 0
start_time = time.time()
table_name = "yellow_tripdata"


def connect_to_hbase():
    conn = happybase.Connection(host = host,
        table_prefix = namespace,
        table_prefix_separator = ":")
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch


def insert_row(batch, row):
    batch.put(row[0],{"TLC:VendorID":row[0],"TLC:tpep_pickup_datetime":row[1],"TLC:tpep_dropoff_datetime":row[2],
            "TLC:passenger_count":row[3],"TLC:trip_distance":row[4],"TLC:RatecodeID":row[5],"TLC:store_and_fwd_flag":row[6],
            "TLC:PULocationID":row[7],"TLC:DOLocationID":row[8],"TLC:payment_type":row[9],"TLC:fare_amount":row[10],"TLC:extra":row[11],
            "TLC:mta_tax":row[12],"TLC:tip_amount":row[13],"TLC:tolls_amount":row[14],"TLC:improvement_surcharge":row[15],"TLC:total_amount":row[16],
            "TLC:congestion_surcharge":row[17],"TLC:airport_fee":row[18]})


def read_csv():
    csvfile = open(file_path, "r")
    csvreader = csv.reader(csvfile)
    return csvreader, csvfile


# After everything has been defined, run the script.
conn, batch = connect_to_hbase()
print("successfully Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size))
csvreader, csvfile = read_csv()
print("Connected to file. name: %s" % (file_path))

try:
    # Loop through the rows. The first row contains column headers, so skip that
    # row. Insert all remaining rows into the database.
    for row in csvreader:
        row_count += 1
        if row_count == 1:
            pass
        else:
            insert_row(batch, row)

    # If there are any leftover rows in the batch, send them now.
    batch.send()
finally:
    # No matter what happens, close the file handle.
    csvfile.close()
    conn.close()

duration = time.time() - start_time
print("File loaded successfully with, row count: %i, duration: %.3f s" % (row_count, duration))
