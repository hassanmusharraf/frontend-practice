# import cx_Oracle

# # Initialize the Oracle client with the correct Instant Client directory.
# cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")

# conn = None
# cursor = None

# try:
#     # Create a DSN with your Oracle details.
#     dsn_tns = cx_Oracle.makedsn("10.0.5.152", 1521, service_name="ebs_TESTAPP")
    
#     # Connect to the Oracle database using your credentials.
#     conn = cx_Oracle.connect(user="APPS", password="appsa24ksa", dsn=dsn_tns)
#     cursor = conn.cursor()

#     # Call the Oracle function.
#     # Notice that we are expecting a VARCHAR2 return value, so we use cx_Oracle.STRING.
#     # Also, since the function doesn't accept any parameters, we don't pass any.
#     result = cursor.callfunc("ADS_ARAMEX_PO_SHIPPING_PKG.aramex_get_api_test_fun", cx_Oracle.STRING)
    
#     print("Function output:", result)

# except cx_Oracle.DatabaseError as error:
#     print("Database error occurred:", error)
# finally:
#     if cursor:
#         cursor.close()
#     if conn:
#         conn.close()
import cx_Oracle
import datetime

# Initialize the Oracle client with your Instant Client directory.
cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_23_7")

conn = None
cursor = None

try:
    # Create a DSN (Data Source Name) using your Oracle connection details.
    dsn_tns = cx_Oracle.makedsn("10.0.5.152", 1521, service_name="ebs_TESTAPP")
    
    # Connect to the Oracle database using your credentials.
    conn = cx_Oracle.connect(user="APPS", password="appsa24ksa", dsn=dsn_tns)
    cursor = conn.cursor()

    # Prepare the INSERT statement with all the column names.
    insert_sql = """
    INSERT INTO ads_po_shipping_mapping_stg (
        CLIENT_PO_REF,
        ADES_PO_REFERENCE,
        CLIENT_PO_LINE_REF,
        ADES_PO_LINE_REF,
        SKU,
        QTY_SHIPPED,
        ARAMEX_SO_REF,
        ARAMEX_SO_LINE_REF,
        ARAMEX_CONSOLE_SHIPMENT,
        SHIPPING_REFERENCE,
        MODE_OF_TRANSPORT,
        FREIGHT_FORWARDER,
        ESTIMATED_DELIVERY_DATE,
        ARAMEX_ORDER_REF,
        ARAMEX_ORDER_LINE_REF,
        PO_HEADER_ID,
        PO_LINE_ID,
        LAST_UPDATE_DATE,
        LAST_UPDATED_BY,
        CREATION_DATE,
        CREATED_BY,
        LAST_UPDATE_LOGIN,
        STATUS,
        MESSAGE,
        REQUEST_ID
    )
    VALUES (
        :1, :2, :3, :4, :5, :6,
        :7, :8, :9, :10, :11, :12, :13, :14, :15, :16,
        :17, :18, :19, :20, :21, :22, :23, :24, :25
    )
    """
    
    # Define your sample data for each column.
    values = (
        "PO_REF_SAMPLE",              # CLIENT_PO_REF (VARCHAR2(100))
        "ADES_REF_SAMPLE",            # ADES_PO_REFERENCE (VARCHAR2(40))
        "PO_LINE_REF_SAMPLE",         # CLIENT_PO_LINE_REF (VARCHAR2(100))
        "ADES_PO_LINE_SAMPLE",        # ADES_PO_LINE_REF (VARCHAR2(40))
        "SKU_SAMPLE",                 # SKU (VARCHAR2(400))
        "Dummy",                           # QTY_SHIPPED (NUMBER)
        "ARAMEX_SO_SAMPLE",           # ARAMEX_SO_REF (VARCHAR2(100))
        "ARAMEX_SO_LINE_SAMPLE",      # ARAMEX_SO_LINE_REF (VARCHAR2(40))
        "ARAMEX_CONSOLE_SAMPLE",      # ARAMEX_CONSOLE_SHIPMENT (VARCHAR2(100))
        "SHIP_REF_SAMPLE",            # SHIPPING_REFERENCE (VARCHAR2(100))
        "Air",                        # MODE_OF_TRANSPORT (VARCHAR2(240))
        "FreightX",                   # FREIGHT_FORWARDER (VARCHAR2(240))
        datetime.date(2025, 5, 1),    # ESTIMATED_DELIVERY_DATE (DATE)
        "ARAMEX_ORDER_SAMPLE",        # ARAMEX_ORDER_REF (VARCHAR2(40))
        "ARAMEX_ORDER_LINE_SAMPLE",   # ARAMEX_ORDER_LINE_REF (VARCHAR2(40))
        1001,                         # PO_HEADER_ID (NUMBER)
        2001,                         # PO_LINE_ID (NUMBER)
        datetime.datetime.now(),      # LAST_UPDATE_DATE (DATE)
        123,                          # LAST_UPDATED_BY (NUMBER)
        datetime.datetime.now(),      # CREATION_DATE (DATE)
        123,                          # CREATED_BY (NUMBER)
        456,                          # LAST_UPDATE_LOGIN (NUMBER)
        "A",                          # STATUS (VARCHAR2(2))
        "Sample message",             # MESSAGE (VARCHAR2(4000))
        9999                          # REQUEST_ID (NUMBER)
    )

    # Execute the insert statement with the provided values.
    cursor.execute(insert_sql, values)
    
    # Commit the transaction to save the changes.
    conn.commit()
    
    print("Data inserted successfully!")

except cx_Oracle.DatabaseError as error:
    print("Database error occurred:", error)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
