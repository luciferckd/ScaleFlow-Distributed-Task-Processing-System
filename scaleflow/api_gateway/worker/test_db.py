
from database import get_db_connection

if __name__ == "__main__":
    connection = get_db_connection()

    cursor = connection.cursor()
    cursor.execute("SELECT DATABASE()")

    result = cursor.fetchone()
    print("Connected Database:", result[0])

    cursor.close()
    connection.close()
