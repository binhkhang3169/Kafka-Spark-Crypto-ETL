import psycopg2
from psycopg2 import sql

def check_dag_writes_to_postgres():
    try:
        # Kết nối đến PostgreSQL
        conn = psycopg2.connect(
            dbname="postgres",       # Tên cơ sở dữ liệu
            user="admin",         # Tên người dùng
            password="admin",     # Mật khẩu
            host="localhost",       # Nếu chạy từ máy chủ host, sử dụng localhost
            port=5432               # Cổng PostgreSQL được ánh xạ trong Docker Compose
        )
        cursor = conn.cursor()

        # Kiểm tra xem bảng binance_trades đã tồn tại chưa
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'warehouse' 
                AND table_name = 'binance_trades'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print("Bảng 'binance_trades' chưa tồn tại trong cơ sở dữ liệu.")
        else:
            # Nếu bảng tồn tại, kiểm tra xem có dữ liệu trong bảng hay không
            cursor.execute("SELECT COUNT(*) FROM binance_trades;")
            count = cursor.fetchone()[0]

            if count > 0:
                print(f"Bảng 'binance_trades' đã có dữ liệu. Số lượng bản ghi: {count}")
            else:
                print("Bảng 'binance_trades' tồn tại nhưng chưa có dữ liệu.")

        # Đóng cursor và kết nối
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Đã xảy ra lỗi khi kiểm tra dữ liệu: {e}")

# Gọi hàm kiểm tra
if __name__ == "__main__":
    check_dag_writes_to_postgres()