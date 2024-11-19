from models import init_db, SessionLocal

def test_database_connection():
    try:
        # Initialize database
        init_db()
        
        # Test connection by creating a session
        db = SessionLocal()
        print("Successfully connected to database!")
        
        # Clean up
        db.close()
        return True
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return False

if __name__ == "__main__":
    test_database_connection()
