import sqlite3
import threading
import time
import os

"""
Assignment: Distributed System Simulation (Python Developer Role)
Author: Harshal
Description: 
    This script simulates a distributed environment where Users, Products, and Orders 
    reside in isolated databases (Microservices pattern). It utilizes Python's 
    threading module to perform concurrent insertions and implements strict 
    application-layer validation to ensure data integrity without relying on 
    database constraints.
"""

# --- Configuration & Architecture Setup ---

# Simulating a microservices architecture by isolating data into distinct SQLite files.
# In a real-world scenario, these would likely be separate database instances.
DB_FILES = {
    'users': 'users.db',
    'products': 'products.db',
    'orders': 'orders.db'
}

# --- Data Injection (Source: Assignment Packet) ---

# Mock data for Users. Contains valid entries and specific edge cases (e.g., duplicates, missing values)
# to test the robustness of the validation logic.
users_data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com"),
    (4, "David", "david@example.com"),
    (5, "Eve", "eve@example.com"),
    (6, "Frank", "frank@example.com"),
    (7, "Grace", "grace@example.com"),
    (8, "Alice", "alice@example.com"),  # Edge Case: Potential duplicate email
    (9, "Henry", "henry@example.com"),
    (10, None, "jane@example.com")       # Edge Case: Integrity violation (Missing Name)
]

products_data = [
    (1, "Laptop", 1000.00),
    (2, "Smartphone", 700.00),
    (3, "Headphones", 150.00),
    (4, "Monitor", 300.00),
    (5, "Keyboard", 50.00),
    (6, "Mouse", 30.00),
    (7, "Laptop", 1000.00),      # Edge Case: Duplicate product entry
    (8, "Smartwatch", 250.00),
    (9, "Gaming Chair", 500.00),
    (10, "Earbuds", -50.00)      # Edge Case: Business Logic violation (Negative Price)
]

# Cleaned Orders data to test relationship integrity and concurrency.
orders_data_clean = [
    (1, 1, 1, 2), (2, 2, 2, 1), (3, 3, 3, 5),
    (4, 4, 1, 1), (5, 5, 3, 1), (6, 6, 4, 1),
    (7, 7, 2, 1), 
    (8, 8, 0, 0),      # Edge Case: Invalid quantity (0)
    (9, 1, 1, -1),     # Edge Case: Invalid quantity (-1)
    (10, 10, 11, 2)    # Edge Case: Foreign Key violation (Product 11 does not exist)
]


# --- Infrastructure Layer: Database Initialization ---

def init_dbs():
    """
    Initializes the storage layer.
    Ensures that the distributed database files exist and are clean for a fresh simulation run.
    """
    # Schema Definition: Users Service
    with sqlite3.connect(DB_FILES['users']) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
    
    # Schema Definition: Products Service
    with sqlite3.connect(DB_FILES['products']) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        
    # Schema Definition: Orders Service
    with sqlite3.connect(DB_FILES['orders']) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER)")
    
    # Reset state: clear old data to ensure test reproducibility.
    for db in DB_FILES.values():
        with sqlite3.connect(db) as conn:
            conn.execute(f"DELETE FROM {db.split('.')[0]}")
    print("System Status: Databases initialized and cleaned.\n")


# --- Business Logic Layer: Application Side Validation ---
# Note: As per requirements, we bypass SQL constraints to handle logic purely in Python.

def validate_user(user_id, name, email):
    """
    Validates user data integrity before persistence.
    Checks for null values and format correctness.
    """
    if not name:
        return False, f"User {user_id}: Name cannot be empty (Data Integrity Error)."
    if not "@" in email:
        return False, f"User {user_id}: Invalid email format."
    return True, "Valid"

def validate_product(prod_id, name, price):
    """
    Enforces pricing business rules.
    """
    if price < 0:
        return False, f"Product {prod_id}: Price cannot be negative ({price}) (Business Rule Violation)."
    return True, "Valid"

def validate_order(order_id, user_id, prod_id, quantity):
    """
    Validates order logic and simulates Cross-Service Communication.
    Instead of SQL Foreign Keys, we manually query the User and Product 'services' 
    to verify existence.
    """
    if quantity <= 0:
        return False, f"Order {order_id}: Quantity must be positive."
    
    # Simulated Service Call: Check User Service
    with sqlite3.connect(DB_FILES['users']) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM users WHERE id = ?", (user_id,))
        if not cursor.fetchone():
            return False, f"Order {order_id}: User {user_id} does not exist (Foreign Key Simulation)."

    # Simulated Service Call: Check Product Service
    with sqlite3.connect(DB_FILES['products']) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM products WHERE id = ?", (prod_id,))
        if not cursor.fetchone():
            return False, f"Order {order_id}: Product {prod_id} does not exist (Foreign Key Simulation)."
            
    return True, "Valid"


# --- Concurrency Layer: Worker Threads ---

def insert_user_worker(data):
    """
    Worker thread for handling User insertion transactions.
    """
    uid, name, email = data
    is_valid, msg = validate_user(uid, name, email)
    
    if is_valid:
        try:
            with sqlite3.connect(DB_FILES['users']) as conn:
                # Atomic insertion
                conn.execute("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", (uid, name, email))
                conn.commit()
            print(f"[Users DB] Inserted User {uid}: {name}")
        except Exception as e:
            print(f"[Users DB] Error inserting User {uid}: {e}")
    else:
        print(f"[Users DB] Validation Failed: {msg}")

def insert_product_worker(data):
    """
    Worker thread for handling Product insertion transactions.
    """
    pid, name, price = data
    is_valid, msg = validate_product(pid, name, price)
    
    if is_valid:
        try:
            with sqlite3.connect(DB_FILES['products']) as conn:
                conn.execute("INSERT INTO products (id, name, price) VALUES (?, ?, ?)", (pid, name, price))
                conn.commit()
            print(f"[Products DB] Inserted Product {pid}: {name}")
        except Exception as e:
            print(f"[Products DB] Error inserting Product {pid}: {e}")
    else:
        print(f"[Products DB] Validation Failed: {msg}")

def insert_order_worker(data):
    """
    Worker thread for Orders. 
    Requires prior successful execution of User/Product transactions.
    """
    oid, uid, pid, qty = data
    
    is_valid, msg = validate_order(oid, uid, pid, qty)
    
    if is_valid:
        try:
            with sqlite3.connect(DB_FILES['orders']) as conn:
                conn.execute("INSERT INTO orders (id, user_id, product_id, quantity) VALUES (?, ?, ?, ?)", (oid, uid, pid, qty))
                conn.commit()
            print(f"[Orders DB] Inserted Order {oid} for User {uid}")
        except Exception as e:
            print(f"[Orders DB] Error inserting Order {oid}: {e}")
    else:
        print(f"[Orders DB] Validation Failed: {msg}")


# --- Main Execution Orchestrator ---

def main():
    init_dbs()
    
    threads = []
    
    print("--- Starting Concurrent User & Product Insertions ---")
    
    # Spawning threads for Users and Products to run in parallel
    for u in users_data:
        t = threading.Thread(target=insert_user_worker, args=(u,))
        threads.append(t)
        t.start()
        
    for p in products_data:
        t = threading.Thread(target=insert_product_worker, args=(p,))
        threads.append(t)
        t.start()
        
    # Synchronization Barrier: 
    # We must wait for all User/Product threads to complete before processing Orders
    # to ensure reference integrity during the simulation.
    for t in threads:
        t.join()
        
    print("\n--- Starting Concurrent Order Insertions ---")
    
    order_threads = []
    for o in orders_data_clean:
        t = threading.Thread(target=insert_order_worker, args=(o,))
        order_threads.append(t)
        t.start()
        
    for t in order_threads:
        t.join()
        
    print("\n--- Final Data Verification ---")
    # Dumping DB contents to verify persistence
    for name, db_file in DB_FILES.items():
        print(f"\nContents of {db_file}:")
        with sqlite3.connect(db_file) as conn:
            rows = conn.execute(f"SELECT * FROM {name}").fetchall()
            for r in rows:
                print(r)

if __name__ == "__main__":
    main()