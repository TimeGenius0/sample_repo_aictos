from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
import os
from typing import Dict, List, Optional

app = Flask(__name__)

def get_db_connection():
    """Create a database connection."""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='test_db',
            user='user',
            password='password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict]:
    """Execute a MySQL query and return results."""
    connection = get_db_connection()
    if not connection:
        return []
        
    try:
        cursor = connection.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            return results
        else:
            connection.commit()
            return [{'affected_rows': cursor.rowcount}]
            
    except Error as e:
        print(f"Error executing query: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/users', methods=['GET'])
def get_users():
    """Get all users from the database."""
    query = "SELECT * FROM users"
    results = execute_query(query)
    return jsonify(results)

@app.route('/users/<int:user_id>', methods=['GET'])
def get_user(user_id: int):
    """Get a specific user by ID."""
    query = "SELECT * FROM users WHERE id = %s"
    results = execute_query(query, (user_id,))
    if results:
        return jsonify(results[0])
    return jsonify({'error': 'User not found'}), 404

@app.route('/users', methods=['POST'])
def create_user():
    """Create a new user."""
    data = request.get_json()
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({'error': 'Missing required fields'}), 400
        
    query = "INSERT INTO users (name, email) VALUES (%s, %s)"
    results = execute_query(query, (data['name'], data['email']))
    return jsonify({'message': 'User created successfully'}), 201

@app.route('/users/<int:user_id>', methods=['PUT'])
def update_user(user_id: int):
    """Update a user's information."""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400
        
    updates = []
    params = []
    if 'name' in data:
        updates.append("name = %s")
        params.append(data['name'])
    if 'email' in data:
        updates.append("email = %s")
        params.append(data['email'])
        
    if not updates:
        return jsonify({'error': 'No valid fields to update'}), 400
        
    params.append(user_id)
    query = f"UPDATE users SET {', '.join(updates)} WHERE id = %s"
    results = execute_query(query, tuple(params))
    return jsonify({'message': 'User updated successfully'})

@app.route('/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id: int):
    """Delete a user."""
    query = "DELETE FROM users WHERE id = %s"
    results = execute_query(query, (user_id,))
    return jsonify({'message': 'User deleted successfully'})

@app.route('/users/search', methods=['GET'])
def search_users():
    """Search users by name or email."""
    name = request.args.get('name')
    email = request.args.get('email')
    
    conditions = []
    params = []
    if name:
        conditions.append("name LIKE %s")
        params.append(f"%{name}%")
    if email:
        conditions.append("email LIKE %s")
        params.append(f"%{email}%")
        
    if not conditions:
        return jsonify({'error': 'No search criteria provided'}), 400
        
    query = f"SELECT * FROM users WHERE {' OR '.join(conditions)}"
    results = execute_query(query, tuple(params))
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True) 