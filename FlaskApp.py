from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
import pandas as pd
import networkx as nx

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# MySQL connection function
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root123$",  # Replace with your actual password
        database="usecase2"
    )

@app.route('/getClusterData/<cluster_id>', methods=['GET'])
def get_cluster_data(cluster_id):
    # Connect to the MySQL database
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # Query to fetch rows with the specified cluster_id
    query = "SELECT * FROM Explanation WHERE cluster_id = %s"
    cursor.execute(query, (cluster_id,))
    
    # Fetch all matching rows
    rows = cursor.fetchall()

    # Convert the result to a DataFrame
    df = pd.DataFrame(rows)
    
    # Separate the DataFrame into two parts
    df1 = df[['tid1', 'name1', 'address1', 'phone1', 'p2pe1', 'email1']]
    df2 = df[['tid2', 'name2', 'address2', 'phone2', 'p2pe2', 'email2']]
    
    # Rename columns to have consistent names
    df1.columns = ['tid', 'name', 'address', 'phone', 'p2pe', 'email']
    df2.columns = ['tid', 'name', 'address', 'phone', 'p2pe', 'email']
    
    # Concatenate and drop duplicates
    node_df = pd.concat([df1, df2]).drop_duplicates(subset=['tid']).reset_index(drop=True)
    top_3_names_with_counts = node_df['name'].value_counts().head(3).items()
    top_3_list_name = list(top_3_names_with_counts)

    top_3_phone_with_counts = node_df['phone'].value_counts().head(3).items()
    top_3_list_phone = list(top_3_phone_with_counts)

    top_3_email_with_counts = node_df['email'].value_counts().head(3).items()
    top_3_list_email = list(top_3_email_with_counts)
    
    # Convert the node DataFrame to a dictionary format
    node_details = node_df.to_dict(orient='records')
    
    # Extract edges for the graph
    edges = [(row['tid1'], row['tid2'], row['explanation']) for row in rows]
    
    # Create a graph
    graph = nx.Graph()
    graph.add_edges_from([(edge[0], edge[1]) for edge in edges])
    
    # Create a list of nodes and edges to return
    node_list = list(graph.nodes)
    edge_list = [(edge[0], edge[1], edge[2]) for edge in edges]  # include explanation
    
    # Return data, nodes, edges, and node details in JSON format
    return jsonify({
        'data': df.to_dict(orient='records'),
        'nodes': node_list,
        'edges': edge_list,
        'node_Details': node_details,
        'top_3_names' : top_3_list_name,
        'top_3_phone' : top_3_list_phone,
        'top_3_email' : top_3_list_email  # List of unique nodes with details
    })

if __name__ == '__main__':
    app.run(debug=True)
