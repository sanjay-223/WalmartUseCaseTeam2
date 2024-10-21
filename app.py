from flask import Flask, jsonify
from flask_cors import CORS
from flask_restx import Api, Resource, fields
import pandas as pd
import pyarrow.orc as orc
import os
import networkx as nx

app = Flask(__name__)
CORS(app)

# Initialize Flask-RESTx API for Swagger documentation
# Initialize Flask-RESTx API for Swagger documentation
api = Api(app, version="1.0", title="Cluster Data API",
          description="API to fetch cluster data and graph structure from ORC files.",
          doc='/swagger/')  # Change the 'doc' path to customize where Swagger UI is served


# Define a namespace
ns = api.namespace('clusters', description='Cluster operations')

# Define a model for the Swagger documentation
cluster_model = api.model('ClusterData', {
    'cluster_id': fields.String(description='Cluster ID to query', required=True),
})

# Define the output model for the nodes and edges in the graph
edge_model = api.model('Edge', {
    'tid1': fields.String(description='Source node'),
    'tid2': fields.String(description='Destination node')
})

node_edge_response = api.model('NodeEdgeResponse', {
    'data': fields.List(fields.Raw, description="The raw data from ORC files"),
    'nodes': fields.List(fields.String, description="List of nodes in the cluster"),
    'edges': fields.List(fields.Nested(edge_model), description="List of edges in the cluster")
})


@ns.route('/<string:cluster_id>')
@ns.param('cluster_id', 'The Cluster identifier')
class ClusterData(Resource):
    @ns.doc('get_cluster_data')
    @ns.response(404, 'Cluster not found')
    @ns.response(200, 'Cluster data retrieved')
    @ns.marshal_with(node_edge_response)
    def get(self, cluster_id):
        """
        Fetch cluster data and its corresponding nodes and edges from ORC files.
        """
        folder_path = f'/home/labuser/Desktop/Persistant_Folder/clusterDf/_temporary/0/_temporary/attempt_20241017142538490208898385350172_0004_m_000003_17/cluster_id=-{cluster_id}/'

        dataframes = []
        edges = []

        # Check if the directory exists
        if not os.path.exists(folder_path):
            api.abort(404, f"Directory for cluster_id {cluster_id} not found.")

        # Iterate through all files in the directory
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path) and file_name.endswith('.orc'):
                try:
                    # Read the ORC file
                    table = orc.read_table(file_path)
                    df = table.to_pandas()
                    dataframes.append(df)

                    # Extract edges from the DataFrame using tid1 and tid2
                    for _, row in df.iterrows():
                        edges.append((row['tid1'], row['tid2']))
                except Exception as e:
                    print(f"Error reading {file_name}: {e}")

        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            graph = nx.Graph()
            graph.add_edges_from(edges)  # Add edges to the graph

            # Create a list of nodes and edges to return
            node_list = list(graph.nodes)
            edge_list = list(graph.edges)

            return {
                'data': combined_df.to_dict(orient='records'),
                'nodes': node_list,
                'edges': [{'tid1': e[0], 'tid2': e[1]} for e in edge_list]
            }
        else:
            api.abort(404, "No ORC files found.")

# Add the namespace to the API
api.add_namespace(ns, path='/getClusterData')

if __name__ == '__main__':
    app.run(debug=True)
