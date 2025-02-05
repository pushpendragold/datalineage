from flask import Flask, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from collections import defaultdict, deque
import json

app = Flask(__name__)
CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///lineage.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Dataset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dataset_name = db.Column(db.String(200), nullable=False)
    type = db.Column(db.String(50), nullable=False)  # 'upstream' or 'downstream'
    path = db.Column(db.String(500), nullable=False)

class SchemaInfo(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    dataset_path = db.Column(db.String(500), nullable=False, unique=True)
    schema_definition = db.Column(db.Text, nullable=False)  # JSON string of schema information

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/schema')
def schema_view():
    return render_template('schema.html')

@app.route('/api/schema/<path:dataset_path>')
def get_schema(dataset_path):
    # Normalize the path
    dataset_path = '/' + dataset_path
    
    # Get schema info
    schema_info = SchemaInfo.query.filter_by(dataset_path=dataset_path).first()
    if not schema_info:
        return jsonify({'error': 'Schema not found'}), 404
    
    # Get upstream DAGs (datasets that this path depends on)
    upstream_dags = []
    dataset = Dataset.query.filter_by(path=dataset_path, type='downstream').first()
    if dataset:
        upstream_datasets = Dataset.query.filter_by(
            dataset_name=dataset.dataset_name,
            type='upstream'
        ).all()
        upstream_dags = [{'name': d.dataset_name, 'path': d.path} for d in upstream_datasets]
    
    return jsonify({
        'schema': json.loads(schema_info.schema_definition),
        'upstream_dags': upstream_dags
    })

@app.route('/api/schemas')
def get_all_schemas():
    schemas = SchemaInfo.query.all()
    return jsonify([{
        'path': schema.dataset_path,
        'schema': json.loads(schema.schema_definition)
    } for schema in schemas])

def get_topological_levels(nodes, edges):
    # Create adjacency list
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    # Build the graph and calculate in-degrees
    for edge in edges:
        from_node = edge['from']
        to_node = edge['to']
        graph[from_node].append(to_node)
        in_degree[to_node] += 1
        # Ensure both nodes are in the graph
        if from_node not in graph:
            graph[from_node] = []
        if to_node not in graph:
            graph[to_node] = []

    # Find nodes with no incoming edges (sources/raw data)
    queue = deque()
    for node in graph:
        if in_degree[node] == 0:
            queue.append(node)

    # Perform topological sort and assign levels
    levels = defaultdict(int)
    current_level = 0
    
    while queue:
        level_size = len(queue)
        for _ in range(level_size):
            node = queue.popleft()
            levels[node] = current_level
            
            # Process neighbors
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        current_level += 1

    return levels

@app.route('/api/lineage')
def get_lineage():
    try:
        print('Fetching lineage data...')
        datasets = Dataset.query.all()
        print(f'Found {len(datasets)} datasets')
        
        if not datasets:
            print('No datasets found in database')
            return jsonify({'error': 'No data available'}), 404

        # First, collect all unique datasets and their relationships
        dataset_info = {}  # dataset_name -> {node_id, upstream_paths, downstream_paths}
        dataset_relationships = defaultdict(set)  # dataset_name -> set of upstream dataset names

        # First pass: collect dataset information and relationships
        for dataset in datasets:
            # Initialize dataset info if not exists
            if dataset.dataset_name not in dataset_info:
                dataset_info[dataset.dataset_name] = {
                    'node_id': f'dataset_{dataset.dataset_name}',
                    'upstream_paths': set(),
                    'downstream_paths': set()
                }

            # Track paths by type
            if dataset.type == 'upstream':
                dataset_info[dataset.dataset_name]['upstream_paths'].add(dataset.path)
            else:  # downstream
                dataset_info[dataset.dataset_name]['downstream_paths'].add(dataset.path)
                # Find upstream datasets for this downstream dataset
                upstreams = Dataset.query.filter_by(
                    path=dataset.path,
                    type='upstream'
                ).all()
                for upstream in upstreams:
                    dataset_relationships[dataset.dataset_name].add(upstream.dataset_name)

        # Create nodes and edges
        nodes = []
        edges = []
        processed_edges = set()

        # Create nodes
        for dataset_name, info in dataset_info.items():
            # Determine dataset type based on relationships
            is_source = len(info['upstream_paths']) == 0 and len(info['downstream_paths']) > 0
            is_target = len(info['upstream_paths']) > 0 and len(info['downstream_paths']) == 0
            group = 'source' if is_source else 'target' if is_target else 'intermediate'

            # Determine layer based on paths
            layer = None
            for path in info['upstream_paths'].union(info['downstream_paths']):
                if '/bronze/' in path:
                    layer = 'bronze'
                    break
                elif '/silver/' in path:
                    layer = 'silver'
                    break
                elif '/gold/' in path:
                    layer = 'gold'
                    break
            
            nodes.append({
                'id': info['node_id'],
                'label': dataset_name,
                'group': f'{layer}_dataset' if layer else 'dataset',
                'title': f'Dataset: {dataset_name}\n'
                        f'Layer: {layer}\n'
                        f'Upstream Paths: {len(info["upstream_paths"])}\n'
                        f'Downstream Paths: {len(info["downstream_paths"])}'
            })

        # Create edges based on relationships
        for downstream_name, upstream_names in dataset_relationships.items():
            downstream_id = dataset_info[downstream_name]['node_id']
            for upstream_name in upstream_names:
                upstream_id = dataset_info[upstream_name]['node_id']
                edge_key = (downstream_id, upstream_id)  # Flipped direction
                if edge_key not in processed_edges:
                    edges.append({
                        'from': downstream_id,  # Downstream dataset is now the source
                        'to': upstream_id,      # Upstream dataset is now the target
                        'arrows': 'to'
                    })
                    processed_edges.add(edge_key)

        result = {'nodes': nodes, 'edges': edges}
        print(f'Returning {len(nodes)} nodes and {len(edges)} edges')
        return jsonify(result)

    except Exception as e:
        print(f'Error in get_lineage: {str(e)}')
        return jsonify({'error': str(e)}), 500

    except Exception as e:
        print(f'Error in get_lineage: {str(e)}')
        return jsonify({'error': str(e)}), 500

    except Exception as e:
        print(f'Error in get_lineage: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/lineage/dataset/<dataset_name>')
def get_dataset_details(dataset_name):
    try:
        print(f'Fetching details for dataset: {dataset_name}')
        # Get all paths for this dataset
        dataset_paths = Dataset.query.filter_by(dataset_name=dataset_name).all()
        
        if not dataset_paths:
            print(f'No paths found for dataset: {dataset_name}')
            return jsonify({'error': 'Dataset not found'}), 404

        # Organize paths by type
        paths = {'upstream': [], 'downstream': []}
        
        for path in dataset_paths:
            print(f'Processing path: {path.path} (type: {path.type})')
            # Get schema for this path
            schema_info = SchemaInfo.query.filter_by(dataset_path=path.path).first()
            
            if schema_info:
                try:
                    schema = json.loads(schema_info.schema_definition)
                    print(f'Found schema for path: {path.path}')
                except json.JSONDecodeError as e:
                    print(f'Error decoding schema for path {path.path}: {str(e)}')
                    schema = None
            else:
                print(f'No schema found for path: {path.path}')
                schema = None
            
            path_info = {
                'path': path.path,
                'schema': schema
            }
            paths[path.type].append(path_info)

        result = {
            'dataset_name': dataset_name,
            'paths': paths
        }
        
        print(f'Returning details for dataset {dataset_name} with {len(paths["upstream"])} upstream and {len(paths["downstream"])} downstream paths')
        return jsonify(result)

    except Exception as e:
        print(f'Error in get_dataset_details: {str(e)}')
        return jsonify({'error': str(e)}), 500

    except Exception as e:
        print(f'Error in get_dataset_details: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/lineage/dependencies/<path:dataset_path>')
def get_dataset_dependencies(dataset_path):
    # Normalize the path
    dataset_path = '/' + dataset_path

    def get_upstream_paths(path, visited=None):
        if visited is None:
            visited = set()
        if path in visited:
            return set()
        visited.add(path)
        
        upstream_datasets = Dataset.query.filter_by(
            dataset_name=Dataset.query.filter_by(path=path).first().dataset_name,
            type='upstream'
        ).all()
        
        paths = {d.path for d in upstream_datasets}
        for upstream_path in list(paths):
            paths.update(get_upstream_paths(upstream_path, visited))
        return paths

    def get_downstream_paths(path, visited=None):
        if visited is None:
            visited = set()
        if path in visited:
            return set()
        visited.add(path)
        
        downstream_datasets = Dataset.query.filter(
            Dataset.type == 'upstream',
            Dataset.path == path
        ).all()
        
        paths = {d.dataset_name for d in downstream_datasets}
        downstream_paths = set()
        for dataset_name in paths:
            downstream_path = Dataset.query.filter_by(
                dataset_name=dataset_name,
                type='downstream'
            ).first()
            if downstream_path:
                downstream_paths.add(downstream_path.path)
                downstream_paths.update(get_downstream_paths(downstream_path.path, visited))
        return downstream_paths

    # Get upstream and downstream paths
    upstream_paths = get_upstream_paths(dataset_path)
    downstream_paths = get_downstream_paths(dataset_path)
    all_paths = {dataset_path} | upstream_paths | downstream_paths

    # Get all relevant datasets
    datasets = Dataset.query.filter(Dataset.path.in_(all_paths)).all()
    
    nodes = []
    edges = []
    processed_nodes = set()
    processed_edges = set()

    # Create nodes
    for dataset in datasets:
        if dataset.path not in processed_nodes:
            nodes.append({
                'id': dataset.path,
                'label': dataset.dataset_name,
                'group': 'default',
                'path': dataset.path
            })
            processed_nodes.add(dataset.path)

    # Create edges
    for dataset in datasets:
        if dataset.type == 'downstream':
            upstreams = Dataset.query.filter_by(
                dataset_name=dataset.dataset_name,
                type='upstream'
            ).all()
            
            for upstream in upstreams:
                if upstream.path in all_paths:  # Only include edges between selected nodes
                    edge_key = (dataset.path, upstream.path)
                    if edge_key not in processed_edges:
                        edges.append({
                            'from': dataset.path,
                            'to': upstream.path,
                            'arrows': 'to'
                        })
                        processed_edges.add(edge_key)

    # Get levels for the filtered graph
    levels = get_topological_levels(nodes, edges)

    # Update nodes with level information
    for node in nodes:
        level = levels[node['id']]
        node['level'] = level
        if node['id'] == dataset_path:
            node['group'] = 'selected'
        elif node['id'] in upstream_paths:
            node['group'] = 'upstream'
        elif node['id'] in downstream_paths:
            node['group'] = 'downstream'
        else:
            # This shouldn't happen but just in case
            node['group'] = 'default'

    return jsonify({'nodes': nodes, 'edges': edges})

@app.route('/api/lineage/path_view')
def get_path_centric_lineage():
    try:
        print('Fetching path-centric lineage data...')
        datasets = Dataset.query.all()
        print(f'Found {len(datasets)} datasets')
        
        if not datasets:
            print('No datasets found in database')
            return jsonify({'error': 'No data available'}), 404

        # First, collect all unique paths and datasets
        unique_paths = {}
        unique_datasets = {}
        dataset_to_paths = defaultdict(list)  # dataset_name -> [(path, type), ...]

        for dataset in datasets:
            # Track unique paths
            if dataset.path not in unique_paths:
                unique_paths[dataset.path] = f'path_{dataset.path}'
            
            # Track unique datasets
            dataset_key = dataset.dataset_name
            if dataset_key not in unique_datasets:
                unique_datasets[dataset_key] = f'dataset_{dataset.dataset_name}'
            
            # Track which paths each dataset appears in
            dataset_to_paths[dataset.dataset_name].append((dataset.path, dataset.type))

        # Create nodes and edges
        nodes = []
        edges = []
        processed_edges = set()

        # Create path nodes
        for path, path_id in unique_paths.items():
            nodes.append({
                'id': path_id,
                'label': path,
                'group': 'path',
                'shape': 'box',
                'title': f'Path: {path}'
            })

        # Create dataset nodes
        for dataset_name, dataset_id in unique_datasets.items():
            nodes.append({
                'id': dataset_id,
                'label': dataset_name,
                'group': 'dataset',
                'shape': 'ellipse',
                'title': f'Dataset: {dataset_name}'
            })

        # Create edges to show the flow
        for dataset_name, paths_and_types in dataset_to_paths.items():
            dataset_id = unique_datasets[dataset_name]
            
            # Group paths by type
            upstream_paths = [p for p, t in paths_and_types if t == 'upstream']
            downstream_paths = [p for p, t in paths_and_types if t == 'downstream']
            
            # Connect upstream paths to dataset
            for path in upstream_paths:
                path_id = unique_paths[path]
                edge_key = (path_id, dataset_id)
                if edge_key not in processed_edges:
                    edges.append({
                        'from': path_id,
                        'to': dataset_id,
                        'arrows': 'to'
                    })
                    processed_edges.add(edge_key)
            
            # Connect dataset to downstream paths
            for path in downstream_paths:
                path_id = unique_paths[path]
                edge_key = (dataset_id, path_id)
                if edge_key not in processed_edges:
                    edges.append({
                        'from': dataset_id,
                        'to': path_id,
                        'arrows': 'to'
                    })
                    processed_edges.add(edge_key)

        result = {'nodes': nodes, 'edges': edges}
        print(f'Returning {len(nodes)} nodes and {len(edges)} edges')
        return jsonify(result)

    except Exception as e:
        print(f'Error in get_path_centric_lineage: {str(e)}')
        return jsonify({'error': str(e)}), 500

        result = {'nodes': nodes, 'edges': edges}
        print(f'Returning {len(nodes)} nodes and {len(edges)} edges')
        return jsonify(result)

        result = {'nodes': nodes, 'edges': edges}
        print(f'Returning {len(nodes)} nodes and {len(edges)} edges')
        return jsonify(result)

        result = {'nodes': nodes, 'edges': edges}
        print(f'Returning {len(nodes)} nodes and {len(edges)} edges')
        return jsonify(result)

    except Exception as e:
        print(f'Error in get_path_centric_lineage: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/raw_data')
def get_raw_data():
    try:
        # Get all datasets
        datasets = Dataset.query.all()
        dataset_list = [{
            'id': dataset.id,
            'dataset_name': dataset.dataset_name,
            'type': dataset.type,
            'path': dataset.path
        } for dataset in datasets]

        # Get all schema information
        schemas = SchemaInfo.query.all()
        schema_list = [{
            'id': schema.id,
            'dataset_path': schema.dataset_path,
            'schema_definition': json.loads(schema.schema_definition)
        } for schema in schemas]

        return jsonify({
            'datasets': dataset_list,
            'schemas': schema_list
        })
    except Exception as e:
        print(f'Error fetching raw data: {str(e)}')
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, port=5001)
