import React, { useState } from 'react';
import './App.css';
import Plot from 'react-plotly.js';
import * as d3 from 'd3';

function App() {
  const [inputValue, setInputValue] = useState('');
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const style1 = { width: '30%' };
  const style2 = { width: '60%' };
  const graphStyle1={display:'block'};
  const graphStyle2={display:'none'};
  const [style, setStyle] = useState(style1);
  const [graphStyle,setGraphStyle]=useState(graphStyle2);

  const handleSubmit = async () => {
    if (!inputValue) return alert("Please enter an ID!");

    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`http://127.0.0.1:5000/getClusterData/-${inputValue}`, {
  mode: 'cors'
});
      console.log(res);
      if (!res.ok) {
        if (res.status === 404) {
          throw new Error(`No data found for Cluster ID: ${inputValue}`);
        }
        throw new Error("Network response was not ok");
      }
      const data = await res.json();
      console.log(data);
      setStyle(style2);
      setGraphStyle(graphStyle1);
      setResponse(data);
    } catch (error) {
      setError("Error fetching cluster data: " + error.message);
    } finally {
      setLoading(false);
    }
  };

  const preparePlotlyData = () => {
  if (!response) return { nodeData: [], edgeData: [], midpoints: [] };

  const nodes = response.nodes.map(node => ({ id: node }));
  const edges = response.edges.map(([source, target, explanation]) => ({ source, target, explanation }));
  const nodeDetailsMap = Object.fromEntries(
    response.node_Details.map(detail => [detail.tid, detail])
  );

  // Use D3 to calculate node positions
  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink().id(d => d.id).distance(50))
    .force("charge", d3.forceManyBody().strength(-100))
    .force("center", d3.forceCenter(0, 0))
    .stop();

  // Run the simulation for a specified number of ticks
  for (let i = 0; i < 300; ++i) simulation.tick();

  // Map nodes to positions and create hover text with details
  const positionedNodes = nodes.map((node) => ({
  ...node,
  x: node.x || Math.random() * 10,
  y: node.y || Math.random() * 10,
  hoverText: nodeDetailsMap[node.id]
    ? `Name: ${nodeDetailsMap[node.id].name || 'N/A'}<br>` +
      `Address: ${nodeDetailsMap[node.id].address || 'N/A'}<br>` +
      `Phone: ${nodeDetailsMap[node.id].phone || 'N/A'}<br>` +
      `Email: ${nodeDetailsMap[node.id].email || 'N/A'}<br>` +
      `P2PE: ${nodeDetailsMap[node.id].p2pe || 'N/A'}`
    : 'No details available',
  displayText: node.id.slice(-3)  // Only last three characters of tid
}));




  const edgeData = edges.map(edge => {
    const sourceNode = positionedNodes.find(n => n.id === edge.source);
    const targetNode = positionedNodes.find(n => n.id === edge.target);
    return {
      x: [sourceNode.x, targetNode.x, null],
      y: [sourceNode.y, targetNode.y, null],
      explanation: edge.explanation,
    };
  });

  // Calculate midpoints for each edge
  const midpoints = edgeData.map(edge => ({
    x: (edge.x[0] + edge.x[1]) / 2,
    y: (edge.y[0] + edge.y[1]) / 2,
    explanation: edge.explanation,
  }));

  return { nodeData: positionedNodes, edgeData, midpoints };
};

const { nodeData, edgeData, midpoints } = preparePlotlyData();

return (
  <div className="App">
    <header className="App-header">
      <h3>Cluster Graph Visualization</h3>
      <img alt="logo" src="./logo.png"></img>
    </header>
    <div className="App-body" style={style}>
      <input
        type="text"
        placeholder="Enter Cluster ID"
        value={inputValue}
        onChange={(e) => setInputValue(e.target.value)}
      />
      <button onClick={handleSubmit}>Submit</button>
      {loading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {response && (
  <div className="response">
    <h2>Cluster Data for ID: {inputValue}</h2>
    <table>
      <thead>
        <tr>
          <th>tid1</th>
          <th>tid2</th>
          <th>explanation</th>
        </tr>
      </thead>
      <tbody>
        {response.data.map((item, index) => (
          <tr key={index}>
            <td>{item.tid1}</td>
            <td>{item.tid2}</td>
            <td>{item.explanation}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
)}
    </div>
    <div style={graphStyle}>
      <h2>Network Graph</h2>
      <Plot
  data={[
    {
      x: nodeData.map(node => node.x),
      y: nodeData.map(node => node.y),
      mode: 'markers+text',
      type: 'scatter',
      marker: { size: 30, color: 'blue' },
      text: nodeData.map(node => node.displayText),  // Display last three letters
      hovertext: nodeData.map(node => node.hoverText),  // Full details on hover
      hoverinfo: 'text',  // Use hovertext for hover information
      name: 'Nodes',
    },
    ...edgeData.map(edge => ({
      x: edge.x,
      y: edge.y,
      mode: 'lines',
      type: 'scatter',
      line: { color: '#ccc', width: 2 },
      hoverinfo: 'none',
    })),
    {
      x: midpoints.map(mp => mp.x),
      y: midpoints.map(mp => mp.y),
      mode: 'markers',
      type: 'scatter',
      marker: { size: 10, color: 'red', symbol: 'circle' },
      hoverinfo: 'text',
      hovertext: midpoints.map(mp => mp.explanation),
      name: 'Edge Midpoints',
    },
  ]}
  layout={{
    title: 'Interactive Graph with Edge Descriptions',
    width: 1200,
    height: 600,
    showlegend: false,
    xaxis: { showgrid: false, zeroline: false, showticklabels: false },
    yaxis: { showgrid: false, zeroline: false, showticklabels: false },
    hovermode: 'closest',
  }}
  config={{ responsive: true }}
/>

    </div>
  </div>
);

}

export default App;
