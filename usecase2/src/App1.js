import React, { useState } from 'react';
import './App.css';

function App() {
  const [inputValue, setInputValue] = useState('');
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async () => {
    if (!inputValue) return alert("Please enter an ID!");
    
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`http://127.0.0.1:5000/getClusterData/${inputValue}`);
      
      if (!res.ok) {
        throw new Error("Network response was not ok");
      }

      const data = await res.json();
      console.log(data)
      setResponse(data);
    } catch (error) {
      setError("Error fetching cluster data: " + error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        Walmart UseCase 2
      </header>
      <div className="App-body">
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
                  {Object.keys(response[0]).map((key) => (
                    <th key={key}>{key}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {response.map((item, index) => (
                  <tr key={index}>
                    {Object.values(item).map((value, idx) => (
                      <td key={idx}>{value}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;

