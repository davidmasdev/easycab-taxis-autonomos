import React, { useEffect, useState } from 'react';

const API_URL = process.env.REACT_APP_AUDIT_API_URL || "http://localhost:7071";

function AuditLog() {
  const [events, setEvents] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    let isMounted = true;
    const fetchAudit = () => {
      fetch(`${API_URL}/audit`)
        .then(res => {
          if (!res.ok) throw new Error('No se pudo obtener el log de auditoría');
          return res.json();
        })
        .then(data => {
          if (isMounted) {
            setEvents(data.events.sort((a, b) => b.timestamp.localeCompare(a.timestamp)));
            setError(null);
          }
        })
        .catch(err => {
          if (isMounted) setError(err.message);
        });
    };
    fetchAudit();
    const interval = setInterval(fetchAudit, 2000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  return (
    <div style={{ padding: 24 }}>
      <h2>Auditoría de Seguridad</h2>
      {error ? (
        <div style={{ color: 'red' }}>Error: {error}</div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ border: '1px solid #ccc', padding: 8 }}>Fecha/Hora</th>
              <th style={{ border: '1px solid #ccc', padding: 8 }}>IP</th>
              <th style={{ border: '1px solid #ccc', padding: 8 }}>Acción</th>
              <th style={{ border: '1px solid #ccc', padding: 8 }}>Descripción</th>
            </tr>
          </thead>
          <tbody>
            {events.map((event, idx) => (
              <tr key={idx}>
                <td style={{ border: '1px solid #ccc', padding: 8 }}>{event.timestamp}</td>
                <td style={{ border: '1px solid #ccc', padding: 8 }}>{event.ip}</td>
                <td style={{ border: '1px solid #ccc', padding: 8 }}>{event.action}</td>
                <td style={{ border: '1px solid #ccc', padding: 8 }}>{event.description}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

export default AuditLog; 