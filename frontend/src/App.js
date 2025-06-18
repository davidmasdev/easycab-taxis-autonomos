import React, { useState, useEffect, useRef, useCallback } from "react";
import "./App.css";

function App() {
  const [gridData, setGridData] = useState(null);
  const [taxisData, setTaxisData] = useState([]);
  const [customersData, setCustomersData] = useState([]);
  const [locationsData, setLocationsData] = useState([]);
  const [cityState, setCityState] = useState(""); // Nuevo estado para el estado de la ciudad
  const [connectionStatus, setConnectionStatus] = useState({
    connected: false,
    message: "Conectando...",
  });
  const [lastUpdate, setLastUpdate] = useState("Nunca");
  const [animatedElements, setAnimatedElements] = useState(new Map());
  const [gridDimensions, setGridDimensions] = useState({
    rows: 0,
    cols: 0,
    cellWidth: 0,
    cellHeight: 0,
  });

  const gridRef = useRef(null);
  const intervalRef = useRef(null);

  const updateStatus = useCallback((isConnected, message) => {
    setConnectionStatus({ connected: isConnected, message });
    if (isConnected) {
      setLastUpdate(new Date().toLocaleTimeString());
    }
  }, []);

  const getCellPosition = useCallback((x, y) => {
    if (!gridRef.current) return { left: 0, top: 0, width: 0, height: 0 };

    const cell = gridRef.current.querySelector(`[data-cell="${y}-${x}"]`);
    if (!cell) return { left: 0, top: 0, width: 0, height: 0 };

    const gridRect = gridRef.current.getBoundingClientRect();
    const cellRect = cell.getBoundingClientRect();

    return {
      left: cellRect.left - gridRect.left,
      top: cellRect.top - gridRect.top,
      width: cellRect.width,
      height: cellRect.height,
    };
  }, []);

  const updateData = useCallback(async () => {
    updateStatus(false, "Actualizando...");

    try {
      const response = await fetch("http://localhost:8080/central");
      if (!response.ok) {
        throw new Error("Error en la respuesta del servidor");
      }

      const data = await response.json();
      console.log("Datos actualizados:", data);
      updateStatus(true, "Conectado");

      setGridData(data.grid);
      setTaxisData(data.taxis);
      setCustomersData(data.customers);
      setLocationsData(data.locations || []);
      setCityState(data.cityState || ""); // Actualizar el estado de la ciudad

      // Calcular dimensiones del grid
      if (data.grid && data.grid.length > 0) {
        setGridDimensions({
          rows: data.grid.length,
          cols: data.grid[0].length,
          cellWidth: 0,
          cellHeight: 0,
        });
      }
    } catch (error) {
      console.error("Error al hacer la petición:", error);
      updateStatus(false, "Error de conexión");
    }
  }, [updateStatus]);

  // Efecto para la actualización automática
  useEffect(() => {
    updateData();
    intervalRef.current = setInterval(updateData, 100);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [updateData]);

  // Actualizar dimensiones de celda después del render
  useEffect(() => {
    if (gridData && gridRef.current) {
      setTimeout(() => {
        const firstCell = gridRef.current.querySelector('[data-cell="0-0"]');
        if (firstCell) {
          const rect = firstCell.getBoundingClientRect();
          setGridDimensions((prev) => ({
            ...prev,
            cellWidth: rect.width,
            cellHeight: rect.height,
          }));
        }
      }, 100);
    }
  }, [gridData]);

  const AnimatedElement = ({ id, x, y, content, className, isMoving }) => {
    const position = getCellPosition(x, y);

    return (
      <div
        className={`animated-element ${className} ${isMoving ? "moving" : ""}`}
        style={{
          left: `${position.left}px`,
          top: `${position.top}px`,
          width: `${position.width}px`,
          height: `${position.height}px`,
        }}
      >
        {content}
      </div>
    );
  };

  const renderGrid = () => {
    if (!gridData) return null;

    const elements = [];
    const animatedItems = [];

    // Crear celdas del grid
    gridData.forEach((row, y) => {
      row.forEach((val, x) => {
        const cellId = `${y}-${x}`;
        let cellClass = "cell";

        if (val !== 0) {
          if (typeof val === "string") {
            if (val.match(/[a-z]/)) {
              cellClass += " has-customer";
              animatedItems.push({
                id: val,
                x,
                y,
                content: val,
                className: "customer",
              });
            } else {
              cellClass += " has-destination";
              animatedItems.push({
                id: val,
                x,
                y,
                content: val,
                className: "destination",
              });
            }
          } else if (typeof val === "number") {
            const taxi = taxisData.find((t) => t[0] === val);
            if (taxi) {
              const status = taxi[1];
              const className =
                status === "OK"
                  ? "taxi-ok"
                  : status === "BUSY"
                  ? "taxi-busy"
                  : "taxi-unauthorized";
              animatedItems.push({
                id: val,
                x,
                y,
                content: `T${val}`,
                className,
              });
            }
          }
        }

        elements.push(
          <div key={cellId} className={cellClass} data-cell={cellId} />
        );
      });
    });

    return (
      <>
        {elements}
        {animatedItems.map((item) => (
          <AnimatedElement
            key={item.id}
            id={item.id}
            x={item.x}
            y={item.y}
            content={item.content}
            className={item.className}
          />
        ))}
      </>
    );
  };

  const getStatusClass = () => {
    if (connectionStatus.connected) return "connected";
    if (connectionStatus.message === "Actualizando...") return "connecting";
    return "disconnected";
  };

  return (
    <div className="easycab-container">
      <div id="grid-container">
        <div id="grid" ref={gridRef}>
          {renderGrid()}
        </div>
      </div>

      <div id="info">
        <h1>*** EASY CAB Release 2 ***</h1>

        {/* Mostrar el estado de la ciudad */}
        <div className="city-state-section">
          <div
            className="section-title"
            style={{ color: cityState === "KO" ? "red" : "green" }}
          >
            Estado de la ciudad: {cityState}
          </div>
        </div>

        {/* <div style={{ textAlign: "center", marginBottom: "20px" }}>
          <div className="status-container">
            <span id="status-indicator" className={getStatusClass()}></span>
            <span id="status-text">{connectionStatus.message}</span>
          </div>
          <button id="refresh-btn" onClick={updateData}>
            Actualizar Ahora
          </button>
          <div style={{ fontSize: "0.9em", color: "#666", marginTop: "8px" }}>
            Última actualización: <span id="last-update">{lastUpdate}</span>
          </div>
        </div> */}

        <div className="taxi-section">
          <div className="section-title">Taxis:</div>
          <div id="taxis-info">
            {taxisData.map((taxi) => {
              const [id, estado, x, y, destino] = taxi;
              return (
                <div key={id} className="entry">
                  <strong>Taxi {id}</strong>
                  <br />
                  Estado: {estado} | Posición: ({x}, {y})<br />
                  Destino: {destino || "Ninguno"}
                </div>
              );
            })}
          </div>
        </div>

        <div className="customer-section">
          <div className="section-title">Clientes:</div>
          <div id="customers-info">
            {customersData.length === 0 ? (
              <div className="entry">No hay clientes activos</div>
            ) : (
              customersData.map((customer) => {
                const [id, destino, pos] = customer;
                const estado =
                  destino === "terminado"
                    ? "Servicio terminado"
                    : destino === ""
                    ? "Sin Taxi"
                    : "Esperando";
                return (
                  <div key={id} className="entry">
                    <strong>Cliente {id}</strong>
                    <br />
                    Destino: {destino} | Posición: ({pos[0]}, {pos[1]})<br />
                    Estado: {estado}
                  </div>
                );
              })
            )}
          </div>
        </div>

        {locationsData.length > 0 && (
          <div>
            <div className="section-title">Destinos Disponibles:</div>
            {locationsData.map((location, index) => {
              const [name, coords] = location;
              const [x, y] = coords;
              return (
                <div key={index} className="entry">
                  <strong>{name}</strong>
                  <br />
                  Coordenadas: ({x}, {y})
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
