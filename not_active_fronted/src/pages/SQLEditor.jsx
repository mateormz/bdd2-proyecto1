import { useState, useEffect } from 'react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { Box } from '@mui/material';
import { getTables, executeSQL } from '../utils/api';
import TablesSidebar from '../components/TablesSidebar';
import QueryEditor from '../components/QueryEditor';
import ResultsPanel from '../components/ResultsPanel';

const theme = createTheme({
  palette: {
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
  typography: {
    fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
  },
});

const SQLEditor = () => {
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM Customers;');
  const [queryResult, setQueryResult] = useState(null);
  const [tables, setTables] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [executionInfo, setExecutionInfo] = useState(null);

  useEffect(() => {
    const loadTables = async () => {
      try {
        const response = await getTables();
        setTables(response.tables || []);
      } catch (err) {
        console.error('Error cargando tablas:', err);
        setError('Error al cargar las tablas desde el servidor');
      }
    };

    loadTables();
  }, []);

  const executeQuery = async () => {
    setLoading(true);
    setError(null);
    setExecutionInfo(null);
    
    try {
      const result = await executeSQL(sqlQuery);
      
      if (result.status === 'success') {
        setQueryResult(result.result || []);
        setExecutionInfo({
          executionTime: result.execution_time,
          rowsAffected: result.rows_affected,
          message: result.message
        });
      } else {
        setError('Error en la consulta: ' + (result.error || 'Error desconocido'));
      }
    } catch (err) {
      setError('Error al ejecutar la consulta: ' + err.message);
      setQueryResult(null);
    } finally {
      setLoading(false);
    }
  };

  const handleTableInfo = (table) => {
    
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex', height: '100vh', bgcolor: 'background.default' }}>
        <TablesSidebar 
          tables={tables} 
          onTableInfo={handleTableInfo}
        />
        
        <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1 }}>
          <QueryEditor 
            sqlQuery={sqlQuery}
            setSqlQuery={setSqlQuery}
            onExecuteQuery={executeQuery}
            loading={loading}
          />
          
          <ResultsPanel 
            queryResult={queryResult}
            loading={loading}
            error={error}
            executionInfo={executionInfo}
          />
        </Box>
      </Box>
    </ThemeProvider>
  );
};

export default SQLEditor;