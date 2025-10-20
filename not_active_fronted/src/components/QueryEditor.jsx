import { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  TextField,
  Paper,
  Divider,
  Tooltip,
  Chip,
  LinearProgress
} from '@mui/material';
import {
  PlayArrow,
  Code,
  Speed
} from '@mui/icons-material';
import './QueryEditor.css';

const QueryEditor = ({ sqlQuery, setSqlQuery, onExecuteQuery, loading }) => {
  const [queryStats, setQueryStats] = useState({
    lines: 1,
    characters: 0
  });

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      if (!loading) {
        onExecuteQuery();
      }
    }
  };

  const handleQueryChange = (e) => {
    const value = e.target.value;
    setSqlQuery(value);
    
    setQueryStats({
      lines: value.split('\n').length,
      characters: value.length
    });
  };

  return (
    <Paper elevation={0} sx={{ display: 'flex', flexDirection: 'column', flex: 1, height: '100%' }}>
      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center', 
        p: 2,
        bgcolor: 'grey.50',
        borderBottom: 1,
        borderColor: 'divider'
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Code color="primary" />
          <Typography variant="h6" component="h3" sx={{ fontWeight: 600 }}>
            Editor SQL
          </Typography>
        </Box>
        
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Chip 
            size="small" 
            label={`${queryStats.lines} líneas`} 
            variant="outlined"
            sx={{ fontSize: '11px' }}
          />
          <Chip 
            size="small" 
            label={`${queryStats.characters} caracteres`} 
            variant="outlined" 
            sx={{ fontSize: '11px' }}
          />
          
          <Tooltip title={loading ? 'Ejecutando consulta...' : 'Ejecutar consulta (Ctrl+Enter)'}>
            <span>
              <Button
                variant="contained"
                startIcon={loading ? <Speed /> : <PlayArrow />}
                onClick={onExecuteQuery}
                disabled={loading}
                sx={{
                  ml: 2,
                  bgcolor: 'primary.main',
                  '&:hover': {
                    bgcolor: 'primary.dark',
                  },
                  '&:disabled': {
                    bgcolor: 'grey.300',
                  }
                }}
              >
                {loading ? 'Ejecutando...' : 'Ejecutar SQL'}
              </Button>
            </span>
          </Tooltip>
        </Box>
      </Box>

      {loading && <LinearProgress />}

      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        <TextField
          multiline
          value={sqlQuery}
          onChange={handleQueryChange}
          onKeyDown={handleKeyDown}
          placeholder="Escribe tu consulta SQL aquí...

Atajos de teclado:
• Ctrl+Enter: Ejecutar consulta

Ejemplos:
SELECT * FROM Customers;
INSERT INTO Customers VALUES (1, 'Juan', 'Pérez', 25, 'México');
DELETE FROM Customers WHERE id = 1;"
          variant="outlined"
          sx={{
            flex: 1,
            '& .MuiOutlinedInput-root': {
              height: '100%',
              alignItems: 'stretch',
              '& fieldset': {
                border: 'none',
              },
              '&:hover fieldset': {
                border: 'none',
              },
              '&.Mui-focused fieldset': {
                border: 'none',
              },
            },
            '& .MuiInputBase-input': {
              height: '100% !important',
              fontFamily: '"Consolas", "Monaco", "Courier New", monospace',
              fontSize: '14px',
              lineHeight: 1.6,
              padding: '20px',
              resize: 'none',
              '&::placeholder': {
                color: 'text.secondary',
                opacity: 0.7,
              }
            },
          }}
        />
      </Box>
    </Paper>
  );
};

export default QueryEditor;