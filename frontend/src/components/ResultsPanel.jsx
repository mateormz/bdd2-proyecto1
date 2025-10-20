import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Alert,
  Chip,
  CircularProgress,
  Divider
} from '@mui/material';
import {
  TableChart,
  Timer,
  DataUsage,
  CheckCircle,
  Error,
  Info
} from '@mui/icons-material';
import './ResultsPanel.css';

const ResultsPanel = ({ queryResult, loading, error, executionInfo }) => {
  const getResultIcon = () => {
    if (error) return <Error color="error" />;
    if (loading) return <CircularProgress size={20} />;
    if (queryResult && queryResult.length > 0) return <TableChart color="primary" />;
    return <Info color="info" />;
  };

  const getResultTitle = () => {
    if (error) return 'Error en la consulta';
    if (loading) return 'Ejecutando consulta...';
    if (queryResult && queryResult.length > 0) return 'Resultados de la consulta';
    return 'Sin resultados';
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
          {getResultIcon()}
          <Typography variant="h6" component="h3" sx={{ fontWeight: 600 }}>
            {getResultTitle()}
          </Typography>
        </Box>

        {executionInfo && !loading && (
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            {executionInfo.executionTime && (
              <Chip
                icon={<Timer />}
                label={executionInfo.executionTime}
                size="small"
                variant="outlined"
                color="primary"
              />
            )}
            {executionInfo.rowsAffected !== undefined && (
              <Chip
                icon={<DataUsage />}
                label={`${executionInfo.rowsAffected} filas`}
                size="small"
                variant="outlined"
                color="success"
              />
            )}
          </Box>
        )}
      </Box>

      <Box sx={{ flex: 1, p: 2, overflow: 'auto' }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {loading && (
          <Box sx={{ 
            display: 'flex', 
            flexDirection: 'column',
            alignItems: 'center', 
            justifyContent: 'center',
            minHeight: 200,
            gap: 2
          }}>
            <CircularProgress size={40} />
            <Typography variant="body1" color="text.secondary">
              Ejecutando consulta...
            </Typography>
          </Box>
        )}

        {executionInfo?.message && !loading && (
          <Alert 
            severity="success" 
            icon={<CheckCircle />}
            sx={{ mb: 2 }}
          >
            {executionInfo.message}
          </Alert>
        )}

        {queryResult && queryResult.length > 0 && !loading && (
          <TableContainer 
            component={Paper} 
            variant="outlined"
            sx={{ 
              maxHeight: 'calc(100vh - 300px)',
              '& .MuiTableCell-head': {
                backgroundColor: 'primary.main',
                color: 'primary.contrastText',
                fontWeight: 600,
                position: 'sticky',
                top: 0,
                zIndex: 1
              }
            }}
          >
            <Table stickyHeader>
              <TableHead>
                <TableRow>
                  {Object.keys(queryResult[0] || {}).map((header, index) => (
                    <TableCell key={index}>
                      {header}
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {queryResult.map((row, index) => (
                  <TableRow 
                    key={index}
                    sx={{ 
                      '&:nth-of-type(odd)': { 
                        backgroundColor: 'grey.50' 
                      },
                      '&:hover': {
                        backgroundColor: 'action.hover'
                      }
                    }}
                  >
                    {Object.values(row).map((value, cellIndex) => (
                      <TableCell key={cellIndex}>
                        {value !== null && value !== undefined ? String(value) : (
                          <Typography 
                            variant="body2" 
                            sx={{ fontStyle: 'italic', color: 'text.disabled' }}
                          >
                            NULL
                          </Typography>
                        )}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}

        {queryResult && queryResult.length === 0 && !loading && !error && (
          <Box sx={{ 
            display: 'flex', 
            flexDirection: 'column',
            alignItems: 'center', 
            justifyContent: 'center',
            minHeight: 200,
            gap: 2
          }}>
            <Info color="info" sx={{ fontSize: 48 }} />
            <Typography variant="h6" color="text.secondary">
              No se encontraron resultados
            </Typography>
            <Typography variant="body2" color="text.secondary">
              La consulta se ejecutó correctamente pero no devolvió datos.
            </Typography>
          </Box>
        )}
      </Box>
    </Paper>
  );
};

export default ResultsPanel;