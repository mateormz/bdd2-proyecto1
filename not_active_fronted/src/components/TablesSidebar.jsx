import { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  IconButton,
  Collapse,
  Divider,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Chip,
  Grid,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip
} from '@mui/material';
import {
  Storage,
  Info,
  ExpandLess,
  ExpandMore,
  TableChart,
  Key,
  Speed,
  DataObject,
  Close,
  Update,
  Folder,
  FolderOpen
} from '@mui/icons-material';
import './TablesSidebar.css';

const TablesSidebar = ({ tables, onTableInfo }) => {
  const [selectedTable, setSelectedTable] = useState(null);
  const [showTableInfo, setShowTableInfo] = useState(false);
  const [expandedTables, setExpandedTables] = useState({});

  const showTableDetails = (table) => {
    setSelectedTable(table);
    setShowTableInfo(true);
    if (onTableInfo) {
      onTableInfo(table);
    }
  };

  const closeTableInfo = () => {
    setShowTableInfo(false);
    setSelectedTable(null);
  };

  const toggleTableExpansion = (tableName) => {
    setExpandedTables(prev => ({
      ...prev,
      [tableName]: !prev[tableName]
    }));
  };

  const getTableStats = (table) => {
    const stats = {
      "Customers": { 
        rowCount: 150000, 
        size: "12.5 MB", 
        lastUpdated: "2024-10-15",
        primaryKey: "ID del cliente",
        indexes: [
          { column: "ID del cliente", type: "B-Tree", unique: true },
          { column: "edad", type: "B-Tree", unique: false },
          { column: "pais", type: "Hash", unique: false }
        ]
      },
      "Pedidos": { 
        rowCount: 89500, 
        size: "8.2 MB", 
        lastUpdated: "2024-10-16",
        primaryKey: "ID del pedido",
        indexes: [
          { column: "ID del pedido", type: "B-Tree", unique: true },
          { column: "ID del cliente", type: "B-Tree", unique: false }
        ]
      },
      "Envios": { 
        rowCount: 45200, 
        size: "3.1 MB", 
        lastUpdated: "2024-10-16",
        primaryKey: "ID de envio",
        indexes: [
          { column: "ID de envio", type: "B-Tree", unique: true },
          { column: "cliente", type: "B-Tree", unique: false }
        ]
      }
    };
    return stats[table.name] || { 
      rowCount: 0, 
      size: "0 MB", 
      lastUpdated: "N/A",
      primaryKey: "N/A",
      indexes: []
    };
  };

  return (
    <Paper 
      elevation={0} 
      sx={{ 
        width: 300, 
        height: '100%', 
        display: 'flex', 
        flexDirection: 'column',
        borderRight: 1,
        borderColor: 'divider'
      }}
    >
      <Box sx={{ 
        p: 2, 
        bgcolor: 'primary.main', 
        color: 'primary.contrastText',
        display: 'flex',
        alignItems: 'center',
        gap: 1
      }}>
        <Storage />
        <Typography variant="h6" component="h3" sx={{ fontWeight: 600 }}>
          Tablas
        </Typography>
      </Box>

      <Box sx={{ flex: 1, overflow: 'auto' }}>
        <List disablePadding>
          {tables.map((table, index) => (
            <Box key={index}>
              <ListItem 
                sx={{ 
                  flexDirection: 'column', 
                  alignItems: 'stretch',
                  bgcolor: index % 2 === 0 ? 'grey.50' : 'white',
                  '&:hover': {
                    bgcolor: 'action.hover'
                  }
                }}
              >
                <Box sx={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  width: '100%',
                  justifyContent: 'space-between'
                }}>
                  <Box 
                    sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1, cursor: 'pointer' }}
                    onClick={() => toggleTableExpansion(table.name)}
                  >
                    <ListItemIcon sx={{ minWidth: 36 }}>
                      {expandedTables[table.name] ? <FolderOpen color="primary" /> : <Folder color="primary" />}
                    </ListItemIcon>
                    <ListItemText 
                      primary={table.name}
                      primaryTypographyProps={{
                        fontWeight: 600,
                        color: 'primary.main'
                      }}
                    />
                    {expandedTables[table.name] ? <ExpandLess /> : <ExpandMore />}
                  </Box>
                  
                  <Tooltip title="Ver información detallada">
                    <IconButton
                      size="small"
                      onClick={() => showTableDetails(table)}
                      sx={{ color: 'info.main' }}
                    >
                      <Info />
                    </IconButton>
                  </Tooltip>
                </Box>

                <Collapse in={expandedTables[table.name]} timeout="auto" unmountOnExit>
                  <Box sx={{ pl: 6, pb: 1 }}>
                    {table.columns.map((column, colIndex) => (
                      <Box 
                        key={colIndex}
                        sx={{ 
                          display: 'flex', 
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          py: 0.5,
                          borderBottom: colIndex < table.columns.length - 1 ? 1 : 0,
                          borderColor: 'divider'
                        }}
                      >
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {column.name}
                        </Typography>
                        <Chip 
                          label={column.type} 
                          size="small" 
                          variant="outlined"
                          sx={{ fontSize: '10px', height: 20 }}
                        />
                      </Box>
                    ))}
                  </Box>
                </Collapse>
              </ListItem>
              {index < tables.length - 1 && <Divider />}
            </Box>
          ))}
        </List>
      </Box>

      <Dialog 
        open={showTableInfo} 
        onClose={closeTableInfo}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          gap: 1,
          bgcolor: 'primary.main',
          color: 'primary.contrastText'
        }}>
          <TableChart />
          Información de la tabla: {selectedTable?.name}
          <IconButton
            onClick={closeTableInfo}
            sx={{ 
              position: 'absolute',
              right: 8,
              top: 8,
              color: 'primary.contrastText'
            }}
          >
            <Close />
          </IconButton>
        </DialogTitle>
        
        <DialogContent sx={{ p: 3 }}>
          {selectedTable && (() => {
            const stats = getTableStats(selectedTable);
            return (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
                <Grid container spacing={2}>
                  <Grid item xs={12} sm={6} md={3}>
                    <Card variant="outlined">
                      <CardContent sx={{ textAlign: 'center' }}>
                        <DataObject color="primary" sx={{ fontSize: 32, mb: 1 }} />
                        <Typography variant="h6" component="div">
                          {stats.rowCount.toLocaleString()}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Filas
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  
                  <Grid item xs={12} sm={6} md={3}>
                    <Card variant="outlined">
                      <CardContent sx={{ textAlign: 'center' }}>
                        <Storage color="primary" sx={{ fontSize: 32, mb: 1 }} />
                        <Typography variant="h6" component="div">
                          {stats.size}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Tamaño
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  
                  <Grid item xs={12} sm={6} md={3}>
                    <Card variant="outlined">
                      <CardContent sx={{ textAlign: 'center' }}>
                        <Update color="primary" sx={{ fontSize: 32, mb: 1 }} />
                        <Typography variant="h6" component="div">
                          {stats.lastUpdated}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Última actualización
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  
                  <Grid item xs={12} sm={6} md={3}>
                    <Card variant="outlined">
                      <CardContent sx={{ textAlign: 'center' }}>
                        <Key color="primary" sx={{ fontSize: 32, mb: 1 }} />
                        <Typography variant="h6" component="div">
                          {stats.primaryKey}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Clave primaria
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                </Grid>

                <Divider />
                <Box>
                  <Typography variant="h6" sx={{ mb: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
                    <TableChart />
                    Columnas
                  </Typography>
                  <Grid container spacing={1}>
                    {selectedTable.columns.map((column, index) => (
                      <Grid item xs={12} sm={6} md={4} key={index}>
                        <Paper 
                          variant="outlined" 
                          sx={{ 
                            p: 2, 
                            borderLeft: 3, 
                            borderLeftColor: 'primary.main',
                            '&:hover': {
                              bgcolor: 'action.hover'
                            }
                          }}
                        >
                          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                            {column.name}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            {column.type}
                          </Typography>
                        </Paper>
                      </Grid>
                    ))}
                  </Grid>
                </Box>

                <Divider />

                <Box>
                  <Typography variant="h6" sx={{ mb: 2, display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Speed />
                    Índices
                  </Typography>
                  <TableContainer component={Paper} variant="outlined">
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell><strong>Columna</strong></TableCell>
                          <TableCell><strong>Tipo</strong></TableCell>
                          <TableCell><strong>Único</strong></TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {stats.indexes.map((index, idx) => (
                          <TableRow key={idx}>
                            <TableCell>{index.column}</TableCell>
                            <TableCell>
                              <Chip 
                                label={index.type} 
                                size="small" 
                                color="primary"
                                variant="outlined"
                              />
                            </TableCell>
                            <TableCell>
                              <Chip 
                                label={index.unique ? 'Sí' : 'No'} 
                                size="small" 
                                color={index.unique ? 'success' : 'default'}
                                variant="outlined"
                              />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Box>
              </Box>
            );
          })()}
        </DialogContent>
        
        <DialogActions>
          <Button onClick={closeTableInfo} variant="contained">
            Cerrar
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export default TablesSidebar;