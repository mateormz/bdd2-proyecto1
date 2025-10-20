const BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api";

export type IoMetrics = {
  reads: number;
  writes: number;
  read_bytes: number;
  write_bytes: number;
  total_time_ms: number; // de tu IOCounter (0 si no usas start/stop)
};

export type SqlResponse = {
  status: "ok" | "error";
  rows?: any[];
  rows_affected?: number;
  message?: string;
  _elapsed_ms?: number;   // wall-clock que añadimos en routes
  metrics?: IoMetrics;    // <-- NUEVO (opcional para compatibilidad)
};

export async function execSQL(query: string): Promise<SqlResponse> {
  const res = await fetch(`${BASE}/sql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json(); // incluirá metrics y _elapsed_ms si el backend los manda
}

export async function listTables(): Promise<{
  tables: { name: string; key: string | null; idx_type: string; columns: string[] }[];
}> {
  const res = await fetch(`${BASE}/tables`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function uploadCSV(file: File): Promise<{ status: string; path: string }> {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${BASE}/upload-csv`, { method: "POST", body: fd });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json();
}

// El backend ahora también devuelve metrics/_elapsed_ms aquí:
export type LoadCsvResponse = {
  status?: "ok" | "error";
  message?: string;
  _elapsed_ms?: number;
  metrics?: IoMetrics;
  // otros campos que ya devuelva tu eng.execute(...)
};

export async function loadCSV(params: {
  table_name: string;
  csv_path: string;
  index_type: string;   // SEQUENTIAL | ISAM | EXTHASH | BPTREE | RTREE
  key_column: string;
  x?: string;
  y?: string;
  z?: string | null;
}): Promise<LoadCsvResponse> {
  const res = await fetch(`${BASE}/load-csv`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json();
}

export async function spatialRange(params: {
  table: string;
  point: number[]; // [x,y] o [x,y,z]
  radius?: number | null;
  coord_column?: string; // "x" simbólico
}): Promise<SqlResponse> {
  const res = await fetch(`${BASE}/spatial/range`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json(); // ahora con metrics/_elapsed_ms
}

export async function spatialKNN(params: {
  table: string;
  point: number[]; // [x,y] o [x,y,z]
  k: number;
  coord_column?: string;
}): Promise<SqlResponse> {
  const res = await fetch(`${BASE}/spatial/knn`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw new Error((await res.json()).detail || res.statusText);
  return res.json(); // ahora con metrics/_elapsed_ms
}