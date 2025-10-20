import React, { useEffect, useMemo, useState } from "react";
import "./index.css";
import ResultTable from "./components/ResultTable";
import {
  execSQL,
  listTables,
  uploadCSV,
  loadCSV,
  spatialRange,
  spatialKNN,
  SqlResponse,
} from "./api";
import { TableMeta, fmtMs } from "./types";

export default function App() {
  const [tables, setTables] = useState<TableMeta[]>([]);
  const [sql, setSql] = useState<string>("SELECT * FROM MiTabla;");
  const [result, setResult] = useState<SqlResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");

  // CSV/load form
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvUploadedPath, setCsvUploadedPath] = useState("");
  const [tableName, setTableName] = useState("Inventory3D");
  const [indexType, setIndexType] = useState<"SEQUENTIAL" | "ISAM" | "EXTHASH" | "BPTREE" | "RTREE">("RTREE");
  const [keyColumn, setKeyColumn] = useState("id");
  const [colX, setColX] = useState("x");
  const [colY, setColY] = useState("y");
  const [colZ, setColZ] = useState("z");

  // Spatial quick actions
  const [spTable, setSpTable] = useState("Inventory3D");
  const [pointStr, setPointStr] = useState("[0,0,0]");
  const [radius, setRadius] = useState<string>("5");
  const [k, setK] = useState<string>("5");

  const has3D = useMemo(() => /\[\s*[^,\]]+\s*,\s*[^,\]]+\s*,\s*[^,\]]+\s*\]/.test(pointStr), [pointStr]);

  async function refreshTables() {
    try {
      const res = await listTables();
      setTables(res.tables);
      if (res.tables.length && !spTable) setSpTable(res.tables[0].name);
    } catch (e: any) {
      console.error(e);
    }
  }

  useEffect(() => {
    refreshTables();
  }, []);

  async function runSQL() {
    setLoading(true);
    setErrorMsg("");
    try {
      const r = await execSQL(sql);
      setResult(r);
      refreshTables();
    } catch (e: any) {
      setErrorMsg(e.message || String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  async function doUpload() {
    if (!csvFile) return;
    setLoading(true);
    setErrorMsg("");
    try {
      const up = await uploadCSV(csvFile);
      setCsvUploadedPath(up.path);
    } catch (e: any) {
      setErrorMsg(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  async function doLoadCSV() {
    if (!csvUploadedPath) {
      setErrorMsg("Primero sube el CSV.");
      return;
    }
    setLoading(true);
    setErrorMsg("");
    try {
      const payload = await loadCSV({
        table_name: tableName,
        csv_path: csvUploadedPath,
        index_type: indexType,
        key_column: keyColumn,
        x: colX || "x",
        y: colY || "y",
        z: colZ || "z",
      });
      setResult(payload);
      await refreshTables();
    } catch (e: any) {
      setErrorMsg(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }

  async function doSpatialRange() {
    setLoading(true);
    setErrorMsg("");
    try {
      const p = JSON.parse(pointStr);
      const r = radius ? Number(radius) : null;
      const res = await spatialRange({
        table: spTable,
        point: p,
        radius: r ?? undefined,
        coord_column: "x",
      });
      setResult(res);
    } catch (e: any) {
      setErrorMsg(e.message || "Error al parsear punto/radio");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  async function doSpatialKNN() {
    setLoading(true);
    setErrorMsg("");
    try {
      const p = JSON.parse(pointStr);
      const res = await spatialKNN({
        table: spTable,
        point: p,
        k: Number(k) || 5,
        coord_column: "x",
      });
      setResult(res);
    } catch (e: any) {
      setErrorMsg(e.message || "Error al parsear punto/k");
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="app">
      {/* SIDEBAR */}
      <aside className="sidebar">
        <div className="logo">Mini SGBD 3D</div>
        <div className="small">API: {import.meta.env.VITE_API_BASE_URL || "http://localhost:8000/api"}</div>

        <div className="section">
          <div className="badge">Tablas</div>
          <div className="table-list">
            {tables.map((t) => (
              <div className="table-item" key={t.name}>
                <div style={{ fontWeight: 600 }}>{t.name}</div>
                <div className="small">key: {t.key ?? "-"}</div>
                <div className="small">idx: {t.idx_type}</div>
                <div className="small">{t.columns.join(", ")}</div>
              </div>
            ))}
            {!tables.length && <div className="small">Sin tablas registradas.</div>}
          </div>
        </div>
      </aside>

      {/* MAIN */}
      <main className="main">
        {/* SQL + acciones */}
        <div className="panel grid2">
          <div>
            <div className="badge">Editor SQL</div>
            <textarea
              className="sql"
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              spellCheck={false}
            />
            <div className="toolbar">
              <button className="btn primary" onClick={runSQL} disabled={loading}>
                {loading ? "Ejecutando..." : "Run SQL"}
              </button>
              <button className="btn" onClick={() => setSql("SELECT * FROM " + (tables[0]?.name || "MiTabla"))}>
                SELECT * quick
              </button>
              <button
                className="btn"
                onClick={() =>
                  setSql(
                    `CREATE TABLE ${tableName} (\n  id INT KEY INDEX SEQUENTIAL,\n  x FLOAT,\n  y FLOAT,\n  z FLOAT,\n  nombre VARCHAR[32]\n)`
                  )
                }
              >
                Plantilla CREATE
              </button>
            </div>
            {!!errorMsg && <div className="alert">{errorMsg}</div>}
          </div>

          {/* CSV Loader */}
          <div>
            <div className="badge">Cargar CSV</div>
            <div className="form-row">
              <input className="input" type="file" accept=".csv" onChange={(e) => setCsvFile(e.target.files?.[0] || null)} />
              <button className="btn" onClick={doUpload} disabled={!csvFile || loading}>
                Subir CSV
              </button>
              {csvUploadedPath && <div className="small">Guardado: {csvUploadedPath}</div>}
            </div>

            <div className="form-row form-2">
              <div>
                <label className="small">Tabla</label>
                <input className="input" value={tableName} onChange={(e) => setTableName(e.target.value)} />
              </div>
              <div>
                <label className="small">Índice</label>
                <select className="input" value={indexType} onChange={(e) => setIndexType(e.target.value as any)}>
                  <option>SEQUENTIAL</option>
                  <option>ISAM</option>
                  <option>EXTHASH</option>
                  <option>BPTREE</option>
                  <option>RTREE</option>
                </select>
              </div>
            </div>

            <div className="form-row form-3">
              <div>
                <label className="small">Key column</label>
                <input className="input" value={keyColumn} onChange={(e) => setKeyColumn(e.target.value)} />
              </div>
              <div>
                <label className="small">X</label>
                <input className="input" value={colX} onChange={(e) => setColX(e.target.value)} />
              </div>
              <div>
                <label className="small">Y</label>
                <input className="input" value={colY} onChange={(e) => setColY(e.target.value)} />
              </div>
            </div>
            <div className="form-row">
              <div>
                <label className="small">Z</label>
                <input className="input" value={colZ} onChange={(e) => setColZ(e.target.value)} />
              </div>
            </div>

            <div className="toolbar">
              <button className="btn primary" onClick={doLoadCSV} disabled={!csvUploadedPath || loading}>
                Cargar en tabla
              </button>
            </div>

            <hr style={{ borderColor: "var(--border)", margin: "12px 0" }} />

            <div className="badge">Consultas espaciales (RTree)</div>
            <div className="form-row form-2">
              <div>
                <label className="small">Tabla</label>
                <input className="input" value={spTable} onChange={(e) => setSpTable(e.target.value)} />
              </div>
              <div>
                <label className="small">Punto {has3D ? "(3D)" : "(2D)"} </label>
                <input
                  className="input"
                  value={pointStr}
                  onChange={(e) => setPointStr(e.target.value)}
                  placeholder='[x,y] o [x,y,z]'
                />
              </div>
            </div>
            <div className="form-row form-2">
              <div>
                <label className="small">Radio (rango)</label>
                <input className="input" value={radius} onChange={(e) => setRadius(e.target.value)} placeholder="5" />
              </div>
              <div>
                <label className="small">k (k-NN)</label>
                <input className="input" value={k} onChange={(e) => setK(e.target.value)} placeholder="5" />
              </div>
            </div>
            <div className="toolbar">
              <button className="btn" onClick={doSpatialRange} disabled={loading}>
                Buscar por rango
              </button>
              <button className="btn" onClick={doSpatialKNN} disabled={loading}>
                Buscar k-NN
              </button>
            </div>
          </div>
        </div>

        {/* RESULTADOS */}
        <div className="section">
          <div className="badge">Resultados</div>
          <div className="kpis">
            <div className="kpi small">status: {result?.status ?? "-"}</div>
            <div className="kpi small">filas: {result?.rows ? result.rows.length : 0}</div>
            <div className="kpi small">tiempo: {fmtMs(result?._elapsed_ms)}</div>
          </div>
          <ResultTable rows={result?.rows} />
          {result?.message && (
            <div className="small" style={{ marginTop: 8, color: "var(--muted)" }}>
              {result.message}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}