import React from "react";

type Props = { rows?: any[] };

const ResultTable: React.FC<Props> = ({ rows }) => {
  if (!rows || rows.length === 0) {
    return <div className="small">Sin filas.</div>;
  }
  const cols = Object.keys(rows[0]);

  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td key={c + i}>{String(r[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default ResultTable;