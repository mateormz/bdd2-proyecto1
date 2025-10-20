export type TableMeta = {
  name: string;
  key: string | null;
  idx_type: string;
  columns: string[];
};

export function fmtMs(ms?: number) {
  return typeof ms === "number" ? `${ms.toFixed(2)} ms` : "-";
}