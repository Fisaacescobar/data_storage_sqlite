#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Script pequeño para armar una BD SQLite de juguete y correr algunas consultas.
# Mantener simple > sobre-ingeniería. Si algo crece, separar en módulos.

import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------
# utilidades
# ---------------------------------------------------------------------

def mkparent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------------------------------------------------------------------
# esquema
# ---------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    first_name  TEXT NOT NULL,
    last_name   TEXT NOT NULL,
    email       TEXT UNIQUE NOT NULL,
    city        TEXT,
    signup_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date  TEXT NOT NULL,
    category    TEXT NOT NULL,
    amount      REAL NOT NULL,
    status      TEXT NOT NULL CHECK(status IN ('PAID','CANCELLED','REFUNDED')),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
"""

CITIES = ["Santiago", "Valparaíso", "Viña del Mar", "Concepción", "La Serena", "Antofagasta"]
CATEGORIES = ["Electronics", "Groceries", "Books", "Home", "Sports", "Beauty"]
STATUSES = ["PAID", "CANCELLED", "REFUNDED"]

# Notar: rng global sólo para reproducibilidad. Cambiar seed si quieres otros datos.
rng = np.random.default_rng(2025)


def create_schema(conn: sqlite3.Connection) -> None:
    with conn:
        conn.executescript(SCHEMA)


# ---------------------------------------------------------------------
# datos sintéticos
# ---------------------------------------------------------------------

def make_customers(n: int = 300) -> list[tuple]:
    first = rng.choice(["Ana","Luis","Camila","Diego","Matías","Carla","Javiera","Pedro"], size=n)
    last  = rng.choice(["Pérez","González","Soto","Muñoz","Rojas","Silva","López"], size=n)
    city  = rng.choice(CITIES, size=n, p=[0.45,0.12,0.12,0.15,0.08,0.08])
    # Fechas de alta distribuidas en ~600 días
    base = datetime(2023, 1, 1)
    signup = [ (base + timedelta(days=int(x))).strftime("%Y-%m-%d") for x in rng.integers(0, 600, size=n) ]
    rows = []
    for i in range(n):
        rows.append((first[i], last[i], f"user{i}@example.com", city[i], signup[i]))
    return rows


def make_orders(n: int, n_customers: int) -> list[tuple]:
    start = datetime(2023, 6, 1)
    days = rng.integers(0, 480, size=n)
    dates = [ (start + timedelta(days=int(d))).strftime("%Y-%m-%d") for d in days ]

    # Montos con cola pesada (lognormal) y mínimo positivo
    amounts = np.round(rng.lognormal(mean=3.2, sigma=0.65, size=n), 2)

    rows = []
    for i in range(n):
        rows.append((
            int(rng.integers(1, n_customers + 1)),
            dates[i],
            str(rng.choice(CATEGORIES, p=[0.25,0.20,0.13,0.18,0.14,0.10])),
            float(amounts[i]),
            str(rng.choice(STATUSES, p=[0.85,0.10,0.05]))
        ))
    return rows


# ---------------------------------------------------------------------
# carga a SQLite (sin pandas, a propósito)
# ---------------------------------------------------------------------

def load_customers(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    with conn:
        conn.executemany(
            "INSERT INTO customers(first_name,last_name,email,city,signup_date) VALUES (?,?,?,?,?)",
            rows,
        )


def load_orders(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    with conn:
        conn.executemany(
            "INSERT INTO orders(customer_id,order_date,category,amount,status) VALUES (?,?,?,?,?)",
            rows,
        )


# ---------------------------------------------------------------------
# consultas y utilidades
# ---------------------------------------------------------------------

SQL = {
    "top_customers": """
        SELECT c.customer_id,
               c.first_name || ' ' || c.last_name AS customer,
               c.city,
               SUM(CASE WHEN o.status='PAID' THEN o.amount ELSE 0 END) AS revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, customer, c.city
        HAVING revenue > 0
        ORDER BY revenue DESC
        LIMIT 10;
    """,
    "monthly_revenue": """
        SELECT strftime('%Y-%m', o.order_date) AS ym,
               SUM(CASE WHEN status='PAID' THEN amount ELSE 0 END) AS revenue
        FROM orders o
        GROUP BY ym
        ORDER BY ym;
    """,
    "category_city_matrix": """
        SELECT c.city, o.category,
               SUM(CASE WHEN o.status='PAID' THEN o.amount ELSE 0 END) AS revenue
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        GROUP BY c.city, o.category
        ORDER BY c.city, o.category;
    """,
    # Nota: ventana simple para un ranking interno por ciudad.
    "window_rank_in_city": """
        SELECT city, customer, revenue, rn FROM (
          SELECT c.city AS city,
                 c.first_name || ' ' || c.last_name AS customer,
                 SUM(CASE WHEN o.status='PAID' THEN o.amount ELSE 0 END) AS revenue,
                 ROW_NUMBER() OVER (PARTITION BY c.city ORDER BY SUM(CASE WHEN o.status='PAID' THEN o.amount ELSE 0 END) DESC) AS rn
          FROM customers c
          JOIN orders o ON o.customer_id = c.customer_id
          GROUP BY c.city, c.customer_id
        ) t
        WHERE rn <= 3
        ORDER BY city, rn;
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);",
    "CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);",
]


def explain_plan(conn: sqlite3.Connection, sql: str) -> str:
    cur = conn.execute("EXPLAIN QUERY PLAN " + sql)
    # EXPLAIN devuelve 4 columnas; nos interesa la última (detalle)
    lines = [row[-1] for row in cur.fetchall()]
    return "\n".join(lines)


def plot_monthly_revenue(df_monthly: pd.DataFrame, outpath: Path) -> None:
    plt.figure()
    plt.plot(df_monthly["ym"], df_monthly["revenue"], marker="o")
    plt.title("Ingresos mensuales (status=PAID)")
    plt.xlabel("Año-Mes")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(outpath, dpi=160)
    plt.close()


def demo_tx_and_params(conn: sqlite3.Connection) -> None:
    print("\n[Transacciones] Ejemplo rápido")
    try:
        with conn:
            conn.execute(
                "INSERT INTO customers(first_name,last_name,email,city,signup_date) VALUES (?,?,?,?,?)",
                ("Temporal","Test","temporal@example.com","Santiago", datetime.now().strftime("%Y-%m-%d"))
            )
            # Forzar violación UNIQUE -> rollback de todo el bloque
            conn.execute(
                "INSERT INTO customers(first_name,last_name,email,city,signup_date) VALUES (?,?,?,?,?)",
                ("Temporal","Test","temporal@example.com","Santiago", datetime.now().strftime("%Y-%m-%d"))
            )
    except sqlite3.IntegrityError as e:
        print("  -> IntegrityError capturado. OK, se revirtió la transacción.")
        print("     ", e)

    # Parametrizada típica
    city = "Santiago"
    cur = conn.execute("SELECT COUNT(*) FROM customers WHERE city = ?;", (city,))
    print(f"  -> Clientes en {city}: {cur.fetchone()[0]}")


# ---------------------------------------------------------------------
# flujo principal
# ---------------------------------------------------------------------

def init_db(db_path: str) -> None:
    p = Path(db_path)
    mkparent(p)
    with connect(db_path) as conn:
        create_schema(conn)
    print(f"[OK] Esquema en {db_path}")


def seed_db(db_path: str, n_customers: int, n_orders: int) -> None:
    with connect(db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM customers;")
        if cur.fetchone()[0] > 0:
            print("[INFO] BD ya tenía datos; no se duplica.")
            return
        load_customers(conn, make_customers(n_customers))
        load_orders(conn, make_orders(n_orders, n_customers))
    print(f"[OK] Cargados {n_customers} clientes y {n_orders} órdenes")


def run_demo(db_path: str) -> None:
    outdir = Path(db_path).parent
    outdir.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        with conn:
            for idx in INDEXES:
                conn.execute(idx)

        plans = []
        results = {}
        for name, sql in SQL.items():
            print(f"\n[Query] {name}")
            df = pd.read_sql_query(sql, conn)
            print(df.head(10))  # vistazo rápido
            results[name] = df
            plans.append(f"-- {name} --\n" + explain_plan(conn, sql) + "\n")

        # Guardar planes
        qp = outdir / "query_plans.txt"
        qp.write_text("\n".join(plans), encoding="utf-8")
        print(f"[OK] Planes -> {qp}")

        # Gráfico mensual
        plot_monthly_revenue(results["monthly_revenue"], outdir / "monthly_revenue.png")
        print(f"[OK] Gráfico -> {outdir / 'monthly_revenue.png'}")

        # Demo de transacciones/parametrizadas
        demo_tx_and_params(conn)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SQLite + pandas + matplotlib (demo)")
    p.add_argument("--db", default="data/shop.db", help="Ruta a la base de datos")
    p.add_argument("--init-db", action="store_true", help="Crear esquema si no existe")
    p.add_argument("--seed", action="store_true", help="Sembrar datos sintéticos")
    p.add_argument("--n_customers", type=int, default=300, help="N clientes")
    p.add_argument("--n_orders", type=int, default=2000, help="N órdenes")
    p.add_argument("--demo", action="store_true", help="Ejecutar consultas demo")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.init_db:
        init_db(args.db)
    if args.seed:
        seed_db(args.db, args.n_customers, args.n_orders)
    if args.demo:
        run_demo(args.db)
    if not (args.init_db or args.seed or args.demo):
        print("Nada que hacer. Usa --init-db / --seed / --demo")


if __name__ == "__main__":
    main()
