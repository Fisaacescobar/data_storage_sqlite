# Módulo 1 — SQLite con Python + Pandas + Matplotlib

Pequeño módulo para probar **almacenamiento y recuperación de datos** usando SQLite.
La idea es simple: crear una BD de juguete, poblarla con datos sintéticos y correr
algunas consultas útiles, integrando `pandas` y graficando con `matplotlib`.

## Requisitos

- Python 3.10+ (probado con 3.11/3.12)
- Paquetes:
  - `numpy`
  - `pandas`
  - `matplotlib`
- `sqlite3` viene con Python estándar (no necesitas instalar nada extra).

> Tip rápido: en la raíz del repo puedes tener un `requirements.txt` con esas libs.

## Cómo correr

Desde esta carpeta:

```bash
# 1) Crear esquema
python main.py --init-db --db data/shop.db

# 2) Sembrar datos sintéticos (cambia los tamaños si quieres)
python main.py --seed --db data/shop.db --n_customers 300 --n_orders 2000

# 3) Ejecutar las consultas demo y generar artefactos
python main.py --demo --db data/shop.db
```

Se generan/actualizan:

- `data/shop.db` — base SQLite.
- `data/query_plans.txt` — texto con `EXPLAIN QUERY PLAN` para cada consulta.
- `data/monthly_revenue.png` — gráfico de ingresos mensuales (status=PAID).

## Consultas incluidas

- **top_customers**: top 10 clientes por revenue (sólo órdenes `PAID`).
- **monthly_revenue**: revenue agregado por mes (`strftime('%Y-%m', order_date)`).
- **category_city_matrix**: matriz ciudad × categoría con revenue.
- **window_rank_in_city**: ranking por ciudad con `ROW_NUMBER()` (top 3 por ciudad).

> Nota: se crean índices sencillos (`customer_id`, `order_date`, `status`) antes de correr las consultas para que el plan de ejecución sea razonable.

## Semillas y tamaños

Los datos se generan con un RNG reproducible:

- Cambia la semilla dentro de `main.py` (variable `rng = np.random.default_rng(2025)`)
- O ajusta los tamaños:
  ```bash
  python main.py --seed --db data/shop.db --n_customers 500 --n_orders 5000
  ```

## Reset rápido

Si quieres partir de cero:
```bash
rm -f data/shop.db data/query_plans.txt data/monthly_revenue.png
python main.py --init-db --seed --db data/shop.db --n_customers 300 --n_orders 2000
python main.py --demo --db data/shop.db
```

## Ver la base “a mano”

- **CLI SQLite** (si lo tienes):
  ```bash
  sqlite3 data/shop.db
  .tables
  .schema orders
  SELECT COUNT(*) FROM customers;
  SELECT * FROM orders LIMIT 5;
  ```
- **GUI**: DB Browser for SQLite o extensiones de VS Code funcionan bien.

## Notas / TODO

- TODO: mover `CATEGORIES` y `CITIES` a tablas si el esquema crece.
- Si `orders` escala, considerar índices compuestos (p. ej. `(status, order_date)`).
- Los montos se generan con lognormal para tener cola pesada (ventas altas ocasionales).
- El gráfico es intencionalmente simple; si necesitas estilos, agrégalos tú para no sobrecargar.

---
Cualquier ajuste que quieras (nombres de columnas, categorías, ciudades, etc.), hazlo en `main.py` y vuelve a correr `--seed` + `--demo`.
