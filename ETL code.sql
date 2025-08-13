-- 1) Pre-sale inventory check
-- -------------------------------
CREATE OR REPLACE FUNCTION public.check_inventory_before() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  v_store_id INTEGER;
  v_stock    INTEGER;
BEGIN
  -- Find which store this sale belongs to
  SELECT store_id INTO v_store_id
  FROM sales
  WHERE sale_id = NEW.sale_id;

   
  SELECT quantity INTO v_stock
  FROM inventory
  WHERE store_id = v_store_id
    AND product_id = NEW.product_id
  FOR UPDATE;

  -- Reject if no inventory row or insufficient stock
  IF v_stock IS NULL THEN
    RAISE EXCEPTION 'No inventory row for store_id=% product_id=%', v_store_id, NEW.product_id;
  END IF;

  IF v_stock < NEW.quantity THEN
    RAISE EXCEPTION 'Insufficient stock: have %, need % (store %, product %)',
      v_stock, NEW.quantity, v_store_id, NEW.product_id;
  END IF;

  RETURN NEW;
END;
$$;

-- -------------------------------
-- 2) Post-sale inventory decrement
-- -------------------------------
CREATE OR REPLACE FUNCTION public.decrement_inventory_on_sale() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  v_store_id INTEGER;
  v_stock    INTEGER;
BEGIN
  SELECT store_id INTO v_store_id
  FROM sales
  WHERE sale_id = NEW.sale_id;

  SELECT quantity INTO v_stock
  FROM inventory
  WHERE store_id = v_store_id
    AND product_id = NEW.product_id
  FOR UPDATE;

  IF v_stock IS NULL THEN
    RAISE EXCEPTION 'No inventory row for store_id=% product_id=%', v_store_id, NEW.product_id;
  END IF;

  IF v_stock < NEW.quantity THEN
    RAISE EXCEPTION 'Insufficient stock: have %, need % (store %, product %)',
      v_stock, NEW.quantity, v_store_id, NEW.product_id;
  END IF;

  UPDATE inventory
  SET quantity = quantity - NEW.quantity
  WHERE store_id = v_store_id AND product_id = NEW.product_id;

  RETURN NEW;
END;
$$;

-- -------------------------------
-- 3) Increase inventory after delivery (upsert; accumulate if exists, insert if not)
-- -------------------------------
CREATE OR REPLACE FUNCTION public.increment_inventory_on_delivery() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO inventory (store_id, product_id, quantity)
  VALUES (NEW.store_id, NEW.product_id, NEW.quantity)
  ON CONFLICT (store_id, product_id)
  DO UPDATE SET quantity = inventory.quantity + EXCLUDED.quantity;
  RETURN NEW;
END;
$$;

-- -------------------------------
-- 4) Low-stock alert (on inventory.quantity update)
--    Applies only to stores 1 and 2; example threshold: < 100
-- -------------------------------
CREATE OR REPLACE FUNCTION public.insert_restock_alert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.store_id IN (1, 2) AND NEW.quantity < 100 THEN
    INSERT INTO restock_alerts (product_id, store_id, product_name, quantity, alert_date)
    SELECT 
      p.product_id,
      NEW.store_id,
      p.name,
      NEW.quantity,
      (
        SELECT s.sale_date
        FROM sales s
        JOIN sale_items si ON s.sale_id = si.sale_id
        WHERE s.store_id = NEW.store_id
          AND si.product_id = NEW.product_id
        ORDER BY s.sale_date DESC
        LIMIT 1
      )
    FROM products p
    WHERE p.product_id = NEW.product_id
    ON CONFLICT (product_id, store_id) DO NOTHING;
  END IF;

  RETURN NEW;
END;
$$;

-- -------------------------------
-- 5) Clean up alerts after delivery (delete alerts when stock restored ≥ 25)
-- -------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_restock_alerts_after_delivery() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM restock_alerts ra
  USING inventory i
  WHERE ra.product_id = i.product_id
    AND ra.store_id  = i.store_id
    AND i.quantity   >= 25;
  RETURN NEW;
END;
$$;

-- -------------------------------
-- 6) Monthly store profitability upsert (stores 1 and 2)
-- -------------------------------
CREATE OR REPLACE FUNCTION public.upsert_store_profitability(p_year integer, p_month integer) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
  p_from date := make_date(p_year, p_month, 1);
  p_to   date := (make_date(p_year, p_month, 1) + INTERVAL '1 month')::date;
BEGIN
  WITH
  sold AS (  -- Sales in month -> Revenue
    SELECT s.store_id,
           SUM(si.quantity * p.unit_price)::numeric(12,2) AS revenue
    FROM sales s
    JOIN sale_items si USING (sale_id)
    JOIN products p ON p.product_id = si.product_id
    WHERE s.sale_date >= p_from AND s.sale_date < p_to
      AND s.store_id IN (1,2)
    GROUP BY s.store_id
  ),
  cogs AS (  -- Deliveries in month -> Cost of goods
    SELECT d.store_id,
           SUM(d.quantity * v.purchase_price)::numeric(12,2) AS cost
    FROM deliveries d
    JOIN vendors v
      ON v.vendor_id = d.vendor_id AND v.product_id = d.product_id
    WHERE d.delivery_date >= p_from AND d.delivery_date < p_to
      AND d.store_id IN (1,2)
    GROUP BY d.store_id
  ),
  opex AS (  -- Operating expenses in month
    SELECT e.store_id,
           SUM(e.amount)::numeric(12,2) AS cost
    FROM expenses e
    WHERE e.expense_date >= p_from AND e.expense_date < p_to
      AND e.store_id IN (1,2)
    GROUP BY e.store_id
  ),
  stores AS (SELECT unnest(ARRAY[1,2]) AS store_id),
  final AS (
    SELECT s.store_id,
           p_from AS profit_month,
           COALESCE(r.revenue, 0)                      AS total_revenue,
           (COALESCE(c.cost, 0) + COALESCE(x.cost, 0)) AS total_expense
    FROM stores s
    LEFT JOIN sold r ON r.store_id = s.store_id
    LEFT JOIN cogs c ON c.store_id = s.store_id
    LEFT JOIN opex x ON x.store_id = s.store_id
  )
  INSERT INTO store_profitability
    (store_profitability_id, store_id, profit_month,
     total_revenue, total_expense, net_profit)
  SELECT
    ((p_year * 100 + p_month) * 10 + f.store_id)       AS store_profitability_id,
    f.store_id,
    f.profit_month,
    f.total_revenue,
    f.total_expense,
    (f.total_revenue - f.total_expense)                AS net_profit
  FROM final f
  ON CONFLICT (store_profitability_id) DO UPDATE
  SET total_revenue = EXCLUDED.total_revenue,
      total_expense = EXCLUDED.total_expense,
      net_profit    = EXCLUDED.net_profit,
      profit_month  = EXCLUDED.profit_month,
      store_id      = EXCLUDED.store_id;
END;
$$;

-- =========================================================
-- Triggers: drop then create (safe to re-run)
-- =========================================================

-- deliveries: receipt -> increment inventory, cleanup alerts
DROP TRIGGER IF EXISTS trg_01_increment_inventory_on_delivery ON public.deliveries;
CREATE TRIGGER trg_01_increment_inventory_on_delivery
AFTER INSERT ON public.deliveries
FOR EACH ROW
EXECUTE FUNCTION public.increment_inventory_on_delivery();

DROP TRIGGER IF EXISTS trg_02_cleanup_restock_alerts_after_delivery ON public.deliveries;
CREATE TRIGGER trg_02_cleanup_restock_alerts_after_delivery
AFTER INSERT ON public.deliveries
FOR EACH ROW
EXECUTE FUNCTION public.cleanup_restock_alerts_after_delivery();

-- sale_items: pre-insert stock check; post-insert decrement
DROP TRIGGER IF EXISTS trg_check_inventory_before ON public.sale_items;
CREATE TRIGGER trg_check_inventory_before
BEFORE INSERT ON public.sale_items
FOR EACH ROW
EXECUTE FUNCTION public.check_inventory_before();

DROP TRIGGER IF EXISTS trg_decrement_inventory_on_sale ON public.sale_items;
CREATE TRIGGER trg_decrement_inventory_on_sale
AFTER INSERT ON public.sale_items
FOR EACH ROW
EXECUTE FUNCTION public.decrement_inventory_on_sale();

-- inventory: on quantity update, check whether a restock alert is needed
DROP TRIGGER IF EXISTS trg_check_restock_alert ON public.inventory;
CREATE TRIGGER trg_check_restock_alert
AFTER UPDATE OF quantity ON public.inventory
FOR EACH ROW
EXECUTE FUNCTION public.insert_restock_alert();
