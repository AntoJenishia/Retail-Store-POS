from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from db_config import get_db_connection

app = Flask(__name__)
app.secret_key = 'dev_replace_with_real_secret'

@app.route('/')
def home():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM products),
            (SELECT COUNT(*) FROM customers),
            (SELECT COUNT(*) FROM sales),
            (SELECT COALESCE(SUM(total_amount),0) FROM sales)
    """)
    data = cursor.fetchone()
    cursor.close()
    conn.close()
    return render_template('dashboard.html', data=data)

@app.route('/products')
def products():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM products")
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('products.html', products=products)

@app.route('/add_sale', methods=['GET', 'POST'])
def add_sale():
    conn = get_db_connection()
    cursor = conn.cursor()

    if request.method == 'POST':
        customer_id = request.form['customer_id']
        product_id = request.form['product_id']
        quantity = request.form['quantity']
        cursor.callproc('add_sale', (customer_id, product_id, quantity))
        conn.commit()
        cursor.close()
        conn.close()
        return redirect(url_for('home'))

    cursor.execute("SELECT customer_id, customer_name FROM customers")
    customers = cursor.fetchall()
    # select product fields including price so templates can access price for subtotal
    cursor.execute("SELECT product_id, product_name, category, price FROM products")
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('add_sale.html', customers=customers, products=products)


@app.route('/add_product', methods=['POST'])
def add_product():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify(success=False, error='name required'), 400
    category = data.get('category', '')
    try:
        price = float(data.get('price', 0) or 0)
    except Exception:
        return jsonify(success=False, error='invalid price'), 400
    try:
        stock = int(data.get('stock', 0) or 0)
    except Exception:
        return jsonify(success=False, error='invalid stock'), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO products (product_name, category, price, stock) VALUES (%s,%s,%s,%s)",
            (name, category, price, stock)
        )
        conn.commit()
        new_id = cursor.lastrowid
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        return jsonify(success=False, error=str(e)), 500
    cursor.close()
    conn.close()
    return jsonify(success=True, product={'id': new_id, 'name': name, 'category': category, 'price': price, 'stock': stock})


@app.route('/add_customer', methods=['POST'])
def add_customer():
    data = request.get_json() or {}
    name = (data.get('name') or '').strip()
    phone = (data.get('phone') or '').strip()
    email = (data.get('email') or '').strip()
    if not name:
        return jsonify(success=False, error='name required'), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    # To reduce duplicate inserts caused by concurrent requests, use a lightweight named lock
    # scoped to the phone (if present) or normalized name. This helps serialize two near-simultaneous
    # creates for the same logical customer.
    lock_key = None
    try:
        if phone:
            lock_key = f"cust_phone_{phone}"
        else:
            lock_key = f"cust_name_{name.strip().lower()}"

        # try to acquire the lock for up to 5 seconds
        cursor.execute("SELECT GET_LOCK(%s, 5)", (lock_key,))
        got = cursor.fetchone()
        # proceed regardless of whether lock obtained; lock reduces race window when available

        # Re-check existing under the lock
        existing = None
        if phone:
            cursor.execute("SELECT customer_id, customer_name, phone, email FROM customers WHERE phone=%s", (phone,))
            existing = cursor.fetchone()
        if not existing and email:
            cursor.execute("SELECT customer_id, customer_name, phone, email FROM customers WHERE email=%s", (email,))
            existing = cursor.fetchone()
        if not existing:
            cursor.execute("SELECT customer_id, customer_name, phone, email FROM customers WHERE LOWER(TRIM(customer_name)) = LOWER(TRIM(%s))", (name,))
            existing = cursor.fetchone()

        if existing:
            # release lock if we acquired it
            try:
                if lock_key:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
            except Exception:
                pass
            cursor.close()
            conn.close()
            return jsonify(success=True, customer={'id': existing[0], 'name': existing[1], 'phone': existing[2], 'email': existing[3], 'existing': True})

        # insert new customer
        try:
            cursor.execute("INSERT INTO customers (customer_name, phone, email) VALUES (%s,%s,%s)", (name, phone or None, email or None))
            conn.commit()
            new_id = cursor.lastrowid
        except Exception as e:
            conn.rollback()
            # if insert failed, try to find existing row and return it
            try:
                cursor.execute("SELECT customer_id, customer_name, phone, email FROM customers WHERE phone=%s OR email=%s OR LOWER(TRIM(customer_name)) = LOWER(TRIM(%s))", (phone, email, name))
                existing_after = cursor.fetchone()
                if existing_after:
                    try:
                        if lock_key:
                            cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
                    except Exception:
                        pass
                    cursor.close()
                    conn.close()
                    return jsonify(success=True, customer={'id': existing_after[0], 'name': existing_after[1], 'phone': existing_after[2], 'email': existing_after[3], 'existing': True})
            except Exception:
                pass
            cursor.close()
            conn.close()
            return jsonify(success=False, error=str(e)), 500

        # release lock
        try:
            if lock_key:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
        except Exception:
            pass
        cursor.close()
        conn.close()
        return jsonify(success=True, customer={'id': new_id, 'name': name, 'phone': phone or None, 'email': email or None, 'existing': False})
    except Exception as e:
        try:
            if lock_key:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
        except Exception:
            pass
        cursor.close()
        conn.close()
        return jsonify(success=False, error=str(e)), 500

if __name__ == '__main__':
    app.run(debug=True)
