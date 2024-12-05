-- Product Categories (created first to avoid foreign key reference issues)
CREATE TABLE product_categories (
  category_id UUID PRIMARY KEY,
  parent_category_id UUID REFERENCES product_categories(category_id),
  name VARCHAR(100),
  description TEXT,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Users and Demographics
CREATE TABLE users (
  user_id UUID PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  registration_date TIMESTAMP DEFAULT now(),
  last_login TIMESTAMP,
  is_active BOOLEAN DEFAULT true,
  preferences JSONB,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE user_demographics (
  demographic_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  age_range VARCHAR(20),
  gender VARCHAR(20),
  income_bracket VARCHAR(20),
  occupation VARCHAR(100),
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE user_addresses (
  address_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  address_type VARCHAR(20),
  street_address TEXT,
  city VARCHAR(255),
  state VARCHAR(100),
  country VARCHAR(100),
  postal_code VARCHAR(100),
  is_default BOOLEAN,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Sessions and Device Info
CREATE TABLE sessions (
  session_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  timestamp_start TIMESTAMP DEFAULT now(),
  timestamp_end TIMESTAMP,
  device_type VARCHAR(50),
  os_info VARCHAR(100),
  browser_info VARCHAR(100),
  ip_address INET,
  referral_source VARCHAR(255),
  utm_source VARCHAR(50),
  utm_medium VARCHAR(50),
  utm_campaign VARCHAR(50),
  created_at TIMESTAMP DEFAULT now(),
  CONSTRAINT sessions_user_timestamp_idx UNIQUE (user_id, timestamp_start)
);

-- Products (references product_categories)
CREATE TABLE products (
  product_id UUID PRIMARY KEY,
  sku VARCHAR(50) UNIQUE,
  name VARCHAR(255),
  description TEXT,
  category_id UUID REFERENCES product_categories(category_id),
  price DECIMAL(10,2),
  cost DECIMAL(10,2),
  stock_quantity INTEGER,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Product Interactions
CREATE TABLE product_views (
  view_id UUID PRIMARY KEY,
  session_id UUID REFERENCES sessions(session_id),
  product_id UUID REFERENCES products(product_id),
  view_timestamp TIMESTAMP DEFAULT now(),
  view_duration INTERVAL,
  source_page VARCHAR(255),
  CONSTRAINT product_views_session_view_timestamp_idx UNIQUE (session_id, view_timestamp)
);

CREATE TABLE wishlists (
  wishlist_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  product_id UUID REFERENCES products(product_id),
  added_timestamp TIMESTAMP DEFAULT now(),
  removed_timestamp TIMESTAMP,
  notes TEXT
);

-- Shopping Cart
CREATE TABLE carts (
  cart_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  session_id UUID REFERENCES sessions(session_id),
  status VARCHAR(20),
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE cart_items (
  cart_item_id UUID PRIMARY KEY,
  cart_id UUID REFERENCES carts(cart_id),
  product_id UUID REFERENCES products(product_id),
  quantity INTEGER,
  added_timestamp TIMESTAMP DEFAULT now(),
  removed_timestamp TIMESTAMP,
  unit_price DECIMAL(10,2),
  CONSTRAINT cart_items_cart_added_timestamp_idx UNIQUE (cart_id, added_timestamp)
);

-- Orders and Transactions
CREATE TABLE orders (
  order_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  cart_id UUID REFERENCES carts(cart_id),
  status VARCHAR(20),
  total_amount DECIMAL(10,2),
  tax_amount DECIMAL(10,2),
  shipping_amount DECIMAL(10,2),
  discount_amount DECIMAL(10,2),
  payment_method VARCHAR(50),
  delivery_method VARCHAR(50),
  billing_address_id UUID REFERENCES user_addresses(address_id),
  shipping_address_id UUID REFERENCES user_addresses(address_id),
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  CONSTRAINT orders_user_created_at_idx UNIQUE (user_id, created_at)
);

CREATE TABLE order_items (
  order_item_id UUID PRIMARY KEY,
  order_id UUID REFERENCES orders(order_id),
  product_id UUID REFERENCES products(product_id),
  quantity INTEGER,
  unit_price DECIMAL(10,2),
  discount_amount DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT now()
);

-- Customer Service
CREATE TABLE support_tickets (
  ticket_id UUID PRIMARY KEY,
  user_id UUID REFERENCES users(user_id),
  order_id UUID REFERENCES orders(order_id),
  issue_type VARCHAR(50),
  priority VARCHAR(20),
  status VARCHAR(20),
  created_at TIMESTAMP DEFAULT now(),
  resolved_at TIMESTAMP,
  satisfaction_score INTEGER,
  CONSTRAINT support_tickets_user_created_at_idx UNIQUE (user_id, created_at)
);

CREATE TABLE ticket_messages (
  message_id UUID PRIMARY KEY,
  ticket_id UUID REFERENCES support_tickets(ticket_id),
  sender_type VARCHAR(20),
  message_text TEXT,
  created_at TIMESTAMP DEFAULT now()
);
