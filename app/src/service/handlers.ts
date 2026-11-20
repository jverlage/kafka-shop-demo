import { CUSTOMERS, PRODUCTS, type Order, type Payment, type ServiceName, type Shipment } from '../shared/config.ts';

export interface HandlerResult {
  info: string;
  output?: { key: string; value: unknown };
}

export interface Handler {
  handle(payload: any): HandlerResult;
  stats(): Record<string, string | number>;
  reset(): void;
}

const euro = (value: number) =>
  value.toLocaleString('de-DE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
const productName = (id: string) => PRODUCTS.find((p) => p.id === id)?.name ?? id;
const customerName = (id: string) => CUSTOMERS.find((c) => c.id === id)?.name ?? id;
const randomId = (prefix: string) => `${prefix}-${Math.random().toString(36).slice(2, 8).toUpperCase()}`;

const INITIAL_STOCK = 50;

function inventory(): Handler {
  let stock: Record<string, number> = {};
  const reset = () => (stock = Object.fromEntries(PRODUCTS.map((p) => [p.id, INITIAL_STOCK])));
  reset();
  return {
    handle(order: Order) {
      stock[order.productId] -= order.quantity;
      let info = `${productName(order.productId)} reserviert, Bestand ${stock[order.productId]}`;
      if (stock[order.productId] <= 0) {
        stock[order.productId] = INITIAL_STOCK;
        info = `${productName(order.productId)} ausverkauft – nachbestellt`;
      }
      return { info };
    },
    stats: () => Object.fromEntries(PRODUCTS.map((p) => [p.icon, stock[p.id]])),
    reset,
  };
}

function payment(): Handler {
  let total = 0;
  return {
    handle(order: Order) {
      total += order.amount;
      const result: Payment = {
        orderId: order.orderId,
        customerId: order.customerId,
        paymentId: randomId('PAY'),
        amount: order.amount,
        paidAt: new Date().toISOString(),
      };
      return { info: `${euro(order.amount)} von ${customerName(order.customerId)} abgebucht`, output: { key: order.customerId, value: result } };
    },
    stats: () => ({ 'Abgebucht': euro(total) }),
    reset: () => (total = 0),
  };
}

function analytics(): Handler {
  let revenue = 0;
  let orders = 0;
  let sold: Record<string, number> = {};
  return {
    handle(order: Order) {
      revenue += order.amount;
      orders += 1;
      sold[order.productId] = (sold[order.productId] ?? 0) + order.quantity;
      return { info: `Umsatz jetzt ${euro(revenue)}` };
    },
    stats() {
      const top = Object.entries(sold).sort((a, b) => b[1] - a[1])[0];
      return {
        Bestellungen: orders,
        Umsatz: euro(revenue),
        Bestseller: top ? PRODUCTS.find((p) => p.id === top[0])!.icon : '–',
      };
    },
    reset: () => {
      revenue = 0;
      orders = 0;
      sold = {};
    },
  };
}

const CARRIERS = ['DHL', 'DPD', 'UPS', 'Hermes'];

function shipping(): Handler {
  let perCarrier: Record<string, number> = {};
  let next = 0;
  return {
    handle(payment: Payment) {
      const carrier = CARRIERS[next++ % CARRIERS.length];
      perCarrier[carrier] = (perCarrier[carrier] ?? 0) + 1;
      const result: Shipment = {
        orderId: payment.orderId,
        customerId: payment.customerId,
        carrier,
        trackingId: randomId(carrier.slice(0, 3).toUpperCase()),
        shippedAt: new Date().toISOString(),
      };
      return { info: `Label bei ${carrier} erstellt`, output: { key: payment.customerId, value: result } };
    },
    stats: () => ({ ...perCarrier }),
    reset: () => {
      perCarrier = {};
      next = 0;
    },
  };
}

function notification(): Handler {
  let sent = 0;
  return {
    handle(shipment: Shipment) {
      sent += 1;
      return { info: `E-Mail an ${customerName(shipment.customerId)}: „Dein Paket ist mit ${shipment.carrier} unterwegs“` };
    },
    stats: () => ({ 'E-Mails': sent }),
    reset: () => (sent = 0),
  };
}

export const HANDLERS: Record<ServiceName, () => Handler> = { inventory, payment, analytics, shipping, notification };
