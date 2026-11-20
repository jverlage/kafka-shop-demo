export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9094').split(',');

export const TOPICS = {
  orders: 'orders',
  payments: 'payments',
  shipments: 'shipments',
  serviceEvents: 'service-events',
} as const;

export const DATA_TOPICS = [TOPICS.orders, TOPICS.payments, TOPICS.shipments];
export const PARTITIONS = 3;
export const MAX_INSTANCES = 4;
export const SERVICE_PORT = 4000;
export const GATEWAY_PORT = 3000;
export const DASHBOARD_GROUP = 'dashboard';

export const CUSTOMERS = [
  { id: 'anna', name: 'Anna', color: '#e4572e' },
  { id: 'ben', name: 'Ben', color: '#2e86ab' },
  { id: 'clara', name: 'Clara', color: '#2a9d4b' },
  { id: 'david', name: 'David', color: '#b5179e' },
  { id: 'finn', name: 'Finn', color: '#e8a317' },
] as const;

export const PRODUCTS = [
  { id: 'hoodie', name: 'Kafka-Hoodie', price: 49, icon: '🧥' },
  { id: 'mug', name: 'Stream-Tasse', price: 12, icon: '☕' },
  { id: 'socks', name: 'Partition-Socken', price: 9, icon: '🧦' },
  { id: 'cap', name: 'Broker-Cap', price: 19, icon: '🧢' },
] as const;

export type ServiceName = 'inventory' | 'payment' | 'analytics' | 'shipping' | 'notification';

export interface ServiceDefinition {
  name: ServiceName;
  label: string;
  description: string;
  consumes: string;
  produces: string | null;
  delayMs: number;
}

export const SERVICES: Record<ServiceName, ServiceDefinition> = {
  inventory: {
    name: 'inventory',
    label: 'Lager',
    description: 'reserviert Ware und bucht den Bestand aus',
    consumes: TOPICS.orders,
    produces: null,
    delayMs: 100,
  },
  payment: {
    name: 'payment',
    label: 'Zahlung',
    description: 'bucht den Betrag ab',
    consumes: TOPICS.orders,
    produces: TOPICS.payments,
    delayMs: 250,
  },
  analytics: {
    name: 'analytics',
    label: 'Analytics',
    description: 'zählt Umsatz und Bestseller',
    consumes: TOPICS.orders,
    produces: null,
    delayMs: 50,
  },
  shipping: {
    name: 'shipping',
    label: 'Versand',
    description: 'erstellt das Versandlabel',
    consumes: TOPICS.payments,
    produces: TOPICS.shipments,
    delayMs: 120,
  },
  notification: {
    name: 'notification',
    label: 'Benachrichtigung',
    description: 'schickt dem Kunden eine E-Mail',
    consumes: TOPICS.shipments,
    produces: null,
    delayMs: 80,
  },
};

export const SERVICE_NAMES = Object.keys(SERVICES) as ServiceName[];

export interface Order {
  orderId: string;
  customerId: string;
  productId: string;
  quantity: number;
  amount: number;
  createdAt: string;
}

export interface Payment {
  orderId: string;
  customerId: string;
  paymentId: string;
  amount: number;
  paidAt: string;
}

export interface Shipment {
  orderId: string;
  customerId: string;
  carrier: string;
  trackingId: string;
  shippedAt: string;
}

export interface InstanceStatus {
  id: string;
  partitions: number[];
}

export type ServiceEvent =
  | {
      type: 'heartbeat';
      service: ServiceName;
      instances: InstanceStatus[];
      processed: number;
      stats: Record<string, string | number>;
      ts: number;
    }
  | {
      type: 'processed';
      service: ServiceName;
      instance: string;
      topic: string;
      partition: number;
      offset: string;
      orderId: string;
      customerId: string;
      info: string;
      ts: number;
    };
