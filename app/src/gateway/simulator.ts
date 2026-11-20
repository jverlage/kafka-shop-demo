export const MIN_RATE = 1;
export const MAX_RATE = 20;

export function createSimulator(placeOrder: () => Promise<unknown>, onChange: () => void) {
  let enabled = false;
  let rate = 2;
  let timer: NodeJS.Timeout | null = null;

  const tick = () => {
    placeOrder().catch((err) => console.error(`[simulator] ${(err as Error).message}`));
    timer = setTimeout(tick, 1000 / rate);
  };

  return {
    state: () => ({ enabled, rate }),
    update(next: { enabled?: boolean; rate?: number }) {
      if (next.rate !== undefined) rate = Math.min(MAX_RATE, Math.max(MIN_RATE, Math.round(next.rate)));
      if (next.enabled !== undefined) enabled = next.enabled;
      if (timer) clearTimeout(timer);
      timer = enabled ? setTimeout(tick, 1000 / rate) : null;
      onChange();
    },
  };
}
