"""Inventory-aware cross-product arbitrage bot.

The exchange must provide ``base_bot.py`` and ``models.py`` next to this file.
Product symbols can be changed with the PRODUCT_* environment variables.
"""

from __future__ import annotations

import os
import importlib.util
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event

try:
    from base_bot import BaseBot
    from models import OrderBook, OrderRequest, Side, Trade
except ModuleNotFoundError as import_error:
    # The supplied read-only SDK copies may carry a ``_副本`` filename suffix.
    # If the bot is placed beside those copies, load them under their canonical
    # module names without editing either SDK file.
    if import_error.name not in {"base_bot", "models"}:
        raise
    sdk_directory = Path(__file__).resolve().parent
    models_copy = sdk_directory / "models_副本.py"
    base_bot_copy = sdk_directory / "base_bot_副本.py"
    if not models_copy.is_file() or not base_bot_copy.is_file():
        raise

    def _load_sdk_module(module_name: str, module_path: Path):
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load SDK module from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    _load_sdk_module("models", models_copy)
    _load_sdk_module("base_bot", base_bot_copy)
    from base_bot import BaseBot
    from models import OrderBook, OrderRequest, Side, Trade


CMI_URL = os.getenv("CMI_URL", "http://127.0.0.1:80")
PRODUCT_CHICKEN = os.getenv("PRODUCT_CHICKEN", "CHICKEN")
PRODUCT_LETTUCE = os.getenv("PRODUCT_LETTUCE", "LETTUCE")
PRODUCT_BREAD = os.getenv("PRODUCT_BREAD", "BREAD")
PRODUCT_BURGER = os.getenv("PRODUCT_BURGER", "BURGER")
PRODUCT_SALAD = os.getenv("PRODUCT_SALAD", "SALAD")


@dataclass(frozen=True)
class Opportunity:
    """One executable conversion, represented as product -> signed quantity."""

    name: str
    deltas: dict[str, int]
    gross_per_unit: float
    executable_volume: int


class ConversionArbitrageBot(BaseBot):
    """Trade recipe-equivalent baskets while bounding speculative inventory."""

    # Conservative defaults. They can be overridden without editing the program.
    max_bundle_volume = int(os.getenv("MAX_BUNDLE_VOLUME", "3"))
    max_abs_position = int(os.getenv("MAX_ABS_POSITION", "6"))
    min_edge = float(os.getenv("MIN_EDGE", "0.01"))
    safety_buffer = float(os.getenv("SAFETY_BUFFER", "0.02"))
    state_refresh_seconds = float(os.getenv("STATE_REFRESH_SECONDS", "5"))
    max_book_age_seconds = float(os.getenv("MAX_BOOK_AGE_SECONDS", "5"))

    def __init__(self, cmi_url: str, username: str, password: str):
        if len(set(self.products)) != len(self.products):
            raise ValueError("The five configured product symbols must be distinct")
        if self.max_bundle_volume <= 0 or self.max_abs_position <= 0:
            raise ValueError("MAX_BUNDLE_VOLUME and MAX_ABS_POSITION must be positive")
        self._books: dict[str, OrderBook] = {}
        self._book_received_at: dict[str, float] = {}
        self._positions: dict[str, float] = {}
        self._positions_known = False
        self._last_state_refresh = 0.0
        self._fee_parameters: tuple[float, float, dict[str, float]] | None = None
        self._last_fee_refresh = 0.0
        self._trading_halted = False
        self._lock = threading.RLock()
        self._in_flight = False
        super().__init__(cmi_url, username, password)
        self._refresh_positions(force=True)

    @property
    def products(self) -> tuple[str, ...]:
        return (
            PRODUCT_CHICKEN,
            PRODUCT_LETTUCE,
            PRODUCT_BREAD,
            PRODUCT_BURGER,
            PRODUCT_SALAD,
        )

    def on_orderbook(self, orderbook: OrderBook):
        if orderbook.product not in self.products:
            return
        with self._lock:
            self._books[orderbook.product] = orderbook
            self._book_received_at[orderbook.product] = time.monotonic()
            self._refresh_positions()
            if (
                self._in_flight
                or self._trading_halted
                or not self._positions_known
                or not all(product in self._books for product in self.products)
                or any(time.monotonic() - self._book_received_at.get(product, 0.0) > self.max_book_age_seconds for product in self.products)
            ):
                return
            opportunity = self._best_opportunity()
            if opportunity is None:
                return
            self._in_flight = True

        try:
            self._execute(opportunity)
        finally:
            with self._lock:
                self._in_flight = False

    def on_trades(self, trades: list[Trade]):
        # Trade events are informational; order responses are the authoritative fill source.
        return None

    def _best_opportunity(self) -> Opportunity | None:
        candidates: list[Opportunity] = []
        candidates.extend(self._make_conversion("base_to_burger", PRODUCT_BURGER, (
            PRODUCT_CHICKEN, PRODUCT_LETTUCE, PRODUCT_BREAD
        )))
        candidates.extend(self._make_conversion("base_to_salad", PRODUCT_SALAD, (
            PRODUCT_CHICKEN, PRODUCT_LETTUCE
        )))
        # Burger and salad differ by exactly one bread unit, so this is the
        # same recipe identity viewed through the two ready-made products.
        candidates.extend(self._make_conversion("salad_bread_to_burger", PRODUCT_BURGER, (
            PRODUCT_SALAD, PRODUCT_BREAD
        )))
        viable: list[tuple[Opportunity, float]] = []
        for candidate in candidates:
            net_pnl = self._risk_adjusted_profit(candidate)
            if net_pnl is not None and net_pnl / candidate.executable_volume >= self.min_edge:
                viable.append((candidate, net_pnl))
        return max(viable, key=lambda item: item[1], default=(None, 0.0))[0]

    def _make_conversion(self, name: str, composite: str, bases: tuple[str, ...]) -> list[Opportunity]:
        composite_book = self._books[composite]
        base_books = [self._books[base] for base in bases]
        # Each direction only needs the side it actually crosses.
        base_to_composite = None
        if composite_book.buy_orders and all(book.sell_orders for book in base_books):
            buy_bases_price = sum(book.sell_orders[0].price for book in base_books)
            base_to_composite = composite_book.buy_orders[0].price - buy_bases_price
        composite_to_base = None
        if composite_book.sell_orders and all(book.buy_orders for book in base_books):
            sell_bases_price = sum(book.buy_orders[0].price for book in base_books)
            composite_to_base = sell_bases_price - composite_book.sell_orders[0].price

        result: list[Opportunity] = []
        for gross, buy_composite_leg in ((base_to_composite, False), (composite_to_base, True)):
            if gross is None or gross <= 0:
                continue
            depth = composite_book.sell_orders[0] if buy_composite_leg else composite_book.buy_orders[0]
            depth_values = [depth.volume]
            for book in base_books:
                leg = book.buy_orders[0] if buy_composite_leg else book.sell_orders[0]
                depth_values.append(leg.volume)
            volume = min(self.max_bundle_volume, *(int(value) for value in depth_values))
            if volume <= 0:
                continue
            unit_deltas = {product: 0 for product in (*bases, composite)}
            if buy_composite_leg:
                unit_deltas[composite] = 1
                for base in bases:
                    unit_deltas[base] = -1
            else:
                unit_deltas[composite] = -1
                for base in bases:
                    unit_deltas[base] = 1
            volume = self._position_limited_volume(unit_deltas, volume)
            if volume > 0:
                result.append(Opportunity(name, {product: delta * volume for product, delta in unit_deltas.items()}, gross, volume))
        return result

    def _position_limited_volume(self, deltas: dict[str, int], volume: int) -> int:
        fee_parameters = self._get_fee_parameters()
        if fee_parameters is None:
            return 0
        _, _, long_limits = fee_parameters
        allowed = volume
        for product, delta in deltas.items():
            if not delta:
                continue
            current = self._positions.get(product, 0.0)
            direction = 1 if delta > 0 else -1
            # Largest integer q satisfying |current + direction*q| <= cap.
            if direction > 0:
                upper_bound = min(self.max_abs_position, long_limits.get(product, self.max_abs_position))
                room = upper_bound - current
            else:
                room = self.max_abs_position + current
            if room < 0:
                return 0
            allowed = min(allowed, int(room))
        return max(0, allowed)

    def _risk_adjusted_edge(self, opportunity: Opportunity) -> float:
        profit = self._risk_adjusted_profit(opportunity)
        if profit is None:
            return float("-inf")
        return profit / opportunity.executable_volume

    def _risk_adjusted_profit(self, opportunity: Opportunity) -> float | None:
        gross = opportunity.gross_per_unit * opportunity.executable_volume
        before = self._estimated_position_fee(self._positions)
        if before is None:
            return None
        after_positions = dict(self._positions)
        for product, delta in opportunity.deltas.items():
            after_positions[product] = after_positions.get(product, 0.0) + delta
        after = self._estimated_position_fee(after_positions)
        if after is None:
            return None
        # A reduction in net-position fees is a real P&L benefit, so retain the
        # signed difference rather than treating every position change as a cost.
        fee_delta = after - before
        return gross - fee_delta - self.safety_buffer * opportunity.executable_volume

    def _estimated_position_fee(self, positions: dict[str, float]) -> float | None:
        parameters = self._get_fee_parameters()
        if parameters is None:
            return None
        regular, excess_surcharge, limits = parameters
        # The exchange describes excessPositionFee as an extra per-lot charge
        # beyond longLimit, so it is added to the regular position fee.
        return sum(
            abs(position) * regular
            + max(0.0, position - limits.get(product, float(self.max_abs_position))) * excess_surcharge
            for product, position in positions.items()
        )

    def _get_fee_parameters(self) -> tuple[float, float, dict[str, float]] | None:
        now = time.monotonic()
        if self._fee_parameters is not None and now - self._last_fee_refresh < self.state_refresh_seconds:
            return self._fee_parameters
        status = self._get_json("/api/status")
        if status is None:
            return None
        regular = float(status.get("positionFee") or 0.0)
        excess = float(status.get("excessPositionFee") or 0.0)
        limits: dict[str, float] = {}
        default_limit = float(self.max_abs_position)
        for item in status.get("positionLimits") or []:
            if item.get("longLimit") is None:
                continue
            product = item.get("product") or item.get("symbol") or item.get("productSymbol")
            if product:
                limits[str(product)] = float(item["longLimit"])
            else:
                default_limit = min(default_limit, float(item["longLimit"]))
        # Preserve a conservative global fallback for SDK versions whose status
        # response has limits without identifying the associated product.
        for product in self.products:
            limits.setdefault(product, default_limit)
        self._fee_parameters = (regular, excess, limits)
        self._last_fee_refresh = now
        return self._fee_parameters

    def _execute(self, opportunity: Opportunity) -> None:
        positions_before = dict(self._positions)
        requests: list[OrderRequest] = []
        for product, delta in opportunity.deltas.items():
            if not delta:
                continue
            book = self._books[product]
            if delta > 0:
                order = book.sell_orders[0]
                side = Side.BUY
            else:
                order = book.buy_orders[0]
                side = Side.SELL
            requests.append(OrderRequest(product=product, side=side, price=order.price, volume=abs(delta)))
        responses = self.send_mass_orders(requests)
        response_by_product = {
            response.product: response
            for response in responses
            if response is not None
        }
        uncertain_order_state = False
        fills: dict[str, float] = {}
        for request in requests:
            response = response_by_product.get(request.product)
            filled = float(response.filled) if response is not None and response.filled else 0.0
            fills[request.product] = filled
            if response is None:
                # The POST may have reached the exchange even if its response was lost.
                uncertain_order_state = True
            elif filled < float(response.volume):
                # Avoid stale/unfilled remainders turning into unintended passive orders.
                if self.cancel_order_by_id(response.id) is None:
                    uncertain_order_state = True

        # Independently submitted legs are not atomic. Keep only the number of
        # complete conversion units filled on every leg; unwind any excess fills.
        common_filled = min(fills.values(), default=0.0)
        excess_requests: list[OrderRequest] = []
        for request in requests:
            excess = max(0, int(fills[request.product] - common_filled))
            if not excess:
                continue
            book = self._books[request.product]
            if request.side == Side.BUY and book.buy_orders:
                side, price = Side.SELL, book.buy_orders[0].price
            elif request.side == Side.SELL and book.sell_orders:
                side, price = Side.BUY, book.sell_orders[0].price
            else:
                self._trading_halted = True
                self._say(f"Could not unwind unpaired fill in {request.product}; trading halted")
                continue
            excess_requests.append(OrderRequest(request.product, side, price, excess))

        if excess_requests:
            unwind_responses = self.send_mass_orders(excess_requests)
            unwind_by_product = {response.product: response for response in unwind_responses if response is not None}
            for request in excess_requests:
                response = unwind_by_product.get(request.product)
                filled = float(response.filled) if response is not None and response.filled else 0.0
                if response is None:
                    uncertain_order_state = True
                elif filled < float(response.volume):
                    if self.cancel_order_by_id(response.id) is None:
                        uncertain_order_state = True

        with self._lock:
            if uncertain_order_state:
                self._trading_halted = True
            # Re-read exchange positions after fills/cancels. This also catches fills
            # that raced with a cancel response; never resume on locally guessed state.
            final_positions = self.request_net_positions()
            if final_positions is None:
                self._positions_known = False
                self._trading_halted = True
            else:
                self._positions = {product: float(value) for product, value in final_positions.items()}
                self._positions_known = True
                net_units: list[float] = []
                for request in requests:
                    sign = 1.0 if request.side == Side.BUY else -1.0
                    actual_change = self._positions.get(request.product, 0.0) - positions_before.get(request.product, 0.0)
                    net_units.append(sign * actual_change)
                if net_units and (min(net_units) < -1e-9 or max(net_units) - min(net_units) > 1e-9):
                    self._trading_halted = True
            if self._trading_halted:
                self._say("Unpaired exposure remains after best-effort unwind; trading halted for manual review")
            self._last_state_refresh = time.monotonic()

    def _refresh_positions(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_state_refresh < self.state_refresh_seconds:
            return
        positions = self.request_net_positions()
        if positions is not None:
            self._positions = {product: float(value) for product, value in positions.items()}
            self._positions_known = True
            self._last_state_refresh = now


def main():
    stop_event = Event()
    username = os.getenv("BOT_USERNAME")
    password = os.getenv("BOT_PASSWORD")
    if not username or not password:
        raise SystemExit("Set BOT_USERNAME and BOT_PASSWORD environment variables before starting the bot")
    bot = ConversionArbitrageBot(CMI_URL, username, password)
    try:
        bot.start()
        stop_event.wait()
    except KeyboardInterrupt:
        print("Stopping bot...")
        bot.stop()


if __name__ == "__main__":
    main()
