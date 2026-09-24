[README.md](https://github.com/user-attachments/files/32596251/README.md)

# Inventory-Aware Cross-Product Arbitrage Bot

An automated Python arbitrage bot for five related order books:

- `CHICKEN`
- `LETTUCE`
- `BREAD`
- `BURGER = CHICKEN + LETTUCE + BREAD`
- `SALAD = CHICKEN + LETTUCE`

The bot compares executable bid/ask prices across the books, estimates net P&L after position-based fees, limits inventory concentration, and handles non-atomic multi-leg execution with cancellation, best-effort unwind, and exchange-position reconciliation.

## Strategy

The bot evaluates these conversion paths in both directions:

1. Buy chicken, lettuce, and bread; sell a burger.
2. Buy a burger; sell chicken, lettuce, and bread.
3. Buy chicken and lettuce; sell a salad.
4. Buy a salad; sell chicken and lettuce.
5. Buy a salad and bread; sell a burger.
6. Buy a burger; sell a salad and bread.

Only executable top-of-book prices and available volumes are used. Opportunities are ranked by estimated total net P&L, not only by unit spread.

The strategy is conversion-based and does not build a speculative single-asset position or use a separate hedging strategy.

## Risk Controls

- Accounts for regular `positionFee` and `excessPositionFee`.
- Applies per-product or conservative global `longLimit` values.
- Caps absolute inventory with `MAX_ABS_POSITION`.
- Caps each conversion with `MAX_BUNDLE_VOLUME`.
- Requires all five books to be present and fresh before trading.
- Refreshes exchange net positions and fee parameters periodically.
- Uses `send_mass_orders` for multi-leg execution.
- Cancels unfilled remainders when possible.
- Detects partial fills and attempts to unwind excess legs.
- Halts trading when order state, unwind state, or final positions are uncertain.

Because the SDK submits multiple legs separately, execution is not atomic. The bot therefore reconciles positions with the exchange after every attempted conversion and stops for manual review if exposure is not balanced.

## Requirements

- Python 3.10 or newer
- The exchange SDK files supplied by the trading platform:
  - `base_bot.py`
  - `models.py`
- An exchange endpoint compatible with the `BaseBot` SDK
- Credentials with permission to read order books, read positions, and submit/cancel orders

Place `base_bot.py`, `models.py`, and `arbitrage_bot.py` in the same directory. The bot also supports SDK copies named `base_bot_副本.py` and `models_副本.py` when those files are available beside it.

## Configuration

Credentials are required through environment variables:

```bash
export BOT_USERNAME="your-username"
export BOT_PASSWORD="your-password"
```

Optional configuration:

| Variable | Default | Description |
| --- | ---: | --- |
| `CMI_URL` | `http://127.0.0.1:80` | Exchange API base URL |
| `PRODUCT_CHICKEN` | `CHICKEN` | Chicken product symbol |
| `PRODUCT_LETTUCE` | `LETTUCE` | Lettuce product symbol |
| `PRODUCT_BREAD` | `BREAD` | Bread product symbol |
| `PRODUCT_BURGER` | `BURGER` | Burger product symbol |
| `PRODUCT_SALAD` | `SALAD` | Salad product symbol |
| `MAX_BUNDLE_VOLUME` | `3` | Maximum units per conversion |
| `MAX_ABS_POSITION` | `6` | Maximum absolute position per product |
| `MIN_EDGE` | `0.01` | Minimum estimated net edge per conversion unit |
| `SAFETY_BUFFER` | `0.02` | Per-unit safety deduction from estimated P&L |
| `STATE_REFRESH_SECONDS` | `5` | Position and fee refresh interval |
| `MAX_BOOK_AGE_SECONDS` | `5` | Maximum accepted order-book age |

The five configured product symbols must be unique.

## Run

```bash
python3 -m py_compile arbitrage_bot.py
python3 arbitrage_bot.py
```

Stop the bot with `Ctrl+C`. If the bot detects uncertain exposure, it stops submitting new trades and prints a manual-review message.

## Validation

The implementation has been checked for:

- Python syntax errors
- Depth-aware opportunity sizing
- Both sides of each conversion
- Fee-adjusted P&L ranking
- Long and short inventory limits
- Unordered multi-order responses
- Cancellation of unfilled orders
- Partial-fill unwind behavior
- Authoritative post-trade position reconciliation

Run against a simulator or sandbox before connecting to a live exchange. The repository does not include live credentials or the exchange's proprietary SDK implementation.

## License

Add the license required by your exchange, organization, or course before publishing the repository publicly.
