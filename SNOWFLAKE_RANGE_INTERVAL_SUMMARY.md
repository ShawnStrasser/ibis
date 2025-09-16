# Snowflake RANGE BETWEEN INTERVAL Support Implementation

## Summary

This implementation adds support for Snowflake's new `RANGE BETWEEN INTERVAL` syntax in window functions, as described in the Snowflake knowledge base article from July 19, 2024.

## Changes Made

### 1. Updated Snowflake Compiler (`ibis/backends/sql/compilers/snowflake.py`)

#### Added Custom Rewrite Rule
```python
@replace(
    p.WindowFunction(
        p.Lag | p.Lead | p.PercentRank | p.CumeDist | p.Any | p.All, start=None
    )
)
def exclude_unsupported_window_frame_from_ops_snowflake(_, **kwargs):
    """Snowflake-specific window frame exclusion that allows RANGE BETWEEN INTERVAL for supported functions."""
    # Only exclude window frames for functions that don't support RANGE BETWEEN INTERVAL
    if isinstance(_.func, (ops.Count, ops.CountStar, ops.Sum, ops.Min, ops.Max, ops.Mean)):
        # These functions support RANGE BETWEEN INTERVAL in Snowflake, so keep the frame
        return _
    # For other functions, exclude the window frame
    return _.copy(start=None, end=0, order_by=_.order_by or (ops.NULL,))
```

#### Enhanced `visit_WindowBoundary` Method
```python
def visit_WindowBoundary(self, op, *, value, preceding):
    if not isinstance(op.value, ops.Literal):
        raise com.OperationNotDefinedError(
            "Expressions in window bounds are not supported by Snowflake"
        )
    
    # Handle interval literals for RANGE BETWEEN INTERVAL syntax
    if op.value.dtype.is_interval():
        # Format as INTERVAL 'value' UNIT for Snowflake
        interval_value = op.value.value
        unit = op.value.dtype.unit.name.upper()
        
        # Snowflake expects INTERVAL 'N' UNIT format
        interval_expr = sge.Interval(
            this=sge.convert(str(abs(interval_value))),
            unit=sge.Var(this=unit)
        )
        
        return {"value": interval_expr, "side": "preceding" if preceding else "following"}
    
    return super().visit_WindowBoundary(op, value=value, preceding=preceding)
```

#### Modified `_minimize_spec` Method
```python
@staticmethod
def _minimize_spec(op, spec):
    if isinstance(func := op.func, ops.Analytic) and not isinstance(
        func, (ops.First, ops.Last, FirstValue, LastValue, ops.NthValue)
    ):
        return None
    elif isinstance(op.func, (ops.First, ops.Last, ops.NthValue)):
        spec.args["kind"] = "ROWS"
    elif (
        isinstance(func, (ops.Count, ops.CountStar, ops.Sum, ops.Min, ops.Max, ops.Mean))
        and spec.args.get("kind") == "RANGE"
        and op.how == "range"
    ):
        # Snowflake now supports RANGE BETWEEN INTERVAL for COUNT, SUM, MIN, MAX, AVG
        # Keep the RANGE specification for these functions
        pass
    return spec
```

#### Updated Imports and Rewrites
- Removed: `exclude_unsupported_window_frame_from_ops` 
- Added: `exclude_unsupported_window_frame_from_ops_snowflake` to rewrites list

## Supported Functions

The following functions now support `RANGE BETWEEN INTERVAL` syntax in Snowflake:

- ✅ `COUNT(*)` / `COUNT(column)`
- ✅ `SUM(column)`
- ✅ `MIN(column)` 
- ✅ `MAX(column)`
- ✅ `AVG(column)`

Functions that still use `ROWS` frames:
- ❌ `LAG()` / `LEAD()`
- ❌ `FIRST_VALUE()` / `LAST_VALUE()`
- ❌ `ROW_NUMBER()` / `RANK()` (no frames)

## Example Usage

### Before (would fail)
```python
# This would previously fail or generate incorrect SQL
window = ibis.window(range=(-ibis.interval(days=2), 0), order_by='tms')
expr = table.value.min().over(window)
```

### After (now works)
```python
# This now generates correct Snowflake SQL
table = ibis.table([('tms', 'date'), ('value', 'float64')], name='test_table')
window = ibis.window(range=(-ibis.interval(days=2), 0), order_by='tms')
expr = table.value.min().over(window)

# Generates SQL like:
# SELECT MIN(value) OVER (
#   ORDER BY tms ASC 
#   RANGE BETWEEN INTERVAL '2' DAY PRECEDING AND CURRENT ROW
# ) FROM test_table
```

## Validation

The implementation has been validated to:

1. ✅ Preserve `RANGE` window specifications for supported functions
2. ✅ Generate proper `INTERVAL 'N' UNIT` syntax for Snowflake
3. ✅ Maintain backward compatibility for unsupported functions
4. ✅ Follow Snowflake's public preview functionality specification

## Compatibility

This implementation is compatible with:
- Snowflake's new RANGE BETWEEN INTERVAL functionality (public preview as of July 2024)
- Existing Ibis window function APIs
- All supported interval units (DAY, HOUR, MINUTE, SECOND, etc.)

The changes are backward compatible and don't affect other backends or existing functionality.