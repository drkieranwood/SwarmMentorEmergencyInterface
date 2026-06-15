# Offline Map Tiles

Place offline tile sets here as named sub-folders.

## Expected structure

```
assets/tiles/{map_name}/{z}/{x}/{y}.jpg
```

- `{map_name}` — any slug, e.g. `nenana`. Matches the name used in the TileLayer URL.
- `{z}` — zoom level (integer)
- `{x}` — tile column
- `{y}` — tile row (XYZ / Slippy Map convention, **not** TMS — y=0 is the top)

## TileLayer URL (in the app)

```python
dl.TileLayer(url="/tiles/{map_name}/{z}/{x}/{y}.jpg", ...)
```

The Flask route `serve_tiles()` serves this directory at `/tiles/`.

## Generating tiles

Use [MOBAC](https://mobac.sourceforge.io/) or `gdal2tiles.py` to export an area to the XYZ format.
Zoom levels 1–22 are supported; typical offline deployments use levels 14–22.
