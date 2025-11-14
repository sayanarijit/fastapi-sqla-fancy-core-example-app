### Run

```bash
uv sync

# docker-compose up -d  # Or podman-compose up -d

uv run ./src/app_1_hidden_context.py
# or
uv run ./src/app_2_dependency_injection.py
# or
uv run ./src/app_3_optional_param.py
```

### Test

```bash
# Create book and author
curl http://localhost:8000/books --json '{"title": "The Great Gatsby", "author_name": "F. Scott Fitzgerald"}'

# Get books
curl http://localhost:8000/books

# Get authors
curl http://localhost:8000/authors
```

### Stress Test with sanity check

```bash
# Run parallel insert queries with wrk
wrk http://localhost:8000/books -s tests/wrk.lua

# Check data integrity
curl http://localhost:8000/stats
```
