Run

```bash
uv sync

uv run app.py
```

Test

```bash
# Create book and author
curl http://localhost:8000/books --json '{"title": "The Great Gatsby", "author_name": "F. Scott Fitzgerald"}'

# Get books
curl http://localhost:8000/books

# Get authors
curl http://localhost:8000/authors
```
