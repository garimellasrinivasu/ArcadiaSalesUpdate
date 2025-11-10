# Arcadia Sales UI

A Flask-based sales tracking dashboard for Arcadia with CRM and Admin workflows.

## Tech Stack
- Python, Flask
- SQLite + SQLAlchemy
- HTML templates (Jinja2) + vanilla CSS/JS

## Running locally
1. Create a virtual environment and install dependencies.
2. Set `FLASK_APP=webapp/app.py` and run the app.

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # if available, else: pip install flask sqlalchemy
export FLASK_APP=webapp/app.py
flask run
```

## Default users
- admin / admin (Admin)
- vasu / kaka (CRM)

## Notes
- Database file is `arcadia_sales.db` in the project root.
- Environment variable `APP_SECRET` can override the development secret key.
# ArcadiaSalesUpdate
