# /wsgi.py
from app import create_app

# Create app instance
app = create_app('Production')

if __name__ == "__main__":
    app.run()
